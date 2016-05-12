package io.advantageous.elekt.consul;

import io.advantageous.consul.Consul;
import io.advantageous.elekt.Endpoint;
import io.advantageous.elekt.LeaderElector;
import io.advantageous.reakt.Callback;
import io.advantageous.reakt.Stream;
import io.advantageous.reakt.reactor.Reactor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;


/**
 * Maintains a new leader endpoint given a service name.
 * Uses the technique described here: https://www.consul.io/docs/guides/leader-election.html
 */
public class ConsulLeadershipElector implements LeaderElector {

    /**
     * Logger
     */
    protected final Logger logger = LoggerFactory.getLogger(ConsulLeadershipElector.class);

    /**
     * Reactor for repeating tasks.
     */
    private final Reactor reactor;

    /**
     * Service name to watch leadership changes on.
     */
    private final String serviceName;

    /**
     * Provider - consul wrapper to make this test more testable.
     */
    private final LeadershipProvider leadershipProvider;
    /**
     * List of stream listeners.
     */
    private final CopyOnWriteArrayList<Stream<Endpoint>> listeners = new CopyOnWriteArrayList<>();
    /**
     * The highest modification index used for long polling key associated with leadership.
     */
    private final AtomicLong modifyIndex = new AtomicLong();
    /**
     * Reference to the current leader.
     */
    protected AtomicReference<Endpoint> currentLeader = new AtomicReference<>();
    /**
     * Is this node the leader? We assume the endpoint you pass to selfElect is self.
     */
    private AtomicBoolean leader = new AtomicBoolean();
    /**
     * The current sessionID.
     */
    private AtomicReference<String> sessionId = new AtomicReference<>();
    /**
     * One request queue for consul so things happen orderly.
     */
    private ExecutorService requestExecutorService = Executors.newSingleThreadExecutor();
    /**
     * Another thread to notify listeners w/o blocking request queue.
     */
    private ExecutorService outExecutorService = Executors.newSingleThreadExecutor();
    /**
     * Another thread for our leadership long poll.
     */
    private ExecutorService longPollExecutorService = Executors.newSingleThreadExecutor();

    /**
     * Used for testing only
     *
     * @param leadershipProvider     leadershipProvider
     * @param serviceName            serviceName
     * @param reactor                reactor
     * @param timeUnit               timeUnit
     * @param sessionLifeTTL         sessionLifeTTL
     * @param newLeaderCheckInterval newLeaderCheckInterval
     */
    ConsulLeadershipElector(final LeadershipProvider leadershipProvider,
                            final String serviceName,
                            final Reactor reactor,
                            final TimeUnit timeUnit,
                            final long sessionLifeTTL,
                            final long newLeaderCheckInterval) {
        this.reactor = reactor;
        this.serviceName = serviceName;
        this.leadershipProvider = leadershipProvider;
        init(reactor, timeUnit, sessionLifeTTL, newLeaderCheckInterval);


    }

    /**
     * @param consul                 consul
     * @param serviceName            serviceName
     * @param reactor                reactor
     * @param timeUnit               timeUnit
     * @param sessionLifeTTL         sessionLifeTTL
     * @param newLeaderCheckInterval newLeaderCheckInterval
     */
    public ConsulLeadershipElector(final Consul consul,
                                   final String serviceName,
                                   final Reactor reactor,
                                   final TimeUnit timeUnit,
                                   final long sessionLifeTTL,
                                   final long newLeaderCheckInterval) {
        this.reactor = reactor;
        this.serviceName = serviceName;
        this.leadershipProvider = new ConsulLeadershipProvider(serviceName, consul, timeUnit, sessionLifeTTL);
        init(reactor, timeUnit, sessionLifeTTL, newLeaderCheckInterval);
    }


    /**
     * Elect this node
     *
     * @param endpoint this node
     * @param callback callback if we are successful
     */
    @Override
    public void selfElect(final Endpoint endpoint, final Callback<Boolean> callback) {

        requestExecutorService.submit(() -> {

            /* If sessionID is null then create a session before we continue. */
            if (sessionId.get() == null) {
                createSessionFromConsul(); //Blocking call
            }
            try {

                final boolean leader = leadershipProvider.electLeader(sessionId.get(), endpoint);

                this.leader.set(leader);

                if (this.leader.get()) {
                    currentLeader.set(endpoint);
                    notifyNewLeader(endpoint);
                }
                callback.reply(this.leader.get());
            } catch (Exception ex) {
                callback.reject("Unable to become leader. for service " + serviceName, ex);
                logger.error("Error electing leader " + endpoint + " for " + serviceName, ex);
                reset();
            }
        });


    }

    /**
     * Register for changes.
     *
     * @param leaderShipStream leaderShipStream
     */
    @Override
    public void leadershipChangeNotice(final Stream<Endpoint> leaderShipStream) {
        listeners.add(leaderShipStream);
    }

    @Override
    public void getLeader(final Callback<Endpoint> callback) {

        final Endpoint endpoint = currentLeader.get();

        /** If the current leader is set then just return the current leader */
        if (endpoint != null) {
            logger.debug("getLeader Leader Found {} Elected for {} ", endpoint, serviceName);
            callback.resolve(endpoint);
        } else {
            doLoadLeader(callback);
        }

    }


    /**
     * Notify all outgoing streams that we have a new leader.
     *
     * @param newLeaderEndpoint newLeaderEndpoint
     */
    private void notifyNewLeader(final Endpoint newLeaderEndpoint) {

        logger.debug("New Leader {} Elected for {} ", newLeaderEndpoint, serviceName);
        outExecutorService.submit(() -> {
                    listeners.stream().forEach(endpointStream ->
                            endpointStream.reply(newLeaderEndpoint, false,
                                    () -> listeners.remove(endpointStream)));

                }
        );
    }

    /**
     * Loads the leader
     *
     * @param callback optional callback
     */
    private void doLoadLeader(final Callback<Endpoint> callback) {
        logger.debug("doLoadLeader called");

        final Optional<Callback<Endpoint>> callbackOptional = Optional.ofNullable(callback);

        requestExecutorService.submit(() -> {
            /** Load the leader now, not with a long poll. */
            try {
                final Optional<Endpoint> leaderEndpoint = leadershipProvider.getLeader(modifyIndex);
                if (!leaderEndpoint.isPresent()) {
                    callbackOptional.ifPresent(endpointCallback -> endpointCallback.resolve(null));
                } else {
                    callbackOptional.ifPresent(endpointCallback -> endpointCallback.resolve(leaderEndpoint.get()));
                }
                setupLeader(leaderEndpoint);
            } catch (Exception ex) {
                logger.error("Error loading leader for service " + serviceName, ex);
                reset();
            }
        });

    }

    /**
     *
     */
    private void reset() {
        sessionId.set(null);
        leader.set(false);
        asyncCreateSession();
    }

    /**
     * Used to run reactor jobs.
     */
    public void process() {
        reactor.process();
    }


    /**
     * setup the leader from the optional
     *
     * @param leaderEndpoint leaderEndpoint
     */
    private void setupLeader(Optional<Endpoint> leaderEndpoint) {
        if (leaderEndpoint.isPresent()) {

            final Endpoint endpoint = leaderEndpoint.get();
            if (!endpoint.equals(currentLeader.get())) {
                logger.info("New Leader found " + endpoint);
                currentLeader.set(endpoint);
                notifyNewLeader(endpoint);
            }

        } else {
            leader.set(false);
            currentLeader.set(null);
        }
    }

    /**
     * Renew the session.
     */
    private void renewSession() {

        /** If the session id is present, then renew. */
        final String sessionId = this.sessionId.get();
        if (sessionId != null) {
            leadershipProvider.renewSession(sessionId, modifyIndex);
        } else {
            /** If the session is not present then create it. */
            createSessionFromConsul();
        }
    }


    private void createSessionFromConsul() {
        try {
            sessionId.set(leadershipProvider.createSession());
        } catch (Exception ex) {
            logger.error("Unable to create session", ex);
            leader.set(false);
            sessionId.set(null);
            throw new IllegalStateException(ex);
        }
    }


    /**
     * Startup background jobs
     *
     * @param reactor                reactor
     * @param timeUnit               timeUnit
     * @param sessionLifeTTL         sessionLifeTTL
     * @param newLeaderCheckInterval newLeaderCheckInterval
     */
    private void init(Reactor reactor, TimeUnit timeUnit, long sessionLifeTTL, long newLeaderCheckInterval) {
        asyncCreateSession();


        /** Load the session from consul every session TTL /2 */
        reactor.addRepeatingTask(Duration.ofSeconds(timeUnit.toSeconds(sessionLifeTTL) / 2),
                () -> requestExecutorService.submit(this::renewSession)
        );


        /** Load the session from consul every session TTL /2 */
        reactor.addRepeatingTask(Duration.ofSeconds(timeUnit.toSeconds(newLeaderCheckInterval)),
                () -> {
                    currentLeader.set(null);
                    doLoadLeader(null);
                });

        /** Setup long poll for leader change notifications. */
        longPollExecutorService.submit(() -> {
            while (true) {
                final Optional<Endpoint> leaderEndpoint = leadershipProvider.getLeaderLongPoll(modifyIndex);
                setupLeader(leaderEndpoint);
                Thread.sleep(1000);
            }
        });
    }

    private void asyncCreateSession() {
        /** Load the session from Consul right now. */
        requestExecutorService.submit(this::createSessionFromConsul);
    }


}
