package io.advantageous.czarmaker.consul;

import io.advantageous.czarmaker.Endpoint;
import io.advantageous.czarmaker.LeaderElector;
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
import java.util.concurrent.atomic.AtomicReference;


public class ConsulLeadershipElector implements LeaderElector {

    protected final Logger logger = LoggerFactory.getLogger(ConsulLeadershipElector.class);


    private final Reactor reactor;
    private final String serviceName;
    private final LeadershipProvider leadershipProvider;
    protected AtomicReference<Endpoint> currentLeader = new AtomicReference<>();
    private final CopyOnWriteArrayList<Stream<Endpoint>> listeners = new CopyOnWriteArrayList<>();
    private AtomicBoolean leader = new AtomicBoolean();
    private AtomicReference<String> sessionId = new AtomicReference<>();
    private ExecutorService executorService = Executors.newSingleThreadExecutor();
    private ExecutorService outExecutorService = Executors.newSingleThreadExecutor();

    public ConsulLeadershipElector(final LeadershipProvider leadershipProvider,
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

    private void init(Reactor reactor, TimeUnit timeUnit, long sessionLifeTTL, long newLeaderCheckInterval) {
        /** Load the session from Consul right now. */
        executorService.submit((Runnable) this::getSessionFromConsul);

        /** Load the session from consul every session TTL /2 */
        reactor.addRepeatingTask(Duration.ofSeconds(timeUnit.toSeconds(sessionLifeTTL)/2) ,
                () -> executorService.submit((Runnable) this::getSessionFromConsul)
        );


        /** Load the session from consul every session TTL /2 */
        reactor.addRepeatingTask(Duration.ofSeconds(timeUnit.toSeconds(newLeaderCheckInterval)),
                () -> {
                    currentLeader.set(null);
                    doLoadLeader(null);
                });
    }


    @Override
    public void selfElect(final Endpoint endpoint, final Callback<Boolean> callback) {



        executorService.submit((Runnable) () -> {
            if (sessionId.get() == null) {
                getSessionFromConsul(); //Blocking call
            }
            try {

                final boolean leader = leadershipProvider.electLeader(sessionId.get(), endpoint);

                this.leader.set(leader);

                if (this.leader.get()) {
                    currentLeader.set(endpoint);
                    notifyNewLeader(endpoint);
                }
                callback.reply(this.leader.get());
            }catch (Exception ex) {
                callback.reject("Unable to become leader. for service " + serviceName, ex);
            }
        });


    }

    private void notifyNewLeader(Endpoint newLeaderEndpoint) {

        outExecutorService.submit((Runnable) () -> {
                    logger.debug("New Leader {} Elected for {} ", newLeaderEndpoint, serviceName);
                    listeners.stream().forEach(endpointStream ->
                            endpointStream.reply(newLeaderEndpoint, false,
                                    () -> listeners.remove(endpointStream)));

                }
            );
    }

    private void getSessionFromConsul() {
        try {
            sessionId.set(leadershipProvider.createSession());
        }catch (Exception ex) {
            logger.error("Unable to create session", ex);
            throw new IllegalStateException(ex);
        }
    }

    @Override
    public void leadershipChangeNotice(Stream<Endpoint> leaderShipStream) {
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

    private void doLoadLeader(final Callback<Endpoint> callback) {

        Optional<Callback<Endpoint>> callbackOptional = Optional.ofNullable(callback);

        logger.debug("doLoadLeader called");

        executorService.submit((Runnable) () -> {

            final Optional<Endpoint> leaderEndpoint = leadershipProvider.getLeader();

            if (!leaderEndpoint.isPresent()) {
                callbackOptional.ifPresent(endpointCallback -> endpointCallback.resolve(null));
            } else {

                callbackOptional.ifPresent(endpointCallback -> endpointCallback.resolve(leaderEndpoint.get()));

                /* If the current leader is not null, then check to see if this endpoint we loaded
                is the same as the current leader. If it is not the same, notifyNewLeader stream.
                 */
                if (currentLeader.get()!=null) {
                    if (!currentLeader.get().equals(leaderEndpoint.get())) {
                        notifyNewLeader(leaderEndpoint.get());
                    }
                }
                /*
                 * If the current leader is null then send this leader out.
                 */
                else {
                    currentLeader.set(leaderEndpoint.get());
                    notifyNewLeader(leaderEndpoint.get());
                }
            }
        });

    }

    public void process() {
        reactor.process();
    }
}
