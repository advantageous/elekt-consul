package io.advantageous.elekt.consul;

import io.advantageous.boon.json.JsonFactory;
import io.advantageous.consul.Consul;
import io.advantageous.consul.domain.KeyValue;
import io.advantageous.consul.domain.Session;
import io.advantageous.consul.domain.SessionBehavior;
import io.advantageous.consul.domain.option.Consistency;
import io.advantageous.consul.domain.option.KeyValuePutOptions;
import io.advantageous.consul.domain.option.RequestOptions;
import io.advantageous.consul.endpoints.KeyValueStoreEndpoint;
import io.advantageous.consul.endpoints.SessionEndpoint;
import io.advantageous.czarmaker.Endpoint;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static io.advantageous.boon.json.JsonFactory.fromJson;

/**
 * This is a wrapper over consul to make the ConsulLeaderShipElector more testable.
 */
class ConsulLeadershipProvider implements LeadershipProvider {

    /**
     * Path pattern to lock user in kvstore.
     */
    private static final String PATH = "service/%s/leader";
    /**
     * Service name.
     */
    private final String serviceName;
    /**
     * Time unit for session TTL.
     */
    private final TimeUnit timeUnit;
    /**
     * Session TTL.
     */
    private final long sessionLifeTTL;
    /**
     * Consul KV store.
     */
    private final KeyValueStoreEndpoint kvStore;
    /**
     * Session endpoint.
     */
    private final SessionEndpoint sessionManager;
    /**
     * The actual path of the key to lock the session with.
     */
    private final String path;


    /**
     * @param serviceName    service name
     * @param consul         consul
     * @param timeUnit       time unit
     * @param sessionLifeTTL session TTL
     */
    public ConsulLeadershipProvider(String serviceName, Consul consul, TimeUnit timeUnit,
                                    long sessionLifeTTL) {
        this.serviceName = serviceName;
        this.timeUnit = timeUnit;
        this.sessionLifeTTL = sessionLifeTTL;
        this.kvStore = consul.keyValueStore();
        this.sessionManager = consul.session();
        path = String.format(PATH, serviceName);
    }


    /**
     * Elect a new leader.
     *
     * @param sessionId session id
     * @param endpoint  endpoint of proposed new leader
     * @return returns true if we get success.
     */
    @Override
    public boolean electLeader(final String sessionId,
                               final Endpoint endpoint) {
        return kvStore.putValue(path,
                JsonFactory.toJson(endpoint), 0L, new KeyValuePutOptions(null, sessionId, null));
    }

    /**
     * Get the leader.
     *
     * @param modifyIndex modifyIndex of last change to the leader key.
     * @return Leader we have one.
     */
    @Override
    public Optional<Endpoint> getLeader(final AtomicLong modifyIndex) {
        final Optional<KeyValue> keyValue = kvStore.getValue(path);

        if (!keyValue.isPresent()) {
            return Optional.empty();
        }


        /* Increase the modify index so long polling will work. */
        modifyIndex.set(keyValue.get().getModifyIndex());

        /**
         * Get the value from the kv pair and convert it into an Endpoint.
         */
        final String value = keyValue.get().getValue();
        final Endpoint endpoint = fromJson(value, Endpoint.class);
        return Optional.of(endpoint);
    }


    /**
     * Get the leader but poll for 30seconds  until there is a change.
     *
     * @param modifyIndex modifyIndex of last change of leader key.
     * @return Leader we have one.
     */
    @Override
    public Optional<Endpoint> getLeaderLongPoll(final AtomicLong modifyIndex) {

        /** Note they we perform the query with the modify index and we wait until something newer comes along. */
        final Optional<KeyValue> keyValue = kvStore.getValue(PATH,
                new RequestOptions("30s", (int) modifyIndex.get(), Consistency.DEFAULT));
        if (!keyValue.isPresent()) {
            return Optional.empty();
        }

        /* Increase the modify index so long polling will work. */
        modifyIndex.set(keyValue.get().getModifyIndex());
        final String value = keyValue.get().getValue();
        final Endpoint endpoint = fromJson(value, Endpoint.class);
        return Optional.of(endpoint);
    }

    /**
     * Create a new session and return the id.
     *
     * @return id of new session.
     */
    @Override
    public String createSession() {
        Session session = new Session();
        session.setName("serviceLeaderLock-" + serviceName).setSessionBehavior(SessionBehavior.DELETE)
                .setTtlSeconds(timeUnit.toSeconds(sessionLifeTTL));
        return sessionManager.create(session);
    }

    /**
     * Renew a session.
     *
     * @param sessionId sessionId
     * @param index     modify index.
     * @return
     */
    @Override
    public boolean renewSession(String sessionId, final AtomicLong index) {

        final Optional<Session> renew = Optional.ofNullable(sessionManager.renew(sessionId));

        renew.ifPresent(session -> {
            if (session.getModifyIndex() != 0) {
                index.set(session.getModifyIndex());
            }
        });

        return renew.isPresent();
    }
}
