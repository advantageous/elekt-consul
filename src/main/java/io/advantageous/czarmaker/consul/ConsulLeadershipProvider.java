package io.advantageous.czarmaker.consul;

import io.advantageous.boon.json.JsonFactory;
import io.advantageous.consul.Consul;
import io.advantageous.consul.domain.KeyValue;
import io.advantageous.consul.domain.Session;
import io.advantageous.consul.domain.SessionBehavior;
import io.advantageous.consul.domain.option.KeyValuePutOptions;
import io.advantageous.consul.endpoints.KeyValueStoreEndpoint;
import io.advantageous.consul.endpoints.SessionEndpoint;
import io.advantageous.czarmaker.Endpoint;
import io.advantageous.reakt.reactor.Reactor;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.advantageous.boon.json.JsonFactory.fromJson;

public class ConsulLeadershipProvider implements LeadershipProvider {

    final String serviceName;

    final Reactor reactor;
    final TimeUnit timeUnit;
    final long sessionLifeTTL;

    private final KeyValueStoreEndpoint kvStore;
    private final SessionEndpoint sessionManager;
    private final String PATH = "service/%s/leader";
    private final String path;


    public ConsulLeadershipProvider(String serviceName, Consul consul, Reactor reactor, TimeUnit timeUnit,
                                    long sessionLifeTTL) {
        this.serviceName = serviceName;
        this.reactor = reactor;
        this.timeUnit = timeUnit;
        this.sessionLifeTTL = sessionLifeTTL;
        this.kvStore = consul.keyValueStore();
        this.sessionManager = consul.session();
        path = String.format(PATH, serviceName);
    }


    @Override
    public boolean electLeader(final String sessionId, Endpoint endpoint) {
        return kvStore.putValue(path,
                JsonFactory.toJson(endpoint), 0L, new KeyValuePutOptions(null, sessionId, null));
    }

    @Override
    public Optional<Endpoint> getLeader() {
        final Optional<KeyValue> keyValue = kvStore.getValue(path);
        if (!keyValue.isPresent()) {
            return Optional.empty();
        }
        final String value = keyValue.get().getValue();
        final Endpoint endpoint = fromJson(value, Endpoint.class);
        return Optional.of(endpoint);
    }

    @Override
    public String createSession() {
        Session session = new Session();
        session.setName("serviceLeaderLock").setSessionBehavior(SessionBehavior.DELETE)
                .setTtlSeconds(timeUnit.toSeconds(sessionLifeTTL));
        return sessionManager.create(session);

    }

    @Override
    public boolean renewSession(String sessionId) {
        return sessionManager.renew(sessionId) != null;
    }
}
