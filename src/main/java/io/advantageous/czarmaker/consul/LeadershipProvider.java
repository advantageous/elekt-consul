package io.advantageous.czarmaker.consul;

import io.advantageous.czarmaker.Endpoint;

import java.util.Optional;

public interface LeadershipProvider {

    boolean electLeader(final String sessionId, Endpoint endpoint);
    Optional<Endpoint> getLeader();
    String createSession();
    boolean renewSession(String sessionId);
}
