package io.advantageous.czarmaker.consul;

import io.advantageous.consul.Consul;
import io.advantageous.czarmaker.Endpoint;
import io.advantageous.qbit.util.TestTimer;
import io.advantageous.reakt.Stream;
import io.advantageous.reakt.StreamResult;
import io.advantageous.reakt.promise.Promise;
import io.advantageous.reakt.promise.Promises;
import io.advantageous.reakt.reactor.Reactor;
import io.advantageous.reakt.reactor.TimeSource;
import org.junit.Test;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.junit.Assert.*;

public class ConsulLeadershipElectorTest {

    private ConsulLeadershipElector leadershipElector;
    private Reactor reactor;
    private TestTimer testTimer;
    private Consul consul;
    private final long sessionTTL = 10;
    private final long newLeaderCheckInterval = 5;
    private LeadershipProviderMock leadershipProviderMock;


    @Test
    public void testSelfElect() throws Exception {

        leadershipProviderMock = new LeadershipProviderMock();
        testTimer= new TestTimer();
        testTimer.setTime();
        reactor = Reactor.reactor(Duration.ofSeconds(30), new TestTimeSource(testTimer));
        final String serviceName = "foo";

        AtomicReference<Endpoint> endpointAtomicReference = new AtomicReference<>();



        leadershipElector = new ConsulLeadershipElector(leadershipProviderMock, serviceName, reactor, TimeUnit.SECONDS,
                sessionTTL, newLeaderCheckInterval);

        leadershipElector.leadershipChangeNotice(result -> {
            result.then(endpointAtomicReference::set);
            result.cancel();
        });


        Promise<Endpoint> promise = Promises.<Endpoint>blockingPromise();
        leadershipElector.getLeader(promise);

        assertTrue(promise.expect().isEmpty());


        Promise<Boolean> selfElectPromise = Promises.<Boolean>blockingPromise();
        leadershipElector.selfElect(new Endpoint("foo.com", 9091), selfElectPromise);

        assertTrue("We are now the leader", selfElectPromise.get());



        Promise<Endpoint> getLeaderPromise = Promises.<Endpoint>blockingPromise();
        leadershipElector.getLeader(getLeaderPromise);

        assertTrue(getLeaderPromise.expect().isPresent());

        assertEquals("foo.com", getLeaderPromise.get().getHost());

        assertEquals(9091, getLeaderPromise.get().getPort());

        testTimer.seconds(100);
        Thread.sleep(1000);

        leadershipElector.process();

        leadershipElector.selfElect(new Endpoint("foo2.com", 9092), selfElectPromise);




        leadershipProviderMock.clearLeader();

        testTimer.seconds(100);
        Thread.sleep(10);


        leadershipElector.process();


        leadershipProviderMock.electLeader("789", new Endpoint("zzz.com", 9092));
        leadershipElector.currentLeader.set(null);

        testTimer.seconds(100);
        Thread.sleep(10);

        leadershipElector.process();






        Promise<Endpoint> getLeaderPromise2 = Promises.<Endpoint>blockingPromise();
        leadershipElector.getLeader(getLeaderPromise2);


        assertNotNull(endpointAtomicReference.get());

        assertEquals("zzz.com", getLeaderPromise2.get().getHost());


    }


    public static class LeadershipProviderMock implements LeadershipProvider {

        AtomicReference<Endpoint> leaderEndpoint = new AtomicReference<>();
        AtomicBoolean rejectNewLeader = new AtomicBoolean();

        @Override
        public boolean electLeader(String sessionId, Endpoint endpoint) {
            if (rejectNewLeader.get()) {
                return false;
            }
            leaderEndpoint.set(endpoint);
            return true;
        }

        @Override
        public Optional<Endpoint> getLeader() {
            return Optional.ofNullable(leaderEndpoint.get());
        }

        void clearLeader() {
            leaderEndpoint.set(null);
        }

        @Override
        public String createSession() {
            return "1234567";
        }

        @Override
        public boolean renewSession(String sessionId) {
            return true;
        }
    }


    static class TestTimeSource implements TimeSource {

        final TestTimer testTimer;

        TestTimeSource(TestTimer testTimer) {
            this.testTimer = testTimer;
        }

        @Override
        public long getTime() {
            return testTimer.time();
        }
    }
}