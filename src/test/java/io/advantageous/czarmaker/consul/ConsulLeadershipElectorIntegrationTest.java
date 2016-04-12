package io.advantageous.czarmaker.consul;

import io.advantageous.consul.Consul;
import io.advantageous.czarmaker.Endpoint;
import io.advantageous.qbit.util.TestTimer;
import io.advantageous.reakt.promise.Promise;
import io.advantageous.reakt.promise.Promises;
import io.advantageous.reakt.reactor.Reactor;
import io.advantageous.reakt.reactor.TimeSource;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ConsulLeadershipElectorIntegrationTest {

    private ConsulLeadershipElector leadershipElector;
    private Reactor reactor;
    private TestTimer testTimer;
    private Consul consul;
    private final long sessionTTL = 10;
    private final long newLeaderCheckInterval = 5;


    @Test
    public void testSelfElect() throws Exception {
        consul = Consul.consul();
        testTimer= new TestTimer();
        testTimer.setTime();
        reactor = Reactor.reactor(Duration.ofSeconds(30), new TestTimeSource(testTimer));
        final String serviceName = "foo";

        ConsulLeadershipProvider provider = new ConsulLeadershipProvider(serviceName, consul, TimeUnit.SECONDS, sessionTTL);

        leadershipElector = new ConsulLeadershipElector(provider, serviceName, reactor, TimeUnit.SECONDS,
                sessionTTL, newLeaderCheckInterval);


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

        leadershipElector.process();

        leadershipElector.selfElect(new Endpoint("foo2.com", 9092), selfElectPromise);



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