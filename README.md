# Czar Maker Consul
Czar Maker implemented in Consul to do leadership election.

##Getting Started

#### Maven
```xml
<dependency>
    <groupId>io.advantageous.czarmaker</groupId>
    <artifactId>czar-maker-consul</artifactId>
    <version>0.1.0.RELEASE</version>
</dependency>
```

#### Gradle
```java
compile 'io.advantageous.czarmaker:czar-maker-consul:0.1.0.RELEASE'
```



#### Sample usage
```java

import io.advantageous.consul.Consul;
import io.advantageous.czarmaker.Endpoint;
import io.advantageous.czarmaker.consul.*;
import io.advantageous.qbit.util.TestTimer;
import io.advantageous.reakt.promise.Promise;
import io.advantageous.reakt.promise.Promises;
import io.advantageous.reakt.reactor.Reactor;
import io.advantageous.reakt.reactor.TimeSource;

...

    private final long sessionTTL = 10;
    private final long newLeaderCheckInterval = 5;
    private ConsulLeadershipElector leadershipElector;
    private Reactor reactor;
    private TestTimer testTimer;
    private Consul consul;

...
        consul = Consul.consul();
        testTimer = new TestTimer();
        testTimer.setTime();
        reactor = Reactor.reactor(Duration.ofSeconds(30), new TestTimeSource(testTimer));
        final String serviceName = "foo";

        ConsulLeadershipProvider provider = new ConsulLeadershipProvider(serviceName, consul, TimeUnit.SECONDS, sessionTTL);

        leadershipElector = new ConsulLeadershipElector(provider, serviceName, reactor, TimeUnit.SECONDS,
                sessionTTL, newLeaderCheckInterval);


        /** Get the current leader. */
        Promise<Endpoint> promise = Promises.<Endpoint>blockingPromise();
        leadershipElector.getLeader(promise);

        assertTrue(promise.expect().isEmpty());

        
        /** Elect this endpoint as the current leader. */
        Promise<Boolean> selfElectPromise = Promises.<Boolean>blockingPromise();
        leadershipElector.selfElect(new Endpoint("foo.com", 9091), selfElectPromise);

        assertTrue("We are now the leader", selfElectPromise.get());


        /** Get the current leader again.  */
        Promise<Endpoint> getLeaderPromise = Promises.<Endpoint>blockingPromise();
        leadershipElector.getLeader(getLeaderPromise);

        /** See if it present. */
        assertTrue(getLeaderPromise.expect().isPresent());

        /** See if it has the host foo.com. */
        assertEquals("foo.com", getLeaderPromise.get().getHost());

        /** See if the port is 9091. */
        assertEquals(9091, getLeaderPromise.get().getPort());

        testTimer.seconds(100);

        leadershipElector.process();

        /** Elect a new leader. */
        leadershipElector.selfElect(new Endpoint("foo2.com", 9092), selfElectPromise);

```
