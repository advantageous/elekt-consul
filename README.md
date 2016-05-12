# Elekt Consul
[Elekt is a Java](http://advantageous.github.io/elekt/) lib for leadership election. 
***Elekt Consul*** uses [Consul](https://www.consul.io/) to 
do [leadership election](https://www.consul.io/docs/guides/leader-election.html).

(This project was called Czar Maker Consul but changed. 
We have Reflekt, Reakt, Conekt and now Elekt.)

##Getting Started

#### Maven
```xml
<dependency>
    <groupId>io.advantageous.elekt</groupId>
    <artifactId>elekt-consul</artifactId>
    <version>0.1.0.RELEASE</version>
</dependency>
```

#### Gradle
```java
compile 'io.advantageous.elekt:elekt-consul:0.1.0.RELEASE'
```



#### Sample usage
```java

import io.advantageous.consul.Consul;
import io.advantageous.elekt.Endpoint;
import io.advantageous.elekt.consul.*;
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


## Starting up consul in docker

```
 docker run -p 8400:8400 -p 8500:8500 \
            -p 8600:53/udp -h node1 \
            progrium/consul -server -bootstrap -ui-dir /ui
```

The above running on OSX should equate to [http://192.168.99.100:8500/ui/#/dc1/services](http://192.168.99.100:8500/ui/#/dc1/services).
Check your $DOCKER_HOST env if that is not the case.


## Related projects
* [QBit Reactive Microservices](http://advantageous.github.io/qbit/)
* [Reakt Reactive Java](http://advantageous.github.io/reakt)
* [Reakt Guava Bridge](http://advantageous.github.io/reakt-guava/)
* [QBit Extensions](https://github.com/advantageous/qbit-extensions)
* [Elekt Consul](http://advantageous.github.io/elekt-consul/)
* [Elekt](http://advantageous.github.io/elekt/)
* [Reactive Microservices](http://www.mammatustech.com/reactive-microservices)

