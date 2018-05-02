This library provides a [Spring Integration] inbound channel adapter that will receive [Flume] events
sent from a [ThriftSink].

[Spring Integration]: https://projects.spring.io/spring-integration/
[Flume]: https://flume.apache.org/index.html
[ThriftSink]: https://flume.apache.org/FlumeUserGuide.html#thrift-sink

## Usage

### Spring Integration

```xml
<beans xmlns="http://www.springframework.org/schema/beans"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xmlns:int="http://www.springframework.org/schema/integration"
       xsi:schemaLocation="http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd
       http://www.springframework.org/schema/integration http://www.springframework.org/schema/integration/spring-integration.xsd">

    <bean class="com.rackspace.siflume.FlumeInboundChannelAdapter">
        <!-- Adapter specific config -->
        <property name="port" value="4141"/>
        <property name="bind" value="0.0.0.0"/>

        <!-- Spring Integration wiring -->
        <property name="outputChannel" ref="flumeThriftContentChannel"/>
        <property name="autoStartup" value="true"/>
    </bean>
    
    <!-- ... -->
</beans>
```

### Flume

```properties
a1.sinks.thriftSink.type = thrift
a1.sinks.thriftSink.hostname = localhost
a1.sinks.thriftSink.port = 4141
```