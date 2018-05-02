/*
 * Copyright 2018 Rackspace US, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.rackspace.siflume;

import org.apache.flume.*;
import org.apache.flume.channel.AbstractChannel;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.PseudoTxnMemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.source.ThriftSource;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.messaging.support.MessageBuilder;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Spring Integration inbound channel adapter that starts a Flume {@link ThriftSource} and delivers
 * incoming events into the configured Spring Integration channel.
 * 
 * <p>
 *     The following is an example Spring Integration XML config:
 *     <pre>
 &lt;beans xmlns="http://www.springframework.org/schema/beans"
 xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 xmlns:int="http://www.springframework.org/schema/integration"
 xsi:schemaLocation="http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd
 http://www.springframework.org/schema/integration http://www.springframework.org/schema/integration/spring-integration.xsd"&gt;

     &lt;bean class="com.rackspace.siflume.FlumeInboundChannelAdapter"&gt;
         &lt;!-- Adapter specific config --&gt;
         &lt;property name="port" value="4141"/&gt;
         &lt;property name="bind" value="0.0.0.0"/&gt;

         &lt;!-- Spring Integration wiring --&gt;
         &lt;property name="outputChannel" ref="flumeLogging"/&gt;
         &lt;property name="autoStartup" value="true"/&gt;
     &lt;/bean&gt;

     &lt;int:channel id="flumeLogging"/&gt;
     &lt;int:logging-channel-adapter channel="flumeLogging" log-full-message="true"
     logger-name="flow.from-flume" auto-startup="true"/&gt;

 &lt;/beans&gt;
 *     </pre>
 * </p>
 *
 * <p>
 *     The following is an example Thrift sink configured in Flume that can send to this adapter:
 *     <pre>
 test.sinks.thriftSink.type = thrift
 test.sinks.thriftSink.hostname = localhost
 test.sinks.thriftSink.port = 4141
 *     </pre>
 * </p>
 */
public class FlumeInboundChannelAdapter extends MessageProducerSupport {

    private ThriftSource thriftSource;

    private Map<String,String> thriftSourceProperties;

    String bind = "127.0.0.1";
    int port = 4141;

    public String getBind() {
        return bind;
    }

    public void setBind(String bind) {
        this.bind = bind;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @Override
    protected void onInit() {

        if (thriftSourceProperties == null) {
            thriftSourceProperties = new HashMap<String, String>();
        }
        thriftSourceProperties.put(ThriftSource.CONFIG_BIND, bind);
        thriftSourceProperties.put(ThriftSource.CONFIG_PORT, String.valueOf(port));

        thriftSource = new ThriftSource();
        Context context = new Context(thriftSourceProperties);
        thriftSource.configure(context);

        // wire it up to spring integration by giving it what it thinks is a flume channel
        ChannelSelector channelSelector = new ReplicatingChannelSelector();
        channelSelector.setChannels(Collections.<Channel>singletonList(new FlumeChannelAdapter()));
        ChannelProcessor cp = new ChannelProcessor(channelSelector);
        thriftSource.setChannelProcessor(cp);

        super.onInit();
    }

    @Override
    protected void doStart() {
        thriftSource.start();
    }

    @Override
    protected void doStop() {
        thriftSource.stop();
    }

    public Map<String, String> getThriftSourceProperties() {
        return thriftSourceProperties;
    }

    public void setThriftSourceProperties(Map<String, String> thriftSourceProperties) {
        this.thriftSourceProperties = thriftSourceProperties;
    }

    private class FlumeChannelAdapter extends AbstractChannel {
        public void put(Event event) throws ChannelException {
            // This is where we convert from a Flume event to a Spring Integration Message
            FlumeInboundChannelAdapter.this.sendMessage(
                    MessageBuilder.withPayload(event.getBody())
                    .build()
            );
        }

        public Event take() throws ChannelException {
            return null;
        }

        public Transaction getTransaction() {
            return PseudoTxnMemoryChannel.NoOpTransaction.sharedInstance();
        }
    }
}
