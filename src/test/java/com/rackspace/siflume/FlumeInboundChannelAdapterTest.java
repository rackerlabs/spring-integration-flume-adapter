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

import org.apache.flume.Context;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.sink.ThriftSink;
import org.junit.Test;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.messaging.Message;
import org.springframework.util.SocketUtils;

import java.util.HashMap;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.springframework.integration.test.matcher.PayloadMatcher.hasPayload;

public class FlumeInboundChannelAdapterTest {
    @Test
    public void testNormalFlow() throws Exception {
        final int port = SocketUtils.findAvailableTcpPort();
        final QueueChannel outputChannel = new QueueChannel();

        final FlumeInboundChannelAdapter flumeInboundChannelAdapter = new FlumeInboundChannelAdapter();
        flumeInboundChannelAdapter.setPort(port);
        flumeInboundChannelAdapter.setBind("127.0.0.1");
        flumeInboundChannelAdapter.setOutputChannel(outputChannel);
        flumeInboundChannelAdapter.onInit();
        flumeInboundChannelAdapter.start();

        final HashMap<String, String> flumeSinkProps = new HashMap<String, String>();
        flumeSinkProps.put("hostname", "127.0.0.1");
        flumeSinkProps.put("port", String.valueOf(port));

        final MemoryChannel flumeChannel = new MemoryChannel();
        flumeChannel.configure(new Context());
        flumeChannel.start();
        final ThriftSink thriftSink = new ThriftSink();
        thriftSink.setChannel(flumeChannel);

        try {
            thriftSink.configure(new Context(flumeSinkProps));
            thriftSink.start();

            try {
                final SimpleEvent event = new SimpleEvent();
                event.setBody("testing".getBytes());
                final Transaction tx = flumeChannel.getTransaction();
                try {
                    tx.begin();
                    flumeChannel.put(event);
                    tx.commit();
                } catch (Exception e) {
                    tx.rollback();
                    throw e;
                } finally {
                    tx.close();
                }

                thriftSink.process();

                assertThat(outputChannel.getQueueSize(), equalTo(1));
                final List<Message<?>> messages = outputChannel.clear();
                assertThat(messages.get(0), hasPayload(equalTo("testing".getBytes())));
            } finally {
                thriftSink.stop();
            }

        } finally {
            flumeInboundChannelAdapter.stop();
            flumeChannel.stop();
        }
    }
}