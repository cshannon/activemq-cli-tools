/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.cli.kahadb.exporter.artemis;

import javax.jms.JMSException;

import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.reader.TextMessageUtil;
import org.apache.activemq.artemis.utils.Base64;
import org.apache.activemq.cli.kahadb.exporter.OpenWireMessageConverter;
import org.apache.activemq.cli.schema.BodyType;
import org.apache.activemq.cli.schema.MessageType;
import org.apache.activemq.cli.schema.PropertiesType;
import org.apache.activemq.cli.schema.PropertyType;
import org.apache.activemq.cli.schema.QueueType;
import org.apache.activemq.cli.schema.QueuesType;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.Message;
import org.fusesource.hawtbuf.UTF8Buffer;

public class ArtemisMessageConverter implements OpenWireMessageConverter<MessageType> {

    /* (non-Javadoc)
     * @see org.apache.activemq.cli.kahadb.exporter.MessageConverter#convert(org.apache.activemq.Message)
     */
    @Override
    public MessageType convert(final Message message) {
        final MessageType messageType = convertAttributes(message);

        try {
            if (!message.getProperties().isEmpty()) {
                final PropertiesType propertiesType = new PropertiesType();
                message.getProperties().forEach((k, v) -> {
                    propertiesType.getProperty().add(PropertyType.builder()
                            .withName(k)
                            .withValueAttribute(v.toString())
                            .withType(convertPropertyType(v.getClass()))
                            .build());
                });
                messageType.setProperties(propertiesType);
            }

            messageType.setQueues(convertQueue(message));
            messageType.setBody(convertBody(message));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }

        return messageType;
    }

    private QueuesType convertQueue(final Message message) throws JMSException {

        return QueuesType.builder()
                .withQueue(QueueType.builder()
                        .withName(message.getDestination().getPhysicalName()).build())
            .build();
    }

    private BodyType convertBody(final Message message) throws JMSException {
        ActiveMQTextMessage tm = (ActiveMQTextMessage) message;
        ActiveMQBuffer buff = ActiveMQBuffers.dynamicBuffer(tm.getText().length());
        TextMessageUtil.writeBodyText(buff, new SimpleString(tm.getText()));
        byte[] bytes = buff.byteBuf().array();
        String value = Base64.encodeBytes(bytes, 0, bytes.length, Base64.DONT_BREAK_LINES | Base64.URL_SAFE);

        return BodyType.builder()
            .withValue("<![CDATA[" + value + "]]>")
            .build();
    }

    private MessageType convertAttributes(final Message message) {
        MessageType messageType = MessageType.builder()
                .withId(message.getMessageId().getProducerSequenceId())
                .withTimestamp(message.getTimestamp())
                .withPriority(message.getPriority()).build();

        if (message instanceof ActiveMQTextMessage) {
            messageType.setType("text");
        }

        return messageType;
    }

    private String convertPropertyType(Class<?> clazz) {
        if (clazz.equals(UTF8Buffer.class)) {
            return String.class.getSimpleName().toLowerCase();
        } else {
            return clazz.getSimpleName().toLowerCase();
        }
    }

}
