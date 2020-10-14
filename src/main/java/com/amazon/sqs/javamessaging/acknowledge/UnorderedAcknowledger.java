/*
 * Copyright 2010-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazon.sqs.javamessaging.acknowledge;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.jms.JMSException;

import com.amazon.sqs.javamessaging.AmazonSQSMessagingClientWrapper;
import com.amazon.sqs.javamessaging.SQSSession;
import com.amazon.sqs.javamessaging.message.SQSMessage;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;

/**
 * Used to acknowledge messages in any order one at a time.
 * <P>
 * This class is not safe for concurrent use.
 */
public class UnorderedAcknowledger implements Acknowledger {

    private final AmazonSQSMessagingClientWrapper amazonSQSClient;
    
    private final SQSSession session;
    
    // key is the receipt handle of the message and value is the message
    // identifier
    private final Map<String, SQSMessageIdentifier> unAckMessages;

    private static Integer maxUnackdMessages;

    public UnorderedAcknowledger (AmazonSQSMessagingClientWrapper amazonSQSClient, SQSSession session) {
        this.amazonSQSClient = amazonSQSClient;
        this.session = session;

        // Use a regular hashmap if we don't set the envvar for max unacknowledged messages,
        // Otherwise, use a LinkedHashMap we limit the max size of to what we've set as MAX_UNACKNOWLEDGED_MESSAGES.
        this.unAckMessages = getMaxUnackdMessages() > 0 ? new LinkedHashMap<String, SQSMessageIdentifier>() {
            @Override
            protected boolean removeEldestEntry(Entry<String, SQSMessageIdentifier> eldest) {
                return size() > getMaxUnackdMessages();
            }
        } : new HashMap<String, SQSMessageIdentifier>();
    }

    /**
     * Acknowledges the consumed message via calling <code>deleteMessage</code>.
     */
    @Override
    public void acknowledge(SQSMessage message) throws JMSException {
        session.checkClosed();
        amazonSQSClient.deleteMessage(new DeleteMessageRequest(
                message.getQueueUrl(), message.getReceiptHandle()));
        unAckMessages.remove(message.getReceiptHandle());
    }
    
    /**
     * Updates the internal data structure for the consumed but not acknowledged
     * message.
     */
    @Override
    public void notifyMessageReceived(SQSMessage message) throws JMSException {
        SQSMessageIdentifier messageIdentifier = SQSMessageIdentifier.fromSQSMessage(message);
        unAckMessages.put(message.getReceiptHandle(), messageIdentifier);
    }
    
    /**
     * Returns the list of all consumed but not acknowledged messages.
     */
    @Override
    public List<SQSMessageIdentifier> getUnAckMessages() {
        return new ArrayList<SQSMessageIdentifier>(unAckMessages.values());
    }
    
    /**
     * Clears the list of not acknowledged messages.
     */
    @Override
    public void forgetUnAckMessages() {
        unAckMessages.clear();
    }

    /*
     * Check a 'MAX_UNACKNOWLEDGED_MESSAGES' environment variable which can be defined as a positive integer value
     * specifying the maximum number of unacknowledged messages we should hold on to. If we deal with more than that
     * number of unacknowledged messages, then we 'forget' older unacknowledged messages in a FIFO manner. This means
     * that calls to the 'getUnAckMessages()' method won't return the old messages, and 'forgetUnAckMessages()' won't
     * clear them.
     */
    private static synchronized int getMaxUnackdMessages() {
        if (maxUnackdMessages != null) {
            return maxUnackdMessages;
        }
        maxUnackdMessages = -1;
        final String envVal = System.getenv("MAX_UNACKNOWLEDGED_MESSAGES");
        if (envVal != null) {
            try {
                maxUnackdMessages = Integer.parseInt(envVal);
            } catch (NumberFormatException ex) {
                // do nothing - will return default value of no max
            }
        }
        return maxUnackdMessages;
    }
}
