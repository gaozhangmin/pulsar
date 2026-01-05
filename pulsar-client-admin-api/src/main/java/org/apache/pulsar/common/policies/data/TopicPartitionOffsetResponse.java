/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.common.policies.data;

import java.util.List;

/**
 * Response data for topic partition offset lookup operations.
 */
public class TopicPartitionOffsetResponse {
    private String topic;
    private Integer partition;
    private Long offset;
    private Long ledgerId;
    private Long entryId;
    private Long logStartOffset;
    private Long logEndOffset;
    private Long latency;
    private LedgerMetadata ledgerMetadata;
    private String getLedgerCommand;

    public TopicPartitionOffsetResponse() {
    }

    public TopicPartitionOffsetResponse(String topic, Integer partition, Long offset, Long ledgerId,
                                      Long entryId, Long logStartOffset, Long logEndOffset, Long latency,
                                      LedgerMetadata ledgerMetadata, String getLedgerCommand) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.logStartOffset = logStartOffset;
        this.logEndOffset = logEndOffset;
        this.latency = latency;
        this.ledgerMetadata = ledgerMetadata;
        this.getLedgerCommand = getLedgerCommand;
    }

    // Getters and Setters
    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getPartition() {
        return partition;
    }

    public void setPartition(Integer partition) {
        this.partition = partition;
    }

    public Long getOffset() {
        return offset;
    }

    public void setOffset(Long offset) {
        this.offset = offset;
    }

    public Long getLedgerId() {
        return ledgerId;
    }

    public void setLedgerId(Long ledgerId) {
        this.ledgerId = ledgerId;
    }

    public Long getEntryId() {
        return entryId;
    }

    public void setEntryId(Long entryId) {
        this.entryId = entryId;
    }

    public Long getLogStartOffset() {
        return logStartOffset;
    }

    public void setLogStartOffset(Long logStartOffset) {
        this.logStartOffset = logStartOffset;
    }

    public Long getLogEndOffset() {
        return logEndOffset;
    }

    public void setLogEndOffset(Long logEndOffset) {
        this.logEndOffset = logEndOffset;
    }

    public Long getLatency() {
        return latency;
    }

    public void setLatency(Long latency) {
        this.latency = latency;
    }

    public LedgerMetadata getLedgerMetadata() {
        return ledgerMetadata;
    }

    public void setLedgerMetadata(LedgerMetadata ledgerMetadata) {
        this.ledgerMetadata = ledgerMetadata;
    }

    public String getGetLedgerCommand() {
        return getLedgerCommand;
    }

    public void setGetLedgerCommand(String getLedgerCommand) {
        this.getLedgerCommand = getLedgerCommand;
    }

    /**
     * Nested class for ledger metadata information.
     */
    public static class LedgerMetadata {
        private Long creationTime;
        private Long length;
        private Long lastEntryId;
        private String state;
        private List<String> ensemble;
        private Integer writeQuorum;
        private Integer ackQuorum;

        public LedgerMetadata() {
        }

        public LedgerMetadata(Long creationTime, Long length, Long lastEntryId, String state,
                             List<String> ensemble, Integer writeQuorum, Integer ackQuorum) {
            this.creationTime = creationTime;
            this.length = length;
            this.lastEntryId = lastEntryId;
            this.state = state;
            this.ensemble = ensemble;
            this.writeQuorum = writeQuorum;
            this.ackQuorum = ackQuorum;
        }

        // Getters and Setters
        public Long getCreationTime() {
            return creationTime;
        }

        public void setCreationTime(Long creationTime) {
            this.creationTime = creationTime;
        }

        public Long getLength() {
            return length;
        }

        public void setLength(Long length) {
            this.length = length;
        }

        public Long getLastEntryId() {
            return lastEntryId;
        }

        public void setLastEntryId(Long lastEntryId) {
            this.lastEntryId = lastEntryId;
        }

        public String getState() {
            return state;
        }

        public void setState(String state) {
            this.state = state;
        }

        public List<String> getEnsemble() {
            return ensemble;
        }

        public void setEnsemble(List<String> ensemble) {
            this.ensemble = ensemble;
        }

        public Integer getWriteQuorum() {
            return writeQuorum;
        }

        public void setWriteQuorum(Integer writeQuorum) {
            this.writeQuorum = writeQuorum;
        }

        public Integer getAckQuorum() {
            return ackQuorum;
        }

        public void setAckQuorum(Integer ackQuorum) {
            this.ackQuorum = ackQuorum;
        }
    }
}