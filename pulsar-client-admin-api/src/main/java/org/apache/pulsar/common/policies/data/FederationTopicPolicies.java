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

import java.util.Map;
import lombok.Getter;

/**
 * Definition of the retention policy.
 *
 * <p>When you set a retention policy you must set **both** a *size limit* and a *time limit*.
 * In the case where you don't want to limit by either time or size, the value must be set to `-1`.
 * Retention policy will be effectively disabled and it won't prevent the deletion of acknowledged
 * messages when either size or time limit is set to `0`.
 * Infinite retention can be achieved by setting both time and size limits to `-1`.
 */
@Getter
public class FederationTopicPolicies {
    public static final String FEDERATION_SOURCE_CLUSTER = "source_cluster";
    public static final String FEDERATION_SOURCE_BOOT_ADDRESS = "source_boot_address";
    public static final String FEDERATION_SOURCE_ZK_ADDRESS = "source_zk_address";
    public static final String FEDERATION_CAN_READ = "can_read";
    public static final String FEDERATION_CAN_WRITE = "can_write";
    public static final String FEDERATION_MIGRATE_BACK = "migrate_back";
    public static final String FEDERATION_TYPE = "type";

    public static final String TYPE_MIGRATING = "migrating";
    public static final String TYPE_MIGRATED = "migrated";
    public static final String TYPE_NORMAL = "normal";

    private String sourceCluster;
    private String sourceBootstrap;
    private String sourceZkAddress;
    private Boolean canRead;
    private Boolean canWrite;
    private Boolean migrateBack;
    private String type;

    public FederationTopicPolicies() {
        this("", "", "", false, false, false, TYPE_NORMAL);
    }

    public FederationTopicPolicies(String sourceCluster, String sourceBootstrap,
                                   String sourceZkAddress, Boolean canRead,
            Boolean canWrite, Boolean migrateBack, String type) {
        this.sourceCluster = sourceCluster;
        this.sourceBootstrap = sourceBootstrap;
        this.sourceZkAddress = sourceZkAddress;
        this.canRead = canRead;
        this.canWrite = canWrite;
        this.migrateBack = migrateBack;
        this.type = type;
    }

    public static FederationTopicPolicies createFederationTopicPolicies(Map<String, String> topicConfig) {
        if (topicConfig == null) {
            return null;
        }
        if (topicConfig.containsKey(FEDERATION_SOURCE_CLUSTER)
                && topicConfig.containsKey(FEDERATION_SOURCE_BOOT_ADDRESS)
                && topicConfig.containsKey(FEDERATION_SOURCE_ZK_ADDRESS)
                && topicConfig.containsKey(FEDERATION_CAN_READ)
                && topicConfig.containsKey(FEDERATION_CAN_WRITE)) {
            FederationTopicPolicies federationTopicPolicies = new FederationTopicPolicies(
                    topicConfig.get(FEDERATION_SOURCE_CLUSTER),
                    topicConfig.get(FEDERATION_SOURCE_BOOT_ADDRESS),
                    topicConfig.get(FEDERATION_SOURCE_ZK_ADDRESS),
                    Boolean.valueOf(topicConfig.getOrDefault(FEDERATION_CAN_READ, "false")),
                    Boolean.valueOf(topicConfig.getOrDefault(FEDERATION_CAN_WRITE, "false")),
                    Boolean.valueOf(topicConfig.getOrDefault(FEDERATION_MIGRATE_BACK, "false")),
                    topicConfig.getOrDefault(FEDERATION_TYPE, TYPE_NORMAL)
            );
            return federationTopicPolicies;
        }
        return null;
    }

    public boolean needSync() {
        return !canWrite && !canRead && typeIsMigrating();
    }

    public boolean typeIsMigrating() {
        return TYPE_MIGRATING.equals(type);
    }

    public boolean typeIsMigrated() {
        return TYPE_MIGRATED.equals(type);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FederationTopicPolicies that = (FederationTopicPolicies) o;

        return sourceCluster.equals(that.sourceCluster)
                && sourceBootstrap.equals(that.sourceBootstrap)
                && sourceZkAddress.equals(that.sourceZkAddress)
                && canRead.equals(that.canRead)
                && canWrite.equals(that.canWrite)
                && migrateBack.equals(that.migrateBack)
                && type.equals(that.type);
    }

    @Override
    public int hashCode() {
        return sourceCluster.hashCode();
    }

    @Override
    public String toString() {
        return "FederationTopicPolicies{" + "sourceCluster='" + sourceCluster + '\''
                + ", sourceBootstrap='" + sourceBootstrap + '\''
                + ", sourceZkAddress='" + sourceZkAddress + '\''
                + ", canRead=" + canRead
                + ", canWrite=" + canWrite
                + ", migrateBack=" + migrateBack
                + ", type='" + type + '\''
                + '}';
    }
}
