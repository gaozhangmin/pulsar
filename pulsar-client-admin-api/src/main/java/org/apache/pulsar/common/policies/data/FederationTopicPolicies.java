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
    private String sourceCluster;
    private String sourceBootstrap;
    private String sourceZkAddress;
    private Boolean canRead;
    private Boolean canWrite;
    private Boolean migrateBack;

    public FederationTopicPolicies() {
        this("", "", "", false, false, false);
    }

    public FederationTopicPolicies(String sourceCluster, String sourceBootstrap,
                                   String sourceZkAddress, Boolean canRead,
            Boolean canWrite, Boolean migrateBack) {
        this.sourceCluster = sourceCluster;
        this.sourceBootstrap = sourceBootstrap;
        this.sourceZkAddress = sourceZkAddress;
        this.canRead = canRead;
        this.canWrite = canWrite;
        this.migrateBack = migrateBack;
    }

    public boolean needSync() {
        return !canWrite && !canRead;
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
                && migrateBack.equals(that.migrateBack);
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
                + '}';
    }
}
