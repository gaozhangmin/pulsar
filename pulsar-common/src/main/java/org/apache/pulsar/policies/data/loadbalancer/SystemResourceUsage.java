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
package org.apache.pulsar.policies.data.loadbalancer;

import java.util.Collections;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

/**
 * This class represents a object which reflects system resource usage per resource and the upper limit on the resource.
 */
@Getter
@Setter
public class SystemResourceUsage {

    /**
     * Definition of possible resource types.
     */
    public enum ResourceType {
        CPU, Memory, BandwidthIn, BandwidthOut
    }

    @AllArgsConstructor
    public enum NetThrottledType {
        TX_THROTTLED_TIME("tx_throttled_time(ns)"),
        TX_THROTTLED_COUNT("tx_throttled_count"),
        RX_THROTTLED_TIME("rx_throttled_time(ms)"),
        RX_THROTTLED_COUNT("rx_throttled_count");

        public final String template;
    }

    public ResourceUsage bandwidthIn;
    public ResourceUsage bandwidthOut;
    public ResourceUsage physicalBandwidthIn;
    public ResourceUsage physicalBandwidthOut;
    public ResourceUsage cpu;
    public ResourceUsage memory;
    public ResourceUsage directMemory;
    public Map<NetThrottledType, Double> netThrottled;

    public SystemResourceUsage() {
        bandwidthIn = new ResourceUsage(-1, -1);
        bandwidthOut = new ResourceUsage(-1, -1);
        physicalBandwidthIn = new ResourceUsage(-1, -1);
        physicalBandwidthOut = new ResourceUsage(-1, -1);
        cpu = new ResourceUsage(-1, -1);
        memory = new ResourceUsage(-1, -1);
        directMemory = new ResourceUsage(-1, -1);
        netThrottled = Collections.emptyMap();
    }
}
