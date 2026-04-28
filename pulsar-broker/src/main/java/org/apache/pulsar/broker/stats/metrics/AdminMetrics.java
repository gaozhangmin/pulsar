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
package org.apache.pulsar.broker.stats.metrics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.admin.v2.Namespaces;
import org.apache.pulsar.common.stats.Metrics;

public class AdminMetrics extends AbstractMetrics {

    private static final String NAMESPACES_API_CALLS = "pulsar_namespaces_api_calls_total";

    public AdminMetrics(PulsarService pulsar) {
        super(pulsar);
    }

    @Override
    public List<Metrics> generate() {
        List<Metrics> metricsList = new ArrayList<>();

        try {

            // Namespaces API call counters
            Map<String, AtomicLong> apiCounters = Namespaces.API_COUNTERS;
            for (Map.Entry<String, AtomicLong> entry : apiCounters.entrySet()) {
                Map<String, String> dimensions = new HashMap<>();
                dimensions.put("api", entry.getKey());
                Metrics metrics = createMetrics(dimensions);
                metrics.put(NAMESPACES_API_CALLS, entry.getValue().get());
                metricsList.add(metrics);
            }
        } catch (Exception e) {
            // Ignore the exception
        }

        return metricsList;
    }
}
