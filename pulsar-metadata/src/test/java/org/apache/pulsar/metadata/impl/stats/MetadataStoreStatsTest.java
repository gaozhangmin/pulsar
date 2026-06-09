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
package org.apache.pulsar.metadata.impl.stats;

import static org.testng.Assert.assertEquals;
import io.prometheus.client.CollectorRegistry;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.testng.annotations.Test;

public class MetadataStoreStatsTest {

    @Test
    public void testRecordSessionEvents() {
        String metadataStoreName = "test";
        double connectionLostCount = getSessionEventCount(metadataStoreName, SessionEvent.ConnectionLost);
        double sessionLostCount = getSessionEventCount(metadataStoreName, SessionEvent.SessionLost);

        MetadataStoreStats stats = new MetadataStoreStats(metadataStoreName);
        stats.recordSessionEvent(SessionEvent.ConnectionLost);
        stats.recordSessionEvent(SessionEvent.SessionLost);

        assertEquals(getSessionEventCount(metadataStoreName, SessionEvent.ConnectionLost), connectionLostCount + 1);
        assertEquals(getSessionEventCount(metadataStoreName, SessionEvent.SessionLost), sessionLostCount + 1);
    }

    private static double getSessionEventCount(String metadataStoreName, SessionEvent event) {
        Double value = CollectorRegistry.defaultRegistry.getSampleValue(
                "pulsar_metadata_store_session_event_total",
                new String[]{"name", "event"},
                new String[]{metadataStoreName, event.name()});
        return value == null ? 0 : value;
    }
}
