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
package org.apache.pulsar.broker.loadbalance;

import static org.apache.pulsar.broker.loadbalance.LinuxInfoUtils.getThrottledMap;
import static org.apache.pulsar.broker.loadbalance.LinuxInfoUtils.getTotalPodNicUsage;
import static org.apache.pulsar.broker.loadbalance.LinuxInfoUtils.parsePodNicFile;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.AssertJUnit.assertNull;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BitRateUnit;
import org.apache.pulsar.policies.data.loadbalancer.SystemResourceUsage;
import org.mockito.MockedStatic;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class LinuxInfoUtilsTest {

    /**
     * simulate reading first line of /proc/stat to get total cpu usage.
     */
    @Test
    public void testGetCpuUsageForEntireHost(){
        try (MockedStatic<Files> filesMockedStatic = mockStatic(Files.class)) {
            filesMockedStatic.when(() -> Files.lines(any())).thenReturn(
                    Stream.generate(() -> "cpu  317808 128  58637  2503692 7634   0   13472   0    0     0"));
            long idle = 2503692 + 7634, total = 2901371;
            LinuxInfoUtils.ResourceUsage resourceUsage = LinuxInfoUtils.ResourceUsage.builder()
                    .usage(total - idle)
                    .idle(idle)
                    .total(total).build();
            assertEquals(LinuxInfoUtils.getCpuUsageForEntireHost(), resourceUsage);
        }
    }

    @Test
    void testGetTotalPodNicUsage() throws Exception {
        try (MockedStatic<LinuxInfoUtils> linuxInfoUtilsMockedStatic = mockStatic(LinuxInfoUtils.class)) {
            String content = "tx_throttled_time(ns): 1\n"
                    + "tx_throttled_count: 1\n"
                    + "tx_throttled_time_v2(ms): 0\n"
                    + "tx_throttled_count_v2: 0\n"
                    + "rx_throttled_time(ms): 1\n"
                    + "rx_throttled_count: 1\n"
                    + "tx_thrput(bytes): 681\n"
                    + "rx_thrput(bytes): 608\n"
                    + "tx_thrput_socket(bytes): 1000\n"
                    + "rx_thrput_socket(bytes): 1000";
            File tempFile = File.createTempFile("testng", ".txt");
            Files.writeString(tempFile.toPath(), content);
            linuxInfoUtilsMockedStatic.when(() -> parsePodNicFile(any())).thenCallRealMethod();
            Map<String, Double> result = parsePodNicFile(tempFile.getPath());
            System.out.println(result);
            assertEquals(10, result.size());
            assertEquals(1.0, result.get("tx_throttled_count"));
            assertNull(result.get("empty"));
            assertEquals(1000, result.get("tx_thrput_socket(bytes)"));
            assertEquals(1000, result.get("rx_thrput_socket(bytes)"));
            for (SystemResourceUsage.NetThrottledType value : SystemResourceUsage.NetThrottledType.values()) {
                assertEquals(result.get(value.template), 1);
            }
            linuxInfoUtilsMockedStatic.when(LinuxInfoUtils::getThrottledMap).thenCallRealMethod();
            linuxInfoUtilsMockedStatic.when(() -> parsePodNicFile(any())).thenReturn(result);
            Map<SystemResourceUsage.NetThrottledType, Double> throttledMap = getThrottledMap();
            for (SystemResourceUsage.NetThrottledType value : SystemResourceUsage.NetThrottledType.values()) {
                assertEquals(throttledMap.get(value), 1);
            }
            System.out.println(parsePodNicFile("a"));
            linuxInfoUtilsMockedStatic.when(() -> getTotalPodNicUsage(any(), any())).thenCallRealMethod();
            assertEquals(getTotalPodNicUsage(LinuxInfoUtils.PodNICUsageType.TX, BitRateUnit.Kilobit)
                    , 8);
            assertEquals(getTotalPodNicUsage(LinuxInfoUtils.PodNICUsageType.RX, BitRateUnit.Kilobit)
                    , 8);

            linuxInfoUtilsMockedStatic.when(() -> parsePodNicFile(any())).thenCallRealMethod();
            Files.writeString(tempFile.toPath(), "");
            Map<String, Double> res1 = parsePodNicFile(tempFile.toString());
            assertEquals(res1.size(), 0);
            assertThrows(IOException.class, () -> parsePodNicFile("null"));
            assertEquals(getTotalPodNicUsage(LinuxInfoUtils.PodNICUsageType.TX, BitRateUnit.Kilobit)
                    , -1);
            assertEquals(getTotalPodNicUsage(LinuxInfoUtils.PodNICUsageType.RX, BitRateUnit.Kilobit)
                    , -1);
        }
    }
}
