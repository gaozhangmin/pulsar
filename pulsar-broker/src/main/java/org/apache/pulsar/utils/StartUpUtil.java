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
package org.apache.pulsar.utils;

import lombok.extern.slf4j.Slf4j;

import static java.lang.Integer.parseInt;
import static java.lang.management.ManagementFactory.getRuntimeMXBean;


/**
 * @author dongzhonghua03 <dongzhonghua03@kuaishou.com>
 * Created on 2025-05-19
 */
@Slf4j
public class StartUpUtil {

    private static volatile Integer pid;
    private static final int UNKNOWN_PID = -1;

    public static void logServiceStartUp(boolean success) {
        log.info("[SERVICE_START_UP] pid={},status={},start_time={}", getPid(),
                success ? "success" : "error", System.currentTimeMillis());
    }

    public static int getPid() {
        if (pid == null) {
            synchronized (StartUpUtil.class) {
                if (pid == null) {
                    try {
                        String name = getRuntimeMXBean().getName();
                        pid = parseInt(name.substring(0, name.indexOf('@')));
                    } catch (Throwable e) {
                        pid = UNKNOWN_PID;
                    }
                }
            }
        }
        return pid;
    }
}
