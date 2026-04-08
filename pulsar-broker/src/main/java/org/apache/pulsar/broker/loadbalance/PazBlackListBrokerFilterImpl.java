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

import java.util.Iterator;
import java.util.Set;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.policies.data.loadbalancer.BrokerData;
import org.apache.pulsar.policies.data.loadbalancer.BundleData;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PazBlackListBrokerFilterImpl implements BrokerFilter {
    private static final Logger log = LoggerFactory.getLogger(PazBlackListBrokerFilterImpl.class);

    @Override
    public void filter(Set<String> brokers, BundleData bundleToAssign, LoadData loadData, ServiceConfiguration conf)
            throws BrokerFilterException {
        if (conf.getLoadBalancerBrokerPazBlackList().isEmpty()) {
            return;
        }
        Iterator<String> brokerIterator = brokers.iterator();
        while (brokerIterator.hasNext()) {
            String broker = brokerIterator.next();
            BrokerData data = loadData.getBrokerData().get(broker);
            LocalBrokerData localData = data.getLocalData();
            String kwsServicePaz = localData == null ? null : localData.getProperties().get("KWS_SERVICE_PAZ");
            String kwsServiceRegion = localData == null ? null : localData.getProperties().get("KWS_SERVICE_REGION");
            if (kwsServicePaz == null || kwsServiceRegion == null) {
                log.error("Broker [{}] kwsServicePaz: {}, kwsServiceRegion: {} is null",
                        broker, kwsServicePaz, kwsServiceRegion);
            } else {
                if (conf.getLoadBalancerBrokerPazBlackList().contains(kwsServicePaz)) {
                    brokerIterator.remove();
                    log.warn("Broker [{}] kwsServicePaz: {}, kwsServiceRegion: {} is in paz black list {},"
                                    + "remove it from candidate list",
                            broker, kwsServicePaz, kwsServiceRegion, conf.getLoadBalancerBrokerPazBlackList());
                }
            }
        }
    }
}
