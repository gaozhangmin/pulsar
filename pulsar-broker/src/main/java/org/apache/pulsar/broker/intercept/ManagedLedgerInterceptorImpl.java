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
package org.apache.pulsar.broker.intercept;

import io.netty.buffer.ByteBuf;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.OpAddEntry;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.pulsar.common.api.proto.BrokerEntryMetadata;
import org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor;
import org.apache.pulsar.common.intercept.BrokerEntryMetadataInterceptor;
import org.apache.pulsar.common.intercept.ManagedLedgerPayloadProcessor;
import org.apache.pulsar.common.protocol.Commands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManagedLedgerInterceptorImpl implements ManagedLedgerInterceptor {
    private static final Logger log = LoggerFactory.getLogger(ManagedLedgerInterceptorImpl.class);
    private static final String INDEX = "index";
    private final Set<BrokerEntryMetadataInterceptor> brokerEntryMetadataInterceptors;

    private final AppendIndexMetadataInterceptor appendIndexMetadataInterceptor;
    private final Set<ManagedLedgerPayloadProcessor.Processor> inputProcessors;
    private final Set<ManagedLedgerPayloadProcessor.Processor> outputProcessors;

    public ManagedLedgerInterceptorImpl(Set<BrokerEntryMetadataInterceptor> brokerEntryMetadataInterceptors,
                                        Set<ManagedLedgerPayloadProcessor> brokerEntryPayloadProcessors) {
        this.brokerEntryMetadataInterceptors = brokerEntryMetadataInterceptors;

        // save appendIndexMetadataInterceptor to field
        AppendIndexMetadataInterceptor appendIndexMetadataInterceptor = null;

        for (BrokerEntryMetadataInterceptor interceptor : this.brokerEntryMetadataInterceptors) {
            if (interceptor instanceof AppendIndexMetadataInterceptor) {
                appendIndexMetadataInterceptor = (AppendIndexMetadataInterceptor) interceptor;
                break;
            }
        }

        this.appendIndexMetadataInterceptor = appendIndexMetadataInterceptor;

        if (brokerEntryPayloadProcessors != null) {
            this.inputProcessors = new LinkedHashSet<>();
            this.outputProcessors = new LinkedHashSet<>();
            for (ManagedLedgerPayloadProcessor processor : brokerEntryPayloadProcessors) {
                this.inputProcessors.add(processor.inputProcessor());
                this.outputProcessors.add(processor.outputProcessor());
            }
        } else {
            this.inputProcessors = null;
            this.outputProcessors = null;
        }
    }

    @Override
    public long getIndex() {
        long index = -1;

        if (appendIndexMetadataInterceptor != null) {
            return appendIndexMetadataInterceptor.getIndex();
        }

        return index;
    }

    @Override
    public OpAddEntry beforeAddEntry(OpAddEntry op, int numberOfMessages) {
       if (op == null || numberOfMessages <= 0) {
           return op;
       }
        op.setData(Commands.addBrokerEntryMetadata(op.getData(), brokerEntryMetadataInterceptors, numberOfMessages));
        if (appendIndexMetadataInterceptor != null) {
            op.setOffsetIndex(appendIndexMetadataInterceptor.getIndex());
        }
        return op;
    }

    @Override
    public void afterFailedAddEntry(int numberOfMessages) {
        if (appendIndexMetadataInterceptor != null) {
            appendIndexMetadataInterceptor.decreaseWithNumberOfMessages(numberOfMessages);
        }
    }

    @Override
    public void onManagedLedgerPropertiesInitialize(Map<String, String> propertiesMap) {
        if (propertiesMap == null || propertiesMap.size() == 0) {
            return;
        }

        if (propertiesMap.containsKey(INDEX)) {
            if (appendIndexMetadataInterceptor != null) {
                appendIndexMetadataInterceptor.recoveryIndexGenerator(
                        Long.parseLong(propertiesMap.get(INDEX)));
            }
        }
    }

    @Override
    public void resetIndex(long index) {
        if (appendIndexMetadataInterceptor != null) {
            appendIndexMetadataInterceptor.resetIndexGenerator(index);
        }
    }

    @Override
    public CompletableFuture<Map<String, String>> onManagedLedgerLastLedgerInitialize(String name,
            LedgerHandle lh, Map<String, String> existingProperties) {
        CompletableFuture<Map<String, String>> promise = new CompletableFuture<>();
        boolean hasAppendIndexMetadataInterceptor = appendIndexMetadataInterceptor != null;
        boolean needsIndexRecovery = hasAppendIndexMetadataInterceptor && lh.getLastAddConfirmed() >= 0;
        boolean needsFirstEntryPropsRecovery = isMissingFirstEntryProps(existingProperties) && lh.getLastAddConfirmed() >= 0;

        if (!needsIndexRecovery && !needsFirstEntryPropsRecovery) {
            promise.complete(Collections.emptyMap());
            return promise;
        }

        // Read last entry for index recovery if needed
        CompletableFuture<Void> indexRecoveryFuture;
        if (needsIndexRecovery) {
            indexRecoveryFuture = recoverIndexGenerator(name, lh);
        } else {
            indexRecoveryFuture = CompletableFuture.completedFuture(null);
        }

        // Read entry 0 for firstEntryProps recovery if needed
        CompletableFuture<Map<String, String>> firstEntryPropsFuture;
        if (needsFirstEntryPropsRecovery) {
            firstEntryPropsFuture = recoverFirstEntryProps(name, lh);
        } else {
            firstEntryPropsFuture = CompletableFuture.completedFuture(Collections.emptyMap());
        }

        indexRecoveryFuture.whenComplete((__, indexEx) -> {
            if (indexEx != null) {
                promise.completeExceptionally(indexEx);
                return;
            }
            firstEntryPropsFuture.whenComplete((recoveredProps, propsEx) -> {
                if (propsEx != null) {
                    // firstEntryProps recovery failure is non-critical; log and continue
                    log.warn("[{}] Failed to recover first-entry props for ledger {}."
                            + " Props will remain missing until next broker restart.", name, lh.getId(), propsEx);
                    promise.complete(Collections.emptyMap());
                } else {
                    promise.complete(recoveredProps);
                }
            });
        });
        return promise;
    }

    private boolean isMissingFirstEntryProps(Map<String, String> existingProperties) {
        if (existingProperties == null) {
            return true;
        }
        boolean hasIndex = existingProperties.containsKey(ManagedLedgerImpl.FIRST_ENTRY_INDEX_PROPS);
        boolean hasTimestamp = existingProperties.containsKey(ManagedLedgerImpl.FIRST_ENTRY_TIMESTAMP_PROPS);
        return !hasIndex || !hasTimestamp;
    }

    private CompletableFuture<Void> recoverIndexGenerator(String name, LedgerHandle lh) {
        CompletableFuture<Void> promise = new CompletableFuture<>();
        lh.readAsync(lh.getLastAddConfirmed(), lh.getLastAddConfirmed()).whenComplete((entries, ex) -> {
            if (ex != null) {
                log.error("[{}] Read last entry error.", name, ex);
                promise.completeExceptionally(ex);
            } else {
                if (entries != null) {
                    try {
                        LedgerEntry ledgerEntry = entries.getEntry(lh.getLastAddConfirmed());
                        if (ledgerEntry != null) {
                            BrokerEntryMetadata brokerEntryMetadata =
                                    Commands.parseBrokerEntryMetadataIfExist(ledgerEntry.getEntryBuffer());
                            if (brokerEntryMetadata != null && brokerEntryMetadata.hasIndex()) {
                                appendIndexMetadataInterceptor.recoveryIndexGenerator(
                                        brokerEntryMetadata.getIndex());
                            }
                        }
                        entries.close();
                        promise.complete(null);
                    } catch (Exception e) {
                        entries.close();
                        log.error("[{}] Failed to recover the index generator from the last add confirmed entry.",
                                name, e);
                        promise.completeExceptionally(e);
                    }
                } else {
                    promise.complete(null);
                }
            }
        });
        return promise;
    }

    private CompletableFuture<Map<String, String>> recoverFirstEntryProps(String name, LedgerHandle lh) {
        CompletableFuture<Map<String, String>> promise = new CompletableFuture<>();
        lh.readAsync(0, 0).whenComplete((entries, ex) -> {
            if (ex != null) {
                log.warn("[{}] Failed to read entry 0 for first-entry props recovery on ledger {}."
                        + " Props will remain missing until next broker restart.", name, lh.getId(), ex);
                promise.complete(Collections.emptyMap());
            } else {
                Map<String, String> recovered = new HashMap<>();
                try (entries) {
                    LedgerEntry entry = entries.getEntry(0);
                    if (entry != null) {
                        ByteBuf buf = entry.getEntryBuffer();
                        long ts = Commands.getEntryTimestamp(buf.duplicate());
                        recovered.put(ManagedLedgerImpl.FIRST_ENTRY_TIMESTAMP_PROPS, Long.toString(ts));
                        BrokerEntryMetadata meta =
                                Commands.parseBrokerEntryMetadataIfExist(buf.duplicate());
                        if (meta != null && meta.hasIndex()) {
                            recovered.put(ManagedLedgerImpl.FIRST_ENTRY_INDEX_PROPS,
                                    Long.toString(meta.getIndex()));
                        }
                    }
                } catch (Exception e) {
                    log.warn("[{}] Error extracting first-entry props from entry 0 of ledger {}.",
                            name, lh.getId(), e);
                }
                if (recovered.isEmpty()) {
                    log.error("[{}] Cannot recover first-entry props for ledger {},"
                            + " recovered props are empty.", name, lh.getId());
                }
                promise.complete(recovered);
            }
        });
        return promise;
    }

    @Override
    public void onUpdateManagedLedgerInfo(Map<String, String> propertiesMap) {
        if (appendIndexMetadataInterceptor != null) {
            propertiesMap.put(INDEX, String.valueOf(appendIndexMetadataInterceptor.getIndex()));
        }
    }

    private PayloadProcessorHandle processPayload(Set<ManagedLedgerPayloadProcessor.Processor> processors,
                                                  Object context, ByteBuf payload) {

        ByteBuf tmpData = payload;
        final Set<ImmutablePair<ManagedLedgerPayloadProcessor.Processor, ByteBuf>> processedSet = new LinkedHashSet<>();
        for (ManagedLedgerPayloadProcessor.Processor payloadProcessor : processors) {
            if (payloadProcessor != null) {
                tmpData = payloadProcessor.process(context, tmpData);
                processedSet.add(new ImmutablePair<>(payloadProcessor, tmpData));
            }
        }
        final ByteBuf dataToReturn = tmpData;
        return new PayloadProcessorHandle() {
            @Override
            public ByteBuf getProcessedPayload() {
                return dataToReturn;
            }

            @Override
            public void release() {
                for (ImmutablePair<ManagedLedgerPayloadProcessor.Processor, ByteBuf> p : processedSet) {
                    p.left.release(p.right);
                }
                processedSet.clear();
            }
        };
    }
    @Override
    public PayloadProcessorHandle processPayloadBeforeLedgerWrite(OpAddEntry op, ByteBuf ledgerData) {
        if (this.inputProcessors == null || this.inputProcessors.size() == 0) {
            return null;
        }
        return processPayload(this.inputProcessors, op.getCtx(), ledgerData);
    }

    @Override
    public PayloadProcessorHandle processPayloadBeforeEntryCache(ByteBuf ledgerData){
        if (this.outputProcessors == null || this.outputProcessors.size() == 0) {
            return null;
        }
        return processPayload(this.outputProcessors, null, ledgerData);
    }
}
