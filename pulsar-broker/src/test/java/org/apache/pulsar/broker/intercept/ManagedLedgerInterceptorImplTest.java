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
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerFactoryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.OpAddEntry;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.common.api.proto.BrokerEntryMetadata;
import org.apache.pulsar.common.intercept.BrokerEntryMetadataInterceptor;
import org.apache.pulsar.common.intercept.BrokerEntryMetadataUtils;
import org.apache.pulsar.common.intercept.ManagedLedgerPayloadProcessor;
import org.apache.pulsar.common.protocol.Commands;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.pulsar.common.protocol.Commands.serializeMetadataAndPayload;
import org.apache.pulsar.common.api.proto.MessageMetadata;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(groups = "broker")
public class ManagedLedgerInterceptorImplTest  extends MockedBookKeeperTestCase {
    private static final Logger log = LoggerFactory.getLogger(ManagedLedgerInterceptorImplTest.class);

    public static class TestPayloadProcessor implements ManagedLedgerPayloadProcessor {
        @Override
        public Processor inputProcessor() {
            return new Processor() {
                @Override
                public ByteBuf process(Object contextObj, ByteBuf inputPayload) {
                    byte[] newMessage = (new String("Modified Test Message")).getBytes();
                    ByteBuf processedPayload =  Unpooled.wrappedBuffer(newMessage, 0, newMessage.length);
                    inputPayload.release();
                    return processedPayload.retainedDuplicate();
                }

                @Override
                public void release(ByteBuf processedPayload) {
                    processedPayload.release();
                }
            };
        }
        @Override
        public Processor outputProcessor() {
            return new Processor() {
                @Override
                public ByteBuf process(Object contextObj, ByteBuf inputPayload) {
                    byte[] bytes = new byte[inputPayload.readableBytes()];
                    inputPayload.readBytes(bytes);
                    String storedMessage = new String(bytes);
                    Assert.assertTrue(storedMessage.equals("Modified Test Message"));

                    byte[] newMessage = (new String("Test Message")).getBytes();
                    inputPayload.release();
                    return Unpooled.wrappedBuffer(newMessage, 0, newMessage.length).retainedDuplicate();
                }

                @Override
                public void release(ByteBuf processedPayload) {
                    processedPayload.release();
                }
            };
        }
    }

    @Test
    public void testAddBrokerEntryMetadata() throws Exception {
        final int mockBatchSize = 2;
        int numberOfEntries = 10;
        final String ledgerAndCursorName = "topicEntryMetadataSequenceId";

        ManagedLedgerInterceptor interceptor = new ManagedLedgerInterceptorImpl(getBrokerEntryMetadataInterceptors(),null);

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(2);
        config.setManagedLedgerInterceptor(interceptor);

        ManagedLedger ledger = factory.open(ledgerAndCursorName, config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor(ledgerAndCursorName);

        for ( int i = 0 ; i < numberOfEntries; i ++) {
            ledger.addEntry(("message" + i).getBytes(), mockBatchSize);
        }

        assertEquals(19, ((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getIndex());
        List<Entry> entryList = cursor.readEntries(numberOfEntries);
        for (int i = 0 ; i < numberOfEntries; i ++) {
            Entry entry = entryList.get(i);
            BrokerEntryMetadata metadata =
                    Commands.parseBrokerEntryMetadataIfExist(entry.getDataBuffer());
            assertNotNull(metadata);
            assertEquals(metadata.getIndex(), (i + 1) * mockBatchSize - 1);
            entry.release();
        }

        cursor.close();
        ledger.close();
        factory.shutdown();
    }
    @Test
    public void testMessagePayloadProcessor() throws Exception {
        final String ledgerAndCursorName = "topicEntryWithPayloadProcessed";

        Set<ManagedLedgerPayloadProcessor> processors = new HashSet();
        processors.add(new TestPayloadProcessor());
        ManagedLedgerInterceptor interceptor = new ManagedLedgerInterceptorImpl(new HashSet(),processors);

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(2);
        config.setManagedLedgerInterceptor(interceptor);

        ManagedLedger ledger = factory.open(ledgerAndCursorName, config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor(ledgerAndCursorName);
        ledger.addEntry("Test Message".getBytes());
        factory.getEntryCacheManager().clear();
        List<Entry> entryList = cursor.readEntries(1);
        Entry entry = entryList.get(0);
        String message = new String(entry.getData());
        entry.release();
        Assert.assertTrue(message.equals("Test Message"));
        cursor.close();
        ledger.close();
        factory.shutdown();
        config.setManagedLedgerInterceptor(null);
    }

    @Test
    public void testTotalSizeCorrectIfHasInterceptor() throws Exception {
        final String mlName = "ml1";
        final String cursorName = "cursor1";

        // Registry interceptor.
        ManagedLedgerConfig config = new ManagedLedgerConfig();
        Set<ManagedLedgerPayloadProcessor> processors = new HashSet();
        processors.add(new TestPayloadProcessor());
        ManagedLedgerInterceptor interceptor = new ManagedLedgerInterceptorImpl(new HashSet(), processors);
        config.setManagedLedgerInterceptor(interceptor);
        config.setMaxEntriesPerLedger(2);

        // Add many entries and consume.
        ManagedLedgerImpl ledger = (ManagedLedgerImpl) factory.open(mlName, config);
        ManagedCursorImpl cursor = (ManagedCursorImpl) ledger.openCursor(cursorName);
        for (int i = 0; i < 5; i++){
            cursor.delete(ledger.addEntry(new byte[1]));
        }

        // Trim ledgers.
        CompletableFuture<Void> trimLedgerFuture = new CompletableFuture<>();
        ledger.trimConsumedLedgersInBackground(trimLedgerFuture);
        trimLedgerFuture.join();

        // verify.
        assertEquals(ledger.getTotalSize(), calculatePreciseSize(ledger));

        // cleanup.
        cursor.close();
        ledger.close();
        factory.getEntryCacheManager().clear();
        factory.shutdown();
        config.setManagedLedgerInterceptor(null);
    }

    public static long calculatePreciseSize(ManagedLedgerImpl ledger){
        return ledger.getLedgersInfo().values().stream()
                .map(info -> info.getSize()).reduce((l1,l2) -> l1 + l2).orElse(0L) + ledger.getCurrentLedgerSize();
    }

    @Test(timeOut = 20000)
    public void testRecoveryIndex() throws Exception {
        final int mockBatchSize = 2;
        ManagedLedgerInterceptor interceptor = new ManagedLedgerInterceptorImpl(getBrokerEntryMetadataInterceptors(),null);

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setManagedLedgerInterceptor(interceptor);
        ManagedLedger ledger = factory.open("my_recovery_index_test_ledger", config);

        ledger.addEntry("dummy-entry-1".getBytes(StandardCharsets.UTF_8), mockBatchSize);

        ManagedCursor cursor = ledger.openCursor("c1");

        ledger.addEntry("dummy-entry-2".getBytes(StandardCharsets.UTF_8), mockBatchSize);

        assertEquals(((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getIndex(), mockBatchSize * 2 - 1);

        ledger.close();

        log.info("Closing ledger and reopening");

        // / Reopen the same managed-ledger
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ledger = factory2.open("my_recovery_index_test_ledger", config);

        cursor = ledger.openCursor("c1");

        assertEquals(ledger.getNumberOfEntries(), 2);
        assertEquals(((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getIndex(), mockBatchSize * 2 - 1);


        List<Entry> entries = cursor.readEntries(100);
        assertEquals(entries.size(), 1);
        entries.forEach(Entry::release);

        cursor.close();
        ledger.close();
    }

    @Test
    public void testFindPositionByIndex() throws Exception {
        final int mockBatchSize = 2;
        final int maxEntriesPerLedger = 5;
        int maxSequenceIdPerLedger = mockBatchSize * maxEntriesPerLedger;
        ManagedLedgerInterceptor interceptor = new ManagedLedgerInterceptorImpl(getBrokerEntryMetadataInterceptors(),null);


        ManagedLedgerConfig managedLedgerConfig = new ManagedLedgerConfig();
        managedLedgerConfig.setManagedLedgerInterceptor(interceptor);
        managedLedgerConfig.setMaxEntriesPerLedger(5);

        ManagedLedger ledger = factory.open("my_ml_broker_entry_metadata_test_ledger", managedLedgerConfig);
        ManagedCursor cursor = ledger.openCursor("c1");

        long firstLedgerId = -1;
        for (int i = 0; i < maxEntriesPerLedger; i++) {
            firstLedgerId = ledger.addEntry("dummy-entry".getBytes(StandardCharsets.UTF_8), mockBatchSize).getLedgerId();
        }

        assertEquals(((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getIndex(), 9);


        PositionImpl position = null;
        for (int index = 0; index <= ((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getIndex(); index ++) {
            position = (PositionImpl) ledger.asyncFindPosition(new IndexSearchPredicate(index)).get();
            assertEquals(position.getEntryId(), (index % maxSequenceIdPerLedger) / mockBatchSize);
        }

        // roll over ledger
        long secondLedgerId = -1;
        for (int i = 0; i < maxEntriesPerLedger; i++) {
            secondLedgerId = ledger.addEntry("dummy-entry".getBytes(StandardCharsets.UTF_8), mockBatchSize).getLedgerId();
        }
        assertEquals(((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getIndex(), 19);
        assertNotEquals(firstLedgerId, secondLedgerId);

        for (int index = 0; index <= ((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getIndex(); index ++) {
            position = (PositionImpl) ledger.asyncFindPosition(new IndexSearchPredicate(index)).get();
            assertEquals(position.getEntryId(), (index % maxSequenceIdPerLedger) / mockBatchSize);
        }

        // reopen ledger
        ledger.close();
        // / Reopen the same managed-ledger
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ledger = factory2.open("my_ml_broker_entry_metadata_test_ledger", managedLedgerConfig);

        long thirdLedgerId = -1;
        for (int i = 0; i < maxEntriesPerLedger; i++) {
            thirdLedgerId = ledger.addEntry("dummy-entry".getBytes(StandardCharsets.UTF_8), mockBatchSize).getLedgerId();
        }
        assertEquals(((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getIndex(), 29);
        assertNotEquals(secondLedgerId, thirdLedgerId);

        for (int index = 0; index <= ((ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor()).getIndex(); index ++) {
            position = (PositionImpl) ledger.asyncFindPosition(new IndexSearchPredicate(index)).get();
            assertEquals(position.getEntryId(), (index % maxSequenceIdPerLedger) / mockBatchSize);
        }
        cursor.close();
        ledger.close();
    }

    @Test
    public void testAddEntryFailed() throws Exception {
        final int mockBatchSize = 2;
        final String ledgerAndCursorName = "testAddEntryFailed";

        ManagedLedgerInterceptor interceptor =
                new ManagedLedgerInterceptorImpl(getBrokerEntryMetadataInterceptors(), null);

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(2);
        config.setManagedLedgerInterceptor(interceptor);

        ByteBuf buffer = Unpooled.wrappedBuffer("message".getBytes());
        ManagedLedger ledger = factory.open(ledgerAndCursorName, config);

        ledger.terminate();

        ManagedLedgerInterceptorImpl interceptor1 =
                (ManagedLedgerInterceptorImpl) ledger.getManagedLedgerInterceptor();

        CountDownLatch countDownLatch = new CountDownLatch(1);
        try {
            ledger.asyncAddEntry(buffer, mockBatchSize, new AsyncCallbacks.AddEntryCallback() {
                @Override
                public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                    countDownLatch.countDown();
                }

                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                    countDownLatch.countDown();
                }
            }, null);

            countDownLatch.await();
            assertEquals(interceptor1.getIndex(), -1);
        } finally {
            ledger.close();
            factory.shutdown();
        }

    }

    @Test
    public void testBeforeAddEntryWithException() throws Exception {
        final int mockBatchSize = 2;
        final String ledgerAndCursorName = "testBeforeAddEntryWithException";

        ManagedLedgerInterceptor interceptor =
                new MockManagedLedgerInterceptorImpl(getBrokerEntryMetadataInterceptors(), null);

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(2);
        config.setManagedLedgerInterceptor(interceptor);

        ByteBuf buffer = Unpooled.wrappedBuffer("message".getBytes());
        ManagedLedger ledger = factory.open(ledgerAndCursorName, config);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        try {
            ledger.asyncAddEntry(buffer, mockBatchSize, new AsyncCallbacks.AddEntryCallback() {
                @Override
                public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                    countDownLatch.countDown();
                }

                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                    countDownLatch.countDown();
                }
            }, null);
            countDownLatch.await();
            assertEquals(buffer.refCnt(), 1);
        } finally {
            ledger.close();
            factory.shutdown();
        }
    }

    private class MockManagedLedgerInterceptorImpl extends ManagedLedgerInterceptorImpl {
        private final Set<BrokerEntryMetadataInterceptor> brokerEntryMetadataInterceptors;

        public MockManagedLedgerInterceptorImpl(
                Set<BrokerEntryMetadataInterceptor> brokerEntryMetadataInterceptors,
                Set<ManagedLedgerPayloadProcessor> brokerEntryPayloadProcessors) {
            super(brokerEntryMetadataInterceptors, brokerEntryPayloadProcessors);
            this.brokerEntryMetadataInterceptors = brokerEntryMetadataInterceptors;
        }

        @Override
        public OpAddEntry beforeAddEntry(OpAddEntry op, int numberOfMessages) {
            if (op == null || numberOfMessages <= 0) {
                return op;
            }
            op.setData(Commands.addBrokerEntryMetadata(op.getData(), brokerEntryMetadataInterceptors,
                    numberOfMessages));
            if (op != null) {
                throw new RuntimeException("throw exception before add entry for test");
            }
            return op;
        }
    }

    public static Set<BrokerEntryMetadataInterceptor> getBrokerEntryMetadataInterceptors() {
        Set<String> interceptorNames = new HashSet<>();
        interceptorNames.add("org.apache.pulsar.common.intercept.AppendBrokerTimestampMetadataInterceptor");
        interceptorNames.add("org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor");
        return BrokerEntryMetadataUtils.loadBrokerEntryMetadataInterceptors(interceptorNames,
                Thread.currentThread().getContextClassLoader());
    }

    static class IndexSearchPredicate implements Predicate<Entry> {

        long indexToSearch = -1;
        public IndexSearchPredicate(long indexToSearch) {
            this.indexToSearch = indexToSearch;
        }

        @Override
        public boolean test(@Nullable Entry entry) {
            try {
                BrokerEntryMetadata brokerEntryMetadata = Commands.parseBrokerEntryMetadataIfExist(entry.getDataBuffer());
                return brokerEntryMetadata.getIndex() < indexToSearch;
            } catch (Exception e) {
                log.error("Error deserialize message for message position find", e);
            } finally {
                entry.release();
            }
            return false;
        }
    }

    @Test(timeOut = 3000)
    public void testManagedLedgerPayloadInputProcessorFailure() throws Exception {
        var config = new ManagedLedgerConfig();
        final String failureMsg = "failed to process input payload";
        config.setManagedLedgerInterceptor(new ManagedLedgerInterceptorImpl(
                Collections.emptySet(), Set.of(new ManagedLedgerPayloadProcessor() {
            @Override
            public Processor inputProcessor() {
                return new Processor() {
                    @Override
                    public ByteBuf process(Object contextObj, ByteBuf inputPayload) {
                        log.info("testManagedLedgerPayloadInputProcessorFailure.process");
                        if (inputPayload.readBoolean()) {
                            throw new RuntimeException(failureMsg);
                        }
                        return inputPayload;
                    }

                    @Override
                    public void release(ByteBuf processedPayload) {
                        // no-op
                    }
                };
            }
        })));

        var ledger = factory.open("testManagedLedgerPayloadProcessorFailure", config);
        int count = 10;
        var countDownLatch = new CountDownLatch(count);
        var successCount = new AtomicInteger(0);
        var expectedException = new ArrayList<Exception>();

        ByteBuf shouldFail = Unpooled.buffer().writeBoolean(true);
        ByteBuf shouldSucceed = Unpooled.buffer().writeBoolean(false);
        byte[] shouldFailBytes = new byte[shouldFail.readableBytes()];
        shouldFail.readBytes(shouldFailBytes);
        byte[] shouldSucceedBytes = new byte[shouldSucceed.readableBytes()];
        shouldSucceed.readBytes(shouldSucceedBytes);
        shouldSucceed.release();
        shouldFail.release();

        for (int i = 0; i < count; i++) {
            if (i % 2 == 0) {
                try {
                    ledger.addEntry(shouldFailBytes);
                    successCount.incrementAndGet();
                    countDownLatch.countDown();
                } catch (Exception t) {
                    expectedException.add(t);
                    countDownLatch.countDown();
                } finally {
                    ledger.unfenceForInterceptorException();
                }
            } else {
                try {
                    ledger.addEntry(shouldSucceedBytes);
                    successCount.incrementAndGet();
                    countDownLatch.countDown();
                } catch (Exception t) {
                    expectedException.add(t);
                    countDownLatch.countDown();
                } finally {
                    ledger.unfenceForInterceptorException();
                }
            }
        }

        countDownLatch.await();
        assertEquals(expectedException.size(), count / 2);
        assertEquals(successCount.get(), count / 2);
        for (Exception e : expectedException) {
            assertEquals(e.getCause().getMessage(), failureMsg);
        }
        ledger.close();
    }

    private ManagedLedgerConfig createTestConfig(int maxEntriesPerLedger) {
        ManagedLedgerInterceptor interceptor =
                new ManagedLedgerInterceptorImpl(getBrokerEntryMetadataInterceptors(), null);

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setManagedLedgerInterceptor(interceptor);
        config.setMaxEntriesPerLedger(maxEntriesPerLedger);
        return config;
    }

    private long addEntriesToLedger(ManagedLedger ledger, int numEntries, int batchSize) throws Exception {
        long ledgerId = -1;
        for (int i = 0; i < numEntries; i++) {
            // Create message content
            String message = "dummy-entry-" + i;
            ByteBuf recordsWrapper = Unpooled.wrappedBuffer(message.getBytes(UTF_8));
            
            // Create message metadata with batch size
            MessageMetadata messageMetadata = getMessageMetadataWithNumberMessages(batchSize);
            
            // Serialize metadata and payload
            ByteBuf buf = serializeMetadataAndPayload(
                Commands.ChecksumType.None,
                messageMetadata,
                recordsWrapper);
                
            // Use async add entry with callback
            CompletableFuture<Position> future = new CompletableFuture<>();
            ledger.asyncAddEntry(buf, batchSize, new AsyncCallbacks.AddEntryCallback() {
                @Override
                public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                    future.complete(position);
                }

                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                    future.completeExceptionally(exception);
                }
            }, null);
            
            Position position = future.get();
            ledgerId = ((PositionImpl) position).getLedgerId();
        }
        return ledgerId;
    }

    protected static MessageMetadata getMessageMetadataWithNumberMessages(int numMessages) {
        final MessageMetadata metadata = new MessageMetadata();
        metadata.setProducerName("test-producer");
        metadata.setSequenceId(0L);
        metadata.setPublishTime(System.currentTimeMillis());
        metadata.setNumMessagesInBatch(numMessages);
        return metadata;
    }

    private void verifyStartOffsetPerLedger(ManagedLedger ledger, long ledgerId, long expectedFirstIndex) throws Exception {
        MLDataFormats.ManagedLedgerInfo.LedgerInfo ledgerInfo = ledger.getLedgersInfo().get(ledgerId);
        assertEquals(ledgerInfo.getPropertiesCount(), 2);
        assertNotNull(ledger.asyncGetLedgerProperty(ledgerId, "firstEntryTimestamp").get());
        assertTrue(Long.parseLong(ledger.asyncGetLedgerProperty(ledgerId, "firstEntryTimestamp").get()) > 0);
        assertEquals(Long.parseLong(ledger.asyncGetLedgerProperty(ledgerId, "firstEntryIndex").get()), expectedFirstIndex);
    }

    private void verifyPositionsByOffset(ManagedLedger ledger, long maxOffset, int batchSize, int maxEntriesPerLedger) throws Exception {
        int maxSequenceIdPerLedger = batchSize * maxEntriesPerLedger;
        for (long offset = 0; offset <= maxOffset; offset++) {
            PositionImpl position = (PositionImpl) ledger.asyncFindPosition(offset, new IndexSearchPredicate(offset)).get();
            PositionImpl positionOld = (PositionImpl) ledger.asyncFindPosition(new IndexSearchPredicate(offset)).get();
            assertEquals(position.getEntryId(), (offset % maxSequenceIdPerLedger) / batchSize);
            //optimized search method results should be same as old method
            assertEquals(position, positionOld);
        }
    }

    private void verifyMissingFirstEntryIndex(ManagedLedger ledger, long ledgerId) throws Exception {
        String property = ledger.asyncGetLedgerProperty(ledgerId, "firstEntryIndex").get();
        assertNull(property, "Expected firstEntryIndex property to be null for ledger " + ledgerId);
    }

    private ManagedLedger reopenLedger(String ledgerName, ManagedLedgerConfig config,
                                       ManagedLedgerFactoryImpl factoryToShutdown) throws Exception {
        if (factoryToShutdown != null) {
            factoryToShutdown.shutdown();
        }
        ManagedLedgerFactoryImpl newFactory = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        return newFactory.open(ledgerName, config);
    }

    @Test
    public void testFindPositionByOffset() throws Exception {
        final int mockBatchSize = 2;
        final int maxEntriesPerLedger = 5;
        ManagedLedgerConfig managedLedgerConfig = createTestConfig(maxEntriesPerLedger);

        ManagedLedger ledger = factory.open("my_ml_broker_entry_offset_test_ledger", managedLedgerConfig);

        // Add entries to first ledger
        long firstLedgerId = addEntriesToLedger(ledger, maxEntriesPerLedger, mockBatchSize);
        assertEquals(ledger.getManagedLedgerInterceptor().getIndex(), 9);
        long earliestOffset = MessageMetadataUtils.getOffsetOfPosition((ManagedLedgerImpl) ledger,
                new PositionImpl(firstLedgerId, 0), false,
                -2, true,
                5).get();
        verifyStartOffsetPerLedger(ledger, firstLedgerId, earliestOffset - 1);
        // Test finding position by offset using the new method
        verifyPositionsByOffset(ledger, ledger.getManagedLedgerInterceptor().getIndex(), mockBatchSize, maxEntriesPerLedger);


        // Roll over to second ledger
        long secondLedgerId = addEntriesToLedger(ledger, maxEntriesPerLedger, mockBatchSize);
        assertEquals(ledger.getManagedLedgerInterceptor().getIndex(), 19);
        assertNotEquals(firstLedgerId, secondLedgerId);
        earliestOffset = MessageMetadataUtils.getOffsetOfPosition((ManagedLedgerImpl) ledger,
                new PositionImpl(secondLedgerId, 0), false,
                -2, true,
                5).get();
        verifyStartOffsetPerLedger(ledger, secondLedgerId, earliestOffset - 1);
        // Test with both ledgers
        verifyPositionsByOffset(ledger, ledger.getManagedLedgerInterceptor().getIndex(), mockBatchSize, maxEntriesPerLedger);

        // Roll over to second ledger
        long thirdLedgerId = addEntriesToLedger(ledger, maxEntriesPerLedger, mockBatchSize);
        assertEquals(ledger.getManagedLedgerInterceptor().getIndex(), 29);
        assertNotEquals(secondLedgerId, thirdLedgerId);
        earliestOffset = MessageMetadataUtils.getOffsetOfPosition((ManagedLedgerImpl) ledger,
                new PositionImpl(thirdLedgerId, 0), false,
                -2, true,
                5).get();
        verifyStartOffsetPerLedger(ledger, thirdLedgerId, earliestOffset - 1);
        // Test with both ledgers
        verifyPositionsByOffset(ledger, ledger.getManagedLedgerInterceptor().getIndex(), mockBatchSize, maxEntriesPerLedger);

        // Roll over to second ledger
        long forthLedgerId = addEntriesToLedger(ledger, maxEntriesPerLedger, mockBatchSize);
        assertEquals(ledger.getManagedLedgerInterceptor().getIndex(), 39);
        assertNotEquals(thirdLedgerId, forthLedgerId);
        earliestOffset = MessageMetadataUtils.getOffsetOfPosition((ManagedLedgerImpl) ledger,
                new PositionImpl(forthLedgerId, 0), false,
                -2, true,
                5).get();
        verifyStartOffsetPerLedger(ledger, forthLedgerId, earliestOffset - 1);
        // Test with both ledgers
        verifyPositionsByOffset(ledger, ledger.getManagedLedgerInterceptor().getIndex(), mockBatchSize, maxEntriesPerLedger);


        // Test edge cases
        // Test offset 0 (should find first position)
        PositionImpl position = (PositionImpl) ledger.asyncFindPosition(0, new IndexSearchPredicate(0)).get();
        assertEquals(position.getEntryId(), 0);
        // Test offset beyond current index (should handle gracefully)
        long beyondIndex = ledger.getManagedLedgerInterceptor().getIndex() + 100;
        position = (PositionImpl) ledger.asyncFindPosition(beyondIndex, new IndexSearchPredicate(beyondIndex)).get();
        assertNotNull(position);


        // Reopen ledger and test again
        ledger = reopenLedger("my_ml_broker_entry_offset_test_ledger", managedLedgerConfig, null);
        // Add more entries to third ledger
        long fifthLedgerId = addEntriesToLedger(ledger, maxEntriesPerLedger, mockBatchSize);
        assertEquals(ledger.getManagedLedgerInterceptor().getIndex(), 49);
        assertNotEquals(secondLedgerId, thirdLedgerId);
        earliestOffset = MessageMetadataUtils.getOffsetOfPosition((ManagedLedgerImpl) ledger,
                new PositionImpl(thirdLedgerId, 0), false,
                -2, true,
                5).get();
        verifyStartOffsetPerLedger(ledger, thirdLedgerId, earliestOffset - 1);
        // Test with all three ledgers after reopen
        verifyPositionsByOffset(ledger, ledger.getManagedLedgerInterceptor().getIndex(), mockBatchSize, maxEntriesPerLedger);

        ledger.close();
    }

    @Test
    public void testFindPositionByOffsetWithMissingFirstEntryIndex() throws Exception {
        final int mockBatchSize = 2;
        final int maxEntriesPerLedger = 5;
        int maxSequenceIdPerLedger = mockBatchSize * maxEntriesPerLedger;
        final String ledgerName = "my_ml_test_missing_firstEntryIndex";

        ManagedLedgerConfig managedLedgerConfig = createTestConfig(maxEntriesPerLedger);

        ManagedLedger ledger = factory.open(ledgerName, managedLedgerConfig);

        // Add entries to create three ledgers
        long firstLedgerId = addEntriesToLedger(ledger, maxEntriesPerLedger, mockBatchSize);
        long secondLedgerId = addEntriesToLedger(ledger, maxEntriesPerLedger, mockBatchSize);
        long thirdLedgerId = addEntriesToLedger(ledger, maxEntriesPerLedger, mockBatchSize);
        assertNotEquals(firstLedgerId, secondLedgerId);
        assertNotEquals(secondLedgerId, thirdLedgerId);

        assertEquals(ledger.getManagedLedgerInterceptor().getIndex(), 29);

        // Verify initial state - all ledgers should have firstEntryIndex property
        long earliestOffset = MessageMetadataUtils.getOffsetOfPosition((ManagedLedgerImpl) ledger,
                new PositionImpl(firstLedgerId, 0), false,
                -2, true,
                5).get();
        verifyStartOffsetPerLedger(ledger, firstLedgerId, earliestOffset - 1);
        earliestOffset = MessageMetadataUtils.getOffsetOfPosition((ManagedLedgerImpl) ledger,
                new PositionImpl(secondLedgerId, 0), false,
                -2, true,
                5).get();
        verifyStartOffsetPerLedger(ledger, secondLedgerId, earliestOffset - 1);
        earliestOffset = MessageMetadataUtils.getOffsetOfPosition((ManagedLedgerImpl) ledger,
                new PositionImpl(thirdLedgerId, 0), false,
                -2, true,
                5).get();
        verifyStartOffsetPerLedger(ledger, thirdLedgerId, earliestOffset - 1);

        // Test normal case before removing properties
        PositionImpl position = (PositionImpl) ledger.asyncFindPosition(15, new IndexSearchPredicate(15)).get();
        assertEquals(position.getEntryId(), (15 % maxSequenceIdPerLedger) / mockBatchSize);

        // Remove first ledger's firstEntryIndex property
        ledger.asyncRemoveLedgerProperty(firstLedgerId, "firstEntryIndex").get();
        ledger.close();

        // Reopen and test after removing first ledger's firstEntryIndex
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory1 = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ledger = factory1.open(ledgerName, managedLedgerConfig);

        verifyMissingFirstEntryIndex(ledger, firstLedgerId);
        assertEquals(ledger.getManagedLedgerInterceptor().getIndex(), 29);

        // Test that position finding still works correctly after removing first ledger's firstEntryIndex
        verifyPositionsByOffset(ledger, ledger.getManagedLedgerInterceptor().getIndex(), mockBatchSize, maxEntriesPerLedger);

        // Remove second ledger's firstEntryIndex property
        ledger.asyncRemoveLedgerProperty(secondLedgerId, "firstEntryIndex").get();
        ledger.close();

        // Reopen and test after removing second ledger's firstEntryIndex
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ledger = factory2.open(ledgerName, managedLedgerConfig);

        verifyMissingFirstEntryIndex(ledger, firstLedgerId);
        verifyMissingFirstEntryIndex(ledger, secondLedgerId);
        assertEquals(ledger.getManagedLedgerInterceptor().getIndex(), 29);

        // Test that position finding still works correctly after removing second ledger's firstEntryIndex
        verifyPositionsByOffset(ledger, ledger.getManagedLedgerInterceptor().getIndex(), mockBatchSize, maxEntriesPerLedger);

        // Remove third ledger's firstEntryIndex property
        ledger.asyncRemoveLedgerProperty(thirdLedgerId, "firstEntryIndex").get();
        ledger.close();

        // Reopen and test after removing all firstEntryIndex properties
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory3 = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ledger = factory3.open(ledgerName, managedLedgerConfig);

        // Verify all properties are now missing
        verifyMissingFirstEntryIndex(ledger, firstLedgerId);
        verifyMissingFirstEntryIndex(ledger, secondLedgerId);
        verifyMissingFirstEntryIndex(ledger, thirdLedgerId);
        assertEquals(ledger.getManagedLedgerInterceptor().getIndex(), 29);

        // Test that position finding still works correctly even with all firstEntryIndex properties removed
        verifyPositionsByOffset(ledger, ledger.getManagedLedgerInterceptor().getIndex(), mockBatchSize, maxEntriesPerLedger);

        // Test edge cases with all properties missing
        // Test offset 0 (should still find first position)
        position = (PositionImpl) ledger.asyncFindPosition(0, new IndexSearchPredicate(0)).get();
        assertEquals(position.getEntryId(), 0);

        // Test middle offset
        position = (PositionImpl) ledger.asyncFindPosition(15, new IndexSearchPredicate(15)).get();
        assertEquals(position.getEntryId(), (15 % maxSequenceIdPerLedger) / mockBatchSize);

        // Test last offset
        position = (PositionImpl) ledger.asyncFindPosition(29, new IndexSearchPredicate(29)).get();
        assertEquals(position.getEntryId(), (29 % maxSequenceIdPerLedger) / mockBatchSize);

        ledger.close();
    }

    @Test
    public void testOffsetFinderOptimizedVsNonOptimizedConsistency() throws Exception {
        final int mockBatchSize = 2;
        final int maxEntriesPerLedger = 5;
        final String ledgerName = "testOffsetFinderConsistency";

        ManagedLedgerConfig config = new ManagedLedgerConfig();
        config.setMaxEntriesPerLedger(2);
        Set<String> interceptorNames = new HashSet<>();
        interceptorNames.add("org.apache.pulsar.common.intercept.AppendIndexMetadataInterceptor");
        ManagedLedgerInterceptor interceptor = new ManagedLedgerInterceptorImpl(
                BrokerEntryMetadataUtils.loadBrokerEntryMetadataInterceptors(interceptorNames,
                Thread.currentThread().getContextClassLoader()),null);
        config.setManagedLedgerInterceptor(interceptor);


        ManagedLedger ledger = factory.open(ledgerName, config);
        OffsetFinder offsetFinder1 = new OffsetFinder((ManagedLedgerImpl) ledger);
        OffsetFinder offsetFinder2 = new OffsetFinder((ManagedLedgerImpl) ledger);

        // Add entries across multiple ledgers
        addEntriesToLedger(ledger, maxEntriesPerLedger, mockBatchSize); // First ledger
        Thread.sleep(100);
        addEntriesToLedger(ledger, maxEntriesPerLedger, mockBatchSize); // Second ledger
        Thread.sleep(100);
        addEntriesToLedger(ledger, maxEntriesPerLedger, mockBatchSize); // Third ledger

        // Wait a bit to ensure entries have different timestamps
        Thread.sleep(2000);
        long currentTime = System.currentTimeMillis();

        // Test multiple timestamps
        long[] timestampsToTest = {
            currentTime - 1000, // Past timestamp
            currentTime - 500,  // Recent past
            currentTime,        // Current time
            currentTime + 1000  // Future timestamp
        };

        for (long timestamp : timestampsToTest) {
            // Test optimized search
            CompletableFuture<Position> optimizedResult = new CompletableFuture<>();
            offsetFinder1.findMessages(timestamp, new AsyncCallbacks.FindEntryCallback() {
                @Override
                public void findEntryComplete(Position position, Object ctx) {
                    optimizedResult.complete(position);
                }

                @Override
                public void findEntryFailed(ManagedLedgerException exception,
                                            java.util.Optional<Position> position, Object ctx) {
                    optimizedResult.completeExceptionally(exception);
                }
            }, true); // optimizeSearch = true

            // Test non-optimized search
            CompletableFuture<Position> nonOptimizedResult = new CompletableFuture<>();
            offsetFinder2.findMessages(timestamp, new AsyncCallbacks.FindEntryCallback() {
                @Override
                public void findEntryComplete(Position position, Object ctx) {
                    nonOptimizedResult.complete(position);
                }

                @Override
                public void findEntryFailed(ManagedLedgerException exception,
                                            java.util.Optional<Position> position, Object ctx) {
                    nonOptimizedResult.completeExceptionally(exception);
                }
            }, false); // optimizeSearch = false

            // Wait for both searches to complete and compare results
            Position optimizedPos = optimizedResult.get();
            Position nonOptimizedPos = nonOptimizedResult.get();

            // Both methods should return the same position
            if (optimizedPos == null && nonOptimizedPos == null) {
                // Both found no matching position - this is consistent
                log.info("Timestamp {}: Both search methods returned null (no matching position)", timestamp);
                continue;
            }
            log.info("Timestamp {}: Optimized Position = {}, Non-Optimized Position = {}",
                    timestamp, optimizedPos, nonOptimizedPos);
            if (optimizedPos != null && nonOptimizedPos != null) {
                assertEquals(optimizedPos, nonOptimizedPos,
                    String.format("Optimized and non-optimized search results should be identical for timestamp %d. " +
                                "Optimized: %s, Non-optimized: %s", timestamp, optimizedPos, nonOptimizedPos));
            } else {
                assertEquals(optimizedPos, nonOptimizedPos,
                    String.format("One search method returned null while the other didn't for timestamp %d. " +
                                "Optimized: %s, Non-optimized: %s", timestamp, optimizedPos, nonOptimizedPos));
            }
        }

        ledger.close();
    }

    @Test
    public void testSetFirstEntryIndex() throws Exception {
        final int mockBatchSize = 2;
        final int maxEntriesPerLedger = 5;
        final String ledgerName = "my_ml_broker_entry_metadata_test_ledger";

        ManagedLedgerConfig managedLedgerConfig = createTestConfig(maxEntriesPerLedger);

        ManagedLedger ledger = factory.open(ledgerName, managedLedgerConfig);

        long firstLedgerId = addEntriesToLedger(ledger, maxEntriesPerLedger, mockBatchSize);

        assertEquals(ledger.getManagedLedgerInterceptor().getIndex(), 9);
        long earliestOffset = MessageMetadataUtils.getOffsetOfPosition((ManagedLedgerImpl) ledger,
                new PositionImpl(firstLedgerId, 0), false,
                -2, true,
                5).get();
        verifyStartOffsetPerLedger(ledger, firstLedgerId, earliestOffset - 1);

        // roll over ledger
        long secondLedgerId = addEntriesToLedger(ledger, maxEntriesPerLedger, mockBatchSize);
        assertEquals(ledger.getManagedLedgerInterceptor().getIndex(), 19);
        assertNotEquals(firstLedgerId, secondLedgerId);
        assertNotEquals(firstLedgerId, secondLedgerId);
        long earliestOffset2 = MessageMetadataUtils.getOffsetOfPosition((ManagedLedgerImpl) ledger,
                new PositionImpl(secondLedgerId, 0), false,
                -2, true,
                5).get();
        verifyStartOffsetPerLedger(ledger, secondLedgerId, earliestOffset2 - 1);

        // reopen ledger
        ledger.close();

        // / Reopen the same managed-ledger
        @Cleanup("shutdown")
        ManagedLedgerFactoryImpl factory2 = new ManagedLedgerFactoryImpl(metadataStore, bkc);
        ledger = factory2.open(ledgerName, managedLedgerConfig);

        verifyStartOffsetPerLedger(ledger, firstLedgerId, earliestOffset - 1);
        verifyStartOffsetPerLedger(ledger, secondLedgerId, earliestOffset2 - 1);

        long thirdLedgerId = addEntriesToLedger(ledger, maxEntriesPerLedger, mockBatchSize);
        assertEquals(ledger.getManagedLedgerInterceptor().getIndex(), 29);
        assertNotEquals(secondLedgerId, thirdLedgerId);
        earliestOffset = MessageMetadataUtils.getOffsetOfPosition((ManagedLedgerImpl) ledger,
                new PositionImpl(thirdLedgerId, 0), false,
                -2, true,
                5).get();
        verifyStartOffsetPerLedger(ledger, thirdLedgerId, earliestOffset - 1);

        ledger.close();
    }
}
