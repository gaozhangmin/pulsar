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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.FIRST_ENTRY_TIMESTAMP_PROPS;

import com.google.common.base.Predicate;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.AsyncCallbacks.FindEntryCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedCursor.FindPositionConstraint;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.common.protocol.Commands;

/**
 * given a timestamp find the first message (position) (published) at or before the timestamp.
 * Most of the code is similar to Pulsar class `PersistentMessageFinder`.
 */
@Slf4j
public class OffsetFinder implements AsyncCallbacks.FindEntryCallback {
    private final ManagedLedgerImpl managedLedger;
    private long timestamp = 0;

    private static final int FALSE = 0;
    private static final int TRUE = 1;
    @SuppressWarnings("unused")
    private volatile int messageFindInProgress = FALSE;
    private static final AtomicIntegerFieldUpdater<OffsetFinder> messageFindInProgressUpdater =
            AtomicIntegerFieldUpdater.newUpdater(OffsetFinder.class, "messageFindInProgress");

    public OffsetFinder(ManagedLedgerImpl managedLedger) {
        this.managedLedger = managedLedger;
    }

    public void findMessages(final long timestamp,
                             AsyncCallbacks.FindEntryCallback callback,
                             boolean optimizeSearch) {
        this.timestamp = timestamp;
        if (messageFindInProgressUpdater.compareAndSet(this, FALSE, TRUE)) {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Starting message position find at timestamp {}", managedLedger.getName(), timestamp);
            }
            if (optimizeSearch) {
                asyncFindNewestMatching(
                        ManagedCursor.FindPositionConstraint.SearchAllAvailableEntries, timestamp,entry -> {
                    if (entry == null) {
                        return false;
                    }
                    try {
                        return Commands.getEntryTimestamp(entry.getDataBuffer().duplicate()) <= timestamp;
                    } catch (Exception e) {
                        log.error("[{}] Error deserialize message for message position find", managedLedger.getName(), e);
                    } finally {
                        entry.release();
                    }
                    return false;
                }, this, callback);
            } else {
                asyncFindNewestMatching(ManagedCursor.FindPositionConstraint.SearchAllAvailableEntries, entry -> {
                    if (entry == null) {
                        return false;
                    }
                    try {
                        return Commands.getEntryTimestamp(entry.getDataBuffer().duplicate()) <= timestamp;
                    } catch (Exception e) {
                        log.error("[{}] Error deserialize message for message position find", managedLedger.getName(), e);
                    } finally {
                        entry.release();
                    }
                    return false;
                }, this, callback);
            }


        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}] Ignore message position find scheduled task, last find is still running",
                        managedLedger.getName());
            }
            callback.findEntryFailed(
                    new ManagedLedgerException.ConcurrentFindCursorPositionException("last find is still running"),
                    Optional.empty(),
                    null);
        }
    }

    @Override
    public void findEntryComplete(Position position, Object ctx) {
        checkArgument(ctx instanceof AsyncCallbacks.FindEntryCallback);
        AsyncCallbacks.FindEntryCallback callback = (AsyncCallbacks.FindEntryCallback) ctx;
        if (position != null) {
            log.info("[{}] Found position {} closest to provided timestamp {}",
                    managedLedger.getName(), position, timestamp);
        } else {
            if (log.isDebugEnabled()) {
                log.debug("[{}] No position found closest to provided timestamp {}",
                        managedLedger.getName(), timestamp);
            }
        }
        messageFindInProgress = FALSE;
        callback.findEntryComplete(position, null);
    }

    @Override
    public void findEntryFailed(ManagedLedgerException exception, Optional<Position> position, Object ctx) {
        checkArgument(ctx instanceof AsyncCallbacks.FindEntryCallback);
        AsyncCallbacks.FindEntryCallback callback = (AsyncCallbacks.FindEntryCallback) ctx;
        if (log.isDebugEnabled()) {
            log.debug("[{}] Message position find operation failed for provided timestamp {}",
                    managedLedger.getName(), timestamp, exception);
        }
        messageFindInProgress = FALSE;
        callback.findEntryFailed(exception, position, null);
    }

    public void asyncFindNewestMatching(FindPositionConstraint constraint,
                                        long timestamp,
                                        Predicate<Entry> condition,
                                        FindEntryCallback callback, Object ctx) {
        checkState(constraint == FindPositionConstraint.SearchAllAvailableEntries);

        // Use findStartingSearchLedger to optimize the starting position
        findStartingSearchLedger(timestamp).whenComplete((startLedgerId, throwable) -> {
            if (throwable != null) {
                callback.findEntryFailed(new ManagedLedgerException("Failed to find starting ledger", throwable),
                        Optional.empty(), ctx);
                return;
            }

            PositionImpl startPosition;
            long max = managedLedger.getNumberOfEntries() - 1;

            if (startLedgerId == null) {
                // No suitable ledger found, use the original approach
                startPosition = managedLedger.getFirstPosition();
            } else {
                // Start from the optimized ledger position
                startPosition = new PositionImpl(startLedgerId, -1);
            }

            if (startPosition == null) {
                callback.findEntryFailed(new ManagedLedgerException("Couldn't find start position"),
                        Optional.empty(), ctx);
                return;
            }

            // Ensure we have a valid starting position
            startPosition = managedLedger.getNextValidPosition(startPosition);
            if (startPosition == null) {
                callback.findEntryFailed(new ManagedLedgerException("No valid starting position found"),
                        Optional.empty(), ctx);
                return;
            }

            // Use the optimized OpFindNewestEntry with the calculated starting position
            OpFindNewestEntry op = new OpFindNewestEntry(managedLedger, startPosition, condition, max, callback, ctx);
            op.find();
        });
    }


    private CompletableFuture<Long> findStartingSearchLedger(long timestamp) {
        CompletableFuture<Long> result = new CompletableFuture<>();

        if (managedLedger.getLedgersInfo().isEmpty()) {
            result.complete(null);
            return result;
        }

        // Check ledgers in order to find the first one where firstEntryIndex <= targetOffset
        checkLedgersRecursively(managedLedger.getLedgersInfo().navigableKeySet().iterator(), timestamp, result);

        return result;
    }

    private void checkLedgersRecursively(java.util.Iterator<Long> ledgerIterator,
                                         long targetTimestamp,
                                         CompletableFuture<Long> result) {
        if (!ledgerIterator.hasNext()) {
            // No suitable ledger found, use the last ledger
            result.complete(managedLedger.getLedgersInfo().isEmpty() ? null : managedLedger.getLedgersInfo().lastKey());
            return;
        }

        Long ledgerId = ledgerIterator.next();
        // Check if this ledger has firstEntryIndex property
        managedLedger.asyncGetLedgerProperty(ledgerId, FIRST_ENTRY_TIMESTAMP_PROPS)
                .whenComplete((firstEntryTsStr, throwable) -> {
                    if (throwable != null) {
                        result.complete(ledgerId);
                        return;
                    }

                    // If firstEntryIndex property is missing, we cannot optimize and should use this ledger
                    if (firstEntryTsStr == null || firstEntryTsStr.isEmpty()) {
                        result.complete(ledgerId);
                        return;
                    }

                    long firstEntryIndex;
                    try {
                        firstEntryIndex = Long.parseLong(firstEntryTsStr);
                    } catch (NumberFormatException e) {
                        result.complete(ledgerId);
                        return;
                    }

                    if (firstEntryIndex <= targetTimestamp) {
                        // Found a suitable ledger, but need to check if the next ledger might be better
                        if (ledgerIterator.hasNext()) {
                            Long nextLedgerId = managedLedger.getLedgersInfo().higherKey(ledgerId);
                            if (nextLedgerId != null) {
                                managedLedger.asyncGetLedgerProperty(nextLedgerId, FIRST_ENTRY_TIMESTAMP_PROPS)
                                        .whenComplete((firstEntryTsStr1, nextThrowable) -> {
                                            if (nextThrowable != null
                                                    || firstEntryTsStr1 == null
                                                    || firstEntryTsStr1.isEmpty()) {
                                                // Next ledger has no firstEntryIndex or error, use current ledger
                                                result.complete(ledgerId);
                                                return;
                                            }

                                            try {
                                                long nextFirstIndex = Long.parseLong(firstEntryTsStr1);
                                                if (nextFirstIndex > targetTimestamp) {
                                                    // Next ledger is not suitable, use current ledger
                                                    result.complete(ledgerId);
                                                } else {
                                                    // Continue checking next ledgers
                                                    checkLedgersRecursively(ledgerIterator, targetTimestamp, result);
                                                }
                                            } catch (NumberFormatException e) {
                                                // Next ledger has invalid firstEntryIndex, use current ledger
                                                result.complete(ledgerId);
                                            }
                                        });
                            } else {
                                result.complete(ledgerId);
                            }
                        } else {
                            result.complete(ledgerId);
                        }
                    } else {
                        // This ledger's first entry is greater than target, use previous ledger if exists
                        Long prevLedgerId = managedLedger.getLedgersInfo().lowerKey(ledgerId);
                        result.complete(prevLedgerId != null ? prevLedgerId : ledgerId);
                    }
                });
    }

    public void asyncFindNewestMatching(FindPositionConstraint constraint, Predicate<Entry> condition,
                                        FindEntryCallback callback, Object ctx) {
        checkState(constraint == FindPositionConstraint.SearchAllAvailableEntries);

        // return PositionImpl(firstLedgerId, -1)
        PositionImpl startPosition = managedLedger.getFirstPosition();
        long max = managedLedger.getNumberOfEntries() - 1;

        if (startPosition == null) {
            callback.findEntryFailed(new ManagedLedgerException("Couldn't find start position"), Optional.empty(), ctx);
            return;
        } else {
            startPosition = managedLedger.getNextValidPosition(startPosition);
        }

        OpFindNewestEntry op = new OpFindNewestEntry(managedLedger, startPosition, condition, max, callback, ctx);
        op.find();
    }
}