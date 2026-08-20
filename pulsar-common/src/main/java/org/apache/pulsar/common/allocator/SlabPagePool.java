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
package org.apache.pulsar.common.allocator;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SlabPagePool implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SlabPagePool.class);

    private static final class PooledPage {
        final ByteBuf buf;
        volatile long lastTouchNanos;

        PooledPage(ByteBuf buf) {
            this.buf = buf;
            this.lastTouchNanos = System.nanoTime();
        }
    }

    private final int pageSize;

    private final int maxPages;

    private final Supplier<Long> idleEvictMsSupplier;

    private final ByteBufAllocator delegate;

    private final ConcurrentLinkedDeque<PooledPage> pool = new ConcurrentLinkedDeque<>();

    private final AtomicInteger pooledCount = new AtomicInteger(0);

    private final ScheduledExecutorService scheduler;
    private final ScheduledFuture<?> evictTask;

    public SlabPagePool(ByteBufAllocator delegate, int pageSize,
                        long maxTotalBytes, Supplier<Long> idleEvictMsSupplier, long evictIntervalMs) {
        this.delegate = delegate;
        this.pageSize = pageSize;
        this.maxPages = (int) Math.max(1, maxTotalBytes / pageSize);
        this.idleEvictMsSupplier = idleEvictMsSupplier;

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "slab-page-pool-evictor");
            t.setDaemon(true);
            return t;
        });
        this.evictTask = scheduler.scheduleAtFixedRate(
                this::drainIdle, evictIntervalMs, evictIntervalMs, TimeUnit.MILLISECONDS);
    }

    public ByteBuf acquire() {
        PooledPage pp = pool.pollFirst();
        if (pp != null) {
            pooledCount.decrementAndGet();
            ByteBuf buf = pp.buf;
            buf.clear();
            return buf;
        }
        return delegate.directBuffer(pageSize, pageSize);
    }

    public void offer(ByteBuf page) {
        int prev = pooledCount.getAndUpdate(n -> n < maxPages ? n + 1 : n);
        if (prev >= maxPages) {
            page.release();
            return;
        }
        pool.offerFirst(new PooledPage(page));
    }

    void drainIdle() {
        if (pool.isEmpty()) {
            return;
        }
        // Read the current idle threshold dynamically to support config hot-reload.
        long currentIdleEvictNanos = TimeUnit.MILLISECONDS.toNanos(idleEvictMsSupplier.get());
        long now = System.nanoTime();
        List<PooledPage> toRelease = new ArrayList<>();

        PooledPage pp;
        while ((pp = pool.peekLast()) != null) {
            if (now - pp.lastTouchNanos >= currentIdleEvictNanos) {
                if (pool.removeLastOccurrence(pp)) {
                    pooledCount.decrementAndGet();
                    toRelease.add(pp);
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        if (!toRelease.isEmpty()) {
            log.debug("SlabPagePool: evicting {} idle pages", toRelease.size());
            for (PooledPage p : toRelease) {
                p.buf.release();
            }
        }
    }

    @Override
    public void close() {
        evictTask.cancel(false);
        scheduler.shutdown();

        PooledPage pp;
        int released = 0;
        while ((pp = pool.pollFirst()) != null) {
            pooledCount.decrementAndGet();
            pp.buf.release();
            released++;
        }
        if (released > 0) {
            log.debug("SlabPagePool: closed, released {} cached pages", released);
        }
    }

    public int pooledPageCount() {
        return pooledCount.get();
    }

    public int maxPageCount() {
        return maxPages;
    }

    @Override
    public String toString() {
        return "SlabPagePool{pooled=" + pooledCount.get()
                + "/" + maxPages
                + ", pageSize=" + pageSize
                + '}';
    }
}
