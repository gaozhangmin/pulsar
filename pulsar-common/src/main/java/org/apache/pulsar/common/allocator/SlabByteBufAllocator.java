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
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.SlabByteBuf;
import io.netty.util.internal.ObjectUtil;
import org.jspecify.annotations.Nullable;

public class SlabByteBufAllocator implements ByteBufAllocator, AutoCloseable {

    public static final int PAGE_SIZE = PooledByteBufAllocator.defaultPageSize();
    static final int MAX_ORDER = PooledByteBufAllocator.defaultMaxOrder();
    static final int CHUNK_SIZE = PAGE_SIZE << MAX_ORDER;

    private final ByteBufAllocator delegate;

    @Nullable
    private final SlabPagePool pagePool;

    public SlabByteBufAllocator(ByteBufAllocator delegate, @Nullable SlabPagePool pagePool) {
        this.delegate = ObjectUtil.checkNotNull(delegate, "delegate");
        this.pagePool = pagePool;
    }

    public ByteBufAllocator delegate() {
        return delegate;
    }

    @Override
    public ByteBuf buffer() {
        return delegate.buffer();
    }

    @Override
    public ByteBuf buffer(int initialCapacity) {
        return delegate.buffer(initialCapacity);
    }

    @Override
    public ByteBuf buffer(int initialCapacity, int maxCapacity) {
        return delegate.buffer(initialCapacity, maxCapacity);
    }

    @Override
    public ByteBuf ioBuffer() {
        return delegate.ioBuffer();
    }

    @Override
    public ByteBuf ioBuffer(int initialCapacity) {
        return delegate.ioBuffer(initialCapacity);
    }

    @Override
    public ByteBuf ioBuffer(int initialCapacity, int maxCapacity) {
        return delegate.ioBuffer(initialCapacity, maxCapacity);
    }

    @Override
    public ByteBuf heapBuffer() {
        return delegate.heapBuffer();
    }

    @Override
    public ByteBuf heapBuffer(int initialCapacity) {
        return delegate.heapBuffer(initialCapacity);
    }

    @Override
    public ByteBuf heapBuffer(int initialCapacity, int maxCapacity) {
        return delegate.heapBuffer(initialCapacity, maxCapacity);
    }

    @Override
    public ByteBuf directBuffer() {
        return delegate.directBuffer();
    }

    @Override
    public ByteBuf directBuffer(int initialCapacity) {
        return directBuffer(initialCapacity, initialCapacity);
    }

    @Override
    public ByteBuf directBuffer(int initialCapacity, int maxCapacity) {
        if (initialCapacity == maxCapacity && initialCapacity > PAGE_SIZE && initialCapacity < CHUNK_SIZE) {
            return new SlabByteBuf(delegate, pagePool, initialCapacity);
        }
        return delegate.directBuffer(initialCapacity, maxCapacity);
    }

    @Override
    public CompositeByteBuf compositeBuffer() {
        return delegate.compositeBuffer();
    }

    @Override
    public CompositeByteBuf compositeBuffer(int maxNumComponents) {
        return delegate.compositeBuffer(maxNumComponents);
    }

    @Override
    public CompositeByteBuf compositeHeapBuffer() {
        return delegate.compositeHeapBuffer();
    }

    @Override
    public CompositeByteBuf compositeHeapBuffer(int maxNumComponents) {
        return delegate.compositeHeapBuffer(maxNumComponents);
    }

    @Override
    public CompositeByteBuf compositeDirectBuffer() {
        return delegate.compositeDirectBuffer();
    }

    @Override
    public CompositeByteBuf compositeDirectBuffer(int maxNumComponents) {
        return delegate.compositeDirectBuffer(maxNumComponents);
    }

    @Override
    public boolean isDirectBufferPooled() {
        return delegate.isDirectBufferPooled();
    }

    @Override
    public int calculateNewCapacity(int minNewCapacity, int maxCapacity) {
        return delegate.calculateNewCapacity(minNewCapacity, maxCapacity);
    }

    @Override
    public void close() {
        if (pagePool != null) {
            pagePool.close();
        }
    }

    @Override
    public String toString() {
        return "SlabByteBufAllocator{delegate=" + delegate
                + ", pageSize=" + PAGE_SIZE
                + ", pagePool=" + pagePool
                + '}';
    }
}
