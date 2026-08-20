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
package io.netty.buffer;

import io.netty.util.internal.ObjectUtil;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import org.apache.pulsar.common.allocator.SlabPagePool;

public final class SlabByteBuf extends AbstractReferenceCountedByteBuf {

    static final int PAGE_SIZE = PooledByteBufAllocator.defaultPageSize();

    private final ByteBufAllocator alloc;

    /**
     * Optional page pool.  When non-null, {@link #deallocate()} returns pages here instead of
     * releasing them directly to the underlying allocator, and the constructor acquires pages
     * from the pool instead of allocating fresh ones.
     */
    private final SlabPagePool pagePool;

    private final ByteBuf[] pages;

    /** Total logical capacity = pages.length * PAGE_SIZE. */
    private final int capacity;

    /** Constructs a SlabByteBuf without a page pool (pages always freshly allocated/released). */
    public SlabByteBuf(ByteBufAllocator alloc, int capacity) {
        this(alloc, null, capacity);
    }

    /**
     * Constructs a SlabByteBuf that acquires pages from {@code pagePool} (if non-null) and returns
     * them to the pool on deallocation instead of releasing them directly.
     */
    public SlabByteBuf(ByteBufAllocator alloc, SlabPagePool pagePool, int capacity) {
        super(roundUpToPage(ObjectUtil.checkPositiveOrZero(capacity, "capacity")));
        this.alloc = ObjectUtil.checkNotNull(alloc, "alloc");
        this.pagePool = pagePool;
        int numPages = (capacity + PAGE_SIZE - 1) / PAGE_SIZE;
        if (numPages == 0) {
            numPages = 1;
        }
        this.capacity = numPages * PAGE_SIZE;
        this.pages = new ByteBuf[numPages];
        for (int i = 0; i < numPages; i++) {
            pages[i] = (pagePool != null) ? pagePool.acquire() : alloc.directBuffer(PAGE_SIZE, PAGE_SIZE);
        }
    }

    private static int roundUpToPage(int capacity) {
        if (capacity == 0) {
            return PAGE_SIZE;
        }
        int pages = (capacity + PAGE_SIZE - 1) / PAGE_SIZE;
        return pages * PAGE_SIZE;
    }

    public CompositeByteBuf toCompositeByteBuf() {
        ensureAccessible();
        int wIdx = writerIndex();
        int rIdx = readerIndex();
        CompositeByteBuf composite = alloc.compositeDirectBuffer(pages.length);
        for (int i = 0; i < pages.length; i++) {
            int pageStart = i * PAGE_SIZE;
            int pageEnd = Math.min(pageStart + PAGE_SIZE, wIdx);
            if (pageEnd <= pageStart) {
                break; // no data written into this page or beyond
            }
            ByteBuf page = pages[i];
            pages[i] = null;
            composite.addComponent(true, page.slice(0, pageEnd - pageStart));
        }
        composite.readerIndex(rIdx);
        return composite;
    }

    @Override
    public int capacity() {
        return capacity;
    }

    @Override
    public ByteBuf capacity(int newCapacity) {
        // Fixed capacity — we do not support resizing.
        if (newCapacity != capacity) {
            throw new UnsupportedOperationException(
                    "SlabByteBuf does not support capacity change. current=" + capacity + " requested=" + newCapacity);
        }
        return this;
    }

    @Override
    public ByteBufAllocator alloc() {
        return alloc;
    }

    @Override
    @Deprecated
    public ByteOrder order() {
        return ByteOrder.BIG_ENDIAN;
    }

    @Override
    public ByteBuf unwrap() {
        return null;
    }

    @Override
    public boolean isDirect() {
        return true;
    }

    @Override
    public boolean hasArray() {
        return false;
    }

    @Override
    public byte[] array() {
        throw new UnsupportedOperationException("SlabByteBuf is a direct buffer");
    }

    @Override
    public int arrayOffset() {
        throw new UnsupportedOperationException("SlabByteBuf is a direct buffer");
    }

    @Override
    public boolean hasMemoryAddress() {
        return false;
    }

    @Override
    public long memoryAddress() {
        throw new UnsupportedOperationException("SlabByteBuf does not expose a contiguous memory address");
    }

    private ByteBuf pageFor(int index) {
        return pages[index / PAGE_SIZE];
    }

    private int offsetInPage(int index) {
        return index % PAGE_SIZE;
    }

    @Override
    protected byte _getByte(int index) {
        return pageFor(index).getByte(offsetInPage(index));
    }

    @Override
    protected short _getShort(int index) {
        int pageIdx = index / PAGE_SIZE;
        int off = index % PAGE_SIZE;
        if (off + 2 <= PAGE_SIZE) {
            return pages[pageIdx].getShort(off);
        }
        // spans two pages
        return (short) ((_getByte(index) & 0xFF) << 8 | (_getByte(index + 1) & 0xFF));
    }

    @Override
    protected short _getShortLE(int index) {
        int pageIdx = index / PAGE_SIZE;
        int off = index % PAGE_SIZE;
        if (off + 2 <= PAGE_SIZE) {
            return pages[pageIdx].getShortLE(off);
        }
        return (short) ((_getByte(index) & 0xFF) | ((_getByte(index + 1) & 0xFF) << 8));
    }

    @Override
    protected int _getUnsignedMedium(int index) {
        return (_getByte(index) & 0xFF) << 16 | (_getByte(index + 1) & 0xFF) << 8 | (_getByte(index + 2) & 0xFF);
    }

    @Override
    protected int _getUnsignedMediumLE(int index) {
        return (_getByte(index) & 0xFF) | ((_getByte(index + 1) & 0xFF) << 8) | ((_getByte(index + 2) & 0xFF) << 16);
    }

    @Override
    protected int _getInt(int index) {
        int pageIdx = index / PAGE_SIZE;
        int off = index % PAGE_SIZE;
        if (off + 4 <= PAGE_SIZE) {
            return pages[pageIdx].getInt(off);
        }
        // spans two pages — read byte by byte
        return (_getByte(index) & 0xFF) << 24
                | (_getByte(index + 1) & 0xFF) << 16
                | (_getByte(index + 2) & 0xFF) << 8
                | (_getByte(index + 3) & 0xFF);
    }

    @Override
    protected int _getIntLE(int index) {
        int pageIdx = index / PAGE_SIZE;
        int off = index % PAGE_SIZE;
        if (off + 4 <= PAGE_SIZE) {
            return pages[pageIdx].getIntLE(off);
        }
        return (_getByte(index) & 0xFF)
                | ((_getByte(index + 1) & 0xFF) << 8)
                | ((_getByte(index + 2) & 0xFF) << 16)
                | ((_getByte(index + 3) & 0xFF) << 24);
    }

    @Override
    protected long _getLong(int index) {
        int pageIdx = index / PAGE_SIZE;
        int off = index % PAGE_SIZE;
        if (off + 8 <= PAGE_SIZE) {
            return pages[pageIdx].getLong(off);
        }
        return ((long) _getByte(index) & 0xFF) << 56
                | ((long) _getByte(index + 1) & 0xFF) << 48
                | ((long) _getByte(index + 2) & 0xFF) << 40
                | ((long) _getByte(index + 3) & 0xFF) << 32
                | ((long) _getByte(index + 4) & 0xFF) << 24
                | ((long) _getByte(index + 5) & 0xFF) << 16
                | ((long) _getByte(index + 6) & 0xFF) << 8
                | ((long) _getByte(index + 7) & 0xFF);
    }

    @Override
    protected long _getLongLE(int index) {
        int pageIdx = index / PAGE_SIZE;
        int off = index % PAGE_SIZE;
        if (off + 8 <= PAGE_SIZE) {
            return pages[pageIdx].getLongLE(off);
        }
        return ((long) _getByte(index) & 0xFF)
                | (((long) _getByte(index + 1) & 0xFF) << 8)
                | (((long) _getByte(index + 2) & 0xFF) << 16)
                | (((long) _getByte(index + 3) & 0xFF) << 24)
                | (((long) _getByte(index + 4) & 0xFF) << 32)
                | (((long) _getByte(index + 5) & 0xFF) << 40)
                | (((long) _getByte(index + 6) & 0xFF) << 48)
                | (((long) _getByte(index + 7) & 0xFF) << 56);
    }

    @Override
    protected void _setByte(int index, int value) {
        pageFor(index).setByte(offsetInPage(index), value);
    }

    @Override
    protected void _setShort(int index, int value) {
        int pageIdx = index / PAGE_SIZE;
        int off = index % PAGE_SIZE;
        if (off + 2 <= PAGE_SIZE) {
            pages[pageIdx].setShort(off, value);
        } else {
            _setByte(index, (byte) (value >>> 8));
            _setByte(index + 1, (byte) value);
        }
    }

    @Override
    protected void _setShortLE(int index, int value) {
        int pageIdx = index / PAGE_SIZE;
        int off = index % PAGE_SIZE;
        if (off + 2 <= PAGE_SIZE) {
            pages[pageIdx].setShortLE(off, value);
        } else {
            _setByte(index, (byte) value);
            _setByte(index + 1, (byte) (value >>> 8));
        }
    }

    @Override
    protected void _setMedium(int index, int value) {
        _setByte(index, (byte) (value >>> 16));
        _setByte(index + 1, (byte) (value >>> 8));
        _setByte(index + 2, (byte) value);
    }

    @Override
    protected void _setMediumLE(int index, int value) {
        _setByte(index, (byte) value);
        _setByte(index + 1, (byte) (value >>> 8));
        _setByte(index + 2, (byte) (value >>> 16));
    }

    @Override
    protected void _setInt(int index, int value) {
        int pageIdx = index / PAGE_SIZE;
        int off = index % PAGE_SIZE;
        if (off + 4 <= PAGE_SIZE) {
            pages[pageIdx].setInt(off, value);
        } else {
            _setByte(index, (byte) (value >>> 24));
            _setByte(index + 1, (byte) (value >>> 16));
            _setByte(index + 2, (byte) (value >>> 8));
            _setByte(index + 3, (byte) value);
        }
    }

    @Override
    protected void _setIntLE(int index, int value) {
        int pageIdx = index / PAGE_SIZE;
        int off = index % PAGE_SIZE;
        if (off + 4 <= PAGE_SIZE) {
            pages[pageIdx].setIntLE(off, value);
        } else {
            _setByte(index, (byte) value);
            _setByte(index + 1, (byte) (value >>> 8));
            _setByte(index + 2, (byte) (value >>> 16));
            _setByte(index + 3, (byte) (value >>> 24));
        }
    }

    @Override
    protected void _setLong(int index, long value) {
        int pageIdx = index / PAGE_SIZE;
        int off = index % PAGE_SIZE;
        if (off + 8 <= PAGE_SIZE) {
            pages[pageIdx].setLong(off, value);
        } else {
            _setByte(index, (byte) (value >>> 56));
            _setByte(index + 1, (byte) (value >>> 48));
            _setByte(index + 2, (byte) (value >>> 40));
            _setByte(index + 3, (byte) (value >>> 32));
            _setByte(index + 4, (byte) (value >>> 24));
            _setByte(index + 5, (byte) (value >>> 16));
            _setByte(index + 6, (byte) (value >>> 8));
            _setByte(index + 7, (byte) value);
        }
    }

    @Override
    protected void _setLongLE(int index, long value) {
        int pageIdx = index / PAGE_SIZE;
        int off = index % PAGE_SIZE;
        if (off + 8 <= PAGE_SIZE) {
            pages[pageIdx].setLongLE(off, value);
        } else {
            _setByte(index, (byte) value);
            _setByte(index + 1, (byte) (value >>> 8));
            _setByte(index + 2, (byte) (value >>> 16));
            _setByte(index + 3, (byte) (value >>> 24));
            _setByte(index + 4, (byte) (value >>> 32));
            _setByte(index + 5, (byte) (value >>> 40));
            _setByte(index + 6, (byte) (value >>> 48));
            _setByte(index + 7, (byte) (value >>> 56));
        }
    }

    @Override
    public ByteBuf getBytes(int index, ByteBuf dst, int dstIndex, int length) {
        checkIndex(index, length);
        while (length > 0) {
            int pageIdx = index / PAGE_SIZE;
            int off = index % PAGE_SIZE;
            int chunk = Math.min(length, PAGE_SIZE - off);
            pages[pageIdx].getBytes(off, dst, dstIndex, chunk);
            index += chunk;
            dstIndex += chunk;
            length -= chunk;
        }
        return this;
    }

    @Override
    public ByteBuf getBytes(int index, byte[] dst, int dstIndex, int length) {
        checkIndex(index, length);
        while (length > 0) {
            int pageIdx = index / PAGE_SIZE;
            int off = index % PAGE_SIZE;
            int chunk = Math.min(length, PAGE_SIZE - off);
            pages[pageIdx].getBytes(off, dst, dstIndex, chunk);
            index += chunk;
            dstIndex += chunk;
            length -= chunk;
        }
        return this;
    }

    @Override
    public ByteBuf getBytes(int index, ByteBuffer dst) {
        checkIndex(index, dst.remaining());
        int length = dst.remaining();
        while (length > 0) {
            int pageIdx = index / PAGE_SIZE;
            int off = index % PAGE_SIZE;
            int chunk = Math.min(length, PAGE_SIZE - off);
            ByteBuffer pageBuf = pages[pageIdx].nioBuffer(off, chunk);
            dst.put(pageBuf);
            index += chunk;
            length -= chunk;
        }
        return this;
    }

    @Override
    public ByteBuf getBytes(int index, OutputStream out, int length) throws IOException {
        checkIndex(index, length);
        byte[] tmp = new byte[Math.min(length, PAGE_SIZE)];
        while (length > 0) {
            int pageIdx = index / PAGE_SIZE;
            int off = index % PAGE_SIZE;
            int chunk = Math.min(length, PAGE_SIZE - off);
            pages[pageIdx].getBytes(off, tmp, 0, chunk);
            out.write(tmp, 0, chunk);
            index += chunk;
            length -= chunk;
        }
        return this;
    }

    @Override
    public int getBytes(int index, GatheringByteChannel out, int length) throws IOException {
        checkIndex(index, length);
        // Build a NIO buffer list for scatter/gather IO
        int numBufs = (length + PAGE_SIZE - 1) / PAGE_SIZE + 1;
        ByteBuffer[] nioBuffers = new ByteBuffer[Math.min(numBufs, pages.length)];
        int idx = 0;
        int remaining = length;
        while (remaining > 0 && idx < nioBuffers.length) {
            int pageIdx = index / PAGE_SIZE;
            int off = index % PAGE_SIZE;
            int chunk = Math.min(remaining, PAGE_SIZE - off);
            nioBuffers[idx++] = pages[pageIdx].nioBuffer(off, chunk);
            index += chunk;
            remaining -= chunk;
        }
        if (idx == 1) {
            return out.write(nioBuffers[0]);
        }
        return (int) out.write(java.util.Arrays.copyOf(nioBuffers, idx));
    }

    @Override
    public int getBytes(int index, FileChannel out, long position, int length) throws IOException {
        checkIndex(index, length);
        int written = 0;
        while (length > 0) {
            int pageIdx = index / PAGE_SIZE;
            int off = index % PAGE_SIZE;
            int chunk = Math.min(length, PAGE_SIZE - off);
            ByteBuffer nioPage = pages[pageIdx].nioBuffer(off, chunk);
            int n = out.write(nioPage, position + written);
            written += n;
            index += chunk;
            length -= chunk;
        }
        return written;
    }

    @Override
    public ByteBuf setBytes(int index, ByteBuf src, int srcIndex, int length) {
        checkIndex(index, length);
        while (length > 0) {
            int pageIdx = index / PAGE_SIZE;
            int off = index % PAGE_SIZE;
            int chunk = Math.min(length, PAGE_SIZE - off);
            pages[pageIdx].setBytes(off, src, srcIndex, chunk);
            index += chunk;
            srcIndex += chunk;
            length -= chunk;
        }
        return this;
    }

    @Override
    public ByteBuf setBytes(int index, byte[] src, int srcIndex, int length) {
        checkIndex(index, length);
        while (length > 0) {
            int pageIdx = index / PAGE_SIZE;
            int off = index % PAGE_SIZE;
            int chunk = Math.min(length, PAGE_SIZE - off);
            pages[pageIdx].setBytes(off, src, srcIndex, chunk);
            index += chunk;
            srcIndex += chunk;
            length -= chunk;
        }
        return this;
    }

    @Override
    public ByteBuf setBytes(int index, ByteBuffer src) {
        checkIndex(index, src.remaining());
        while (src.hasRemaining()) {
            int pageIdx = index / PAGE_SIZE;
            int off = index % PAGE_SIZE;
            int chunk = Math.min(src.remaining(), PAGE_SIZE - off);
            ByteBuffer limited = src.duplicate();
            limited.limit(limited.position() + chunk);
            pages[pageIdx].setBytes(off, limited);
            src.position(src.position() + chunk);
            index += chunk;
        }
        return this;
    }

    @Override
    public int setBytes(int index, InputStream in, int length) throws IOException {
        checkIndex(index, length);
        int total = 0;
        while (length > 0) {
            int pageIdx = index / PAGE_SIZE;
            int off = index % PAGE_SIZE;
            int chunk = Math.min(length, PAGE_SIZE - off);
            int n = pages[pageIdx].setBytes(off, in, chunk);
            if (n < 0) {
                return total == 0 ? -1 : total;
            }
            total += n;
            index += n;
            length -= n;
        }
        return total;
    }

    @Override
    public int setBytes(int index, ScatteringByteChannel in, int length) throws IOException {
        checkIndex(index, length);
        int total = 0;
        while (length > 0) {
            int pageIdx = index / PAGE_SIZE;
            int off = index % PAGE_SIZE;
            int chunk = Math.min(length, PAGE_SIZE - off);
            int n = pages[pageIdx].setBytes(off, in, chunk);
            if (n < 0) {
                break;
            }
            total += n;
            index += n;
            length -= n;
        }
        return total;
    }

    @Override
    public int setBytes(int index, FileChannel in, long position, int length) throws IOException {
        checkIndex(index, length);
        int total = 0;
        while (length > 0) {
            int pageIdx = index / PAGE_SIZE;
            int off = index % PAGE_SIZE;
            int chunk = Math.min(length, PAGE_SIZE - off);
            int n = pages[pageIdx].setBytes(off, in, position + total, chunk);
            if (n < 0) {
                break;
            }
            total += n;
            index += n;
            length -= n;
        }
        return total;
    }

    @Override
    public int nioBufferCount() {
        return pages.length;
    }

    @Override
    public ByteBuffer nioBuffer(int index, int length) {
        // Single-page fast path
        int pageIdx = index / PAGE_SIZE;
        int off = index % PAGE_SIZE;
        if (off + length <= PAGE_SIZE) {
            return pages[pageIdx].nioBuffer(off, length);
        }
        // Multi-page: must copy into a single contiguous ByteBuffer
        ByteBuffer composite = isDirect()
                ? ByteBuffer.allocateDirect(length)
                : ByteBuffer.allocate(length);
        int remaining = length;
        int idx = index;
        while (remaining > 0) {
            int pIdx = idx / PAGE_SIZE;
            int pOff = idx % PAGE_SIZE;
            int chunk = Math.min(remaining, PAGE_SIZE - pOff);
            ByteBuffer slice = pages[pIdx].nioBuffer(pOff, chunk);
            composite.put(slice);
            idx += chunk;
            remaining -= chunk;
        }
        composite.flip();
        return composite;
    }

    @Override
    public ByteBuffer internalNioBuffer(int index, int length) {
        return nioBuffer(index, length);
    }

    @Override
    public ByteBuffer[] nioBuffers(int index, int length) {
        checkIndex(index, length);
        int firstPage = index / PAGE_SIZE;
        int lastPage = (index + length - 1) / PAGE_SIZE;
        int numBufs = lastPage - firstPage + 1;
        ByteBuffer[] result = new ByteBuffer[numBufs];
        int remaining = length;
        int idx = index;
        for (int i = 0; i < numBufs; i++) {
            int pIdx = idx / PAGE_SIZE;
            int pOff = idx % PAGE_SIZE;
            int chunk = Math.min(remaining, PAGE_SIZE - pOff);
            result[i] = pages[pIdx].nioBuffer(pOff, chunk);
            idx += chunk;
            remaining -= chunk;
        }
        return result;
    }

    @Override
    public ByteBuf copy(int index, int length) {
        checkIndex(index, length);
        ByteBuf copy = alloc.directBuffer(length, length);
        copy.writeBytes(this, index, length);
        return copy;
    }

    @Override
    protected void deallocate() {
        for (ByteBuf page : pages) {
            if (page == null) {
                continue;
            }
            if (pagePool != null) {
                // Return to pool: avoids immediate cross-thread MPSC CAS to allocating thread.
                // The pool will release the page lazily when it is evicted or the pool is closed.
                pagePool.offer(page);
            } else {
                page.release();
            }
        }
    }

    @Override
    public String toString() {
        return "SlabByteBuf(pages=" + pages.length
                + ", pageSize=" + PAGE_SIZE
                + ", capacity=" + capacity
                + ", readerIndex=" + readerIndex()
                + ", writerIndex=" + writerIndex()
                + ')';
    }
}
