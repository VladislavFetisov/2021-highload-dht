package ru.mail.polis.lsm.vladislav_fetisov;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.stream.IntStream;

public class BigByteBuffer {
    private final int SEGMENT_SIZE = Integer.MAX_VALUE;
    private final ByteBuffer[] buffers;
    private int bufNum;
    private final long limit;
    private long position = 0;

    public BigByteBuffer(FileChannel channel) throws IOException {
        limit = channel.size();
        if (limit == 0) {
            buffers = new ByteBuffer[0];
            return;
        }
        this.buffers = new MappedByteBuffer[(int) (limit / SEGMENT_SIZE) + 1];

        int buffersIndex = 0;
        for (long offset = 0; offset < limit; offset += SEGMENT_SIZE) {
            long remaining = limit - offset;
            long currentSize = Math.min(SEGMENT_SIZE, remaining);
            buffers[buffersIndex++] = channel.map(FileChannel.MapMode.READ_ONLY, offset, currentSize);
        }
    }

    private BigByteBuffer(ByteBuffer[] buffers, long position, long limit) {
        this.buffers = buffers;
        this.limit = limit;
        this.position = position;
    }

    public BigByteBuffer duplicate() {
        ByteBuffer[] duplicate = new MappedByteBuffer[buffers.length];
        IntStream.range(0, buffers.length).forEach(i -> duplicate[i] = buffers[i].duplicate());
        return new BigByteBuffer(duplicate, position, limit);
    }

    public ByteBuffer getByLength(int length) {
        ByteBuffer current = buffers[bufNum];
        position += length;
        int diff = current.remaining() - length;
        if (diff >= 0) {
            ByteBuffer buffer = current.slice().limit(length);
            current.position(current.position() + length);
            return buffer;
        }
        return getCrossBuffer(length, current, diff);
    }

    public int getInt() {
        ByteBuffer current = buffers[bufNum];
        int length = Integer.BYTES;
        position += length;
        int diff = current.remaining() - length;
        if (diff >= 0) {
            return current.getInt();
        }
        return getCrossBuffer(length, current, diff).getInt();
    }

    private ByteBuffer getCrossBuffer(int length, ByteBuffer current, int diff) {
        ByteBuffer buffer = ByteBuffer.allocate(length);
        int bound = Math.min(current.remaining(), length);
        for (int i = 0; i < bound; i++) {
            buffer.put(current.get());
        }
        current = buffers[++bufNum];
        current.position(0);
        diff = Math.abs(diff);
        for (int i = 0; i < diff; i++) {
            buffer.put(current.get());
        }
        return buffer.flip();
    }

    public void position(long position) {
        this.position = position;
        bufNum = (int) (position / SEGMENT_SIZE);
        if (bufNum < buffers.length) {
            buffers[bufNum].position((int) (position % SEGMENT_SIZE));
        }
    }

    public long position() {
        return position;
    }

    public long limit() {
        return limit;
    }
}
