package ru.mail.polis.lsm.vladislav_fetisov;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class Utils {

    private Utils() {

    }

    public static int leftBinarySearch(int l, int r, @Nonnull ByteBuffer key, BigByteBuffer records, ByteBuffer offsets) {
        while (l != r) {
            int mid = (l + r) >>> 1;
            int res = compareKeys(mid, key, records, offsets);
            if (res == 0) {
                return mid;
            }
            if (res > 0) {
                r = mid;
            } else {
                l = mid + 1;
            }
        }
        if (l == offsets.limit() / Long.BYTES) {
            return -1;
        }
        return l;
    }

    public static int rightBinarySearch(int l, int r, @Nonnull ByteBuffer key, BigByteBuffer records, ByteBuffer offsets) {
        if (l == r) {
            return l;
        }
        if (compareKeys(l, key, records, offsets) >= 0) {
            return -1;
        }
        if (compareKeys(r - 1, key, records, offsets) < 0) {
            return r;
        }
        while (l != r) {
            int mid = (l + r + 1) / 2;
            int res = compareKeys(mid, key, records, offsets);
            if (res == 0) {
                return mid;
            }
            if (res > 0) {
                r = mid - 1;
            } else {
                l = mid;
            }
        }
        return l + 1;
    }

    private static int compareKeys(int mid, ByteBuffer key, BigByteBuffer records, ByteBuffer offsets) {
        ByteBuffer buffer = readKey(mid, records, offsets);
        return buffer.compareTo(key);
    }


    private static ByteBuffer readKey(int index, BigByteBuffer records, ByteBuffer offsets) {
        long offset = getLong(offsets, index * Long.BYTES);
        int length = getInt(records, offset);
        return records.getByLength(length); //removed duplicate
    }

    public static int getInt(BigByteBuffer buffer, long offset) {
        buffer.position(offset);
        return buffer.getInt();
    }

    public static long getLong(ByteBuffer buffer, int offset) {
        buffer.position(offset);
        return buffer.getLong();
    }

    public static void checkMemory(AtomicInteger memoryConsumption) {
        long usedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        System.out.println("our memory " + memoryConsumption.get() / 1024L);
        System.out.println("memory use: " + usedMemory / 1024L);
        long free = Runtime.getRuntime().maxMemory() - usedMemory;
        System.out.println("free: " + free / 1024L);
        System.out.println("all " + (usedMemory + free) / 1024L);
    }
}
