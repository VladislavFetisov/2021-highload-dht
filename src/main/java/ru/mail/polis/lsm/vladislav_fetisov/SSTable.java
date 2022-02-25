package ru.mail.polis.lsm.vladislav_fetisov;

import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class SSTable implements Closeable {
    public static final String SUFFIX_INDEX = "i";
    private static final Method CLEAN;
    private volatile Path file;
    private volatile Path offsetsName;
    private MappedByteBuffer nmap;
    private MappedByteBuffer offsetsMap;

    static {
        try {
            Class<?> aClass = Class.forName("sun.nio.ch.FileChannelImpl");
            CLEAN = aClass.getDeclaredMethod("unmap", MappedByteBuffer.class);
            CLEAN.setAccessible(true);
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            throw new IllegalStateException();
        }
    }

    private SSTable(Path file) {
        this.file = file;
        Path offsetsName = pathWithSuffix(file, SUFFIX_INDEX);
        this.offsetsName = offsetsName;
        try (FileChannel tableChannel = FileChannel.open(file, StandardOpenOption.READ);
             FileChannel offsetChannel = FileChannel.open(offsetsName, StandardOpenOption.READ)) {

            nmap = tableChannel.map(FileChannel.MapMode.READ_ONLY, 0, tableChannel.size());
            offsetsMap = offsetChannel.map(FileChannel.MapMode.READ_ONLY, 0, offsetChannel.size());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Path getFileName() {
        return file;
    }

    public Path getOffsetsName() {
        return offsetsName;
    }

    public synchronized void setFileName(Path file) {
        this.file = file;
        this.offsetsName = pathWithSuffix(file, SUFFIX_INDEX);
    }

    public static List<SSTable> getAllSSTables(Path dir) throws IOException {
        List<SSTable> result = new ArrayList<>();
        for (int i = 0; ; i++) {
            Path SSTable = dir.resolve(String.valueOf(i));
            if (!Files.exists(SSTable)) {
                break;
            }
            result.add(new SSTable(SSTable));
        }
        return result;
    }

    Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        ByteBuffer recordsBuffer = nmap.asReadOnlyBuffer();
        ByteBuffer offsetsBuffer = offsetsMap.asReadOnlyBuffer();
        int leftPos;
        int rightPos;
        int temp;

        int limit = offsetsBuffer.limit() / Integer.BYTES;
        if (fromKey == null) {
            leftPos = 0;
        } else {
            temp = Utils.leftBinarySearch(0, limit, fromKey, recordsBuffer, offsetsBuffer);
            if (temp == -1) {
                return Collections.emptyIterator();
            }
            leftPos = Utils.getInt(offsetsBuffer, temp * Integer.BYTES);
        }


        if (toKey == null) {
            rightPos = recordsBuffer.limit();
        } else {
            temp = Utils.rightBinarySearch(0, limit, toKey, recordsBuffer, offsetsBuffer);
            if (temp == -1) {
                return Collections.emptyIterator();
            }
            if (temp == limit) {
                rightPos = recordsBuffer.limit();
            } else {
                rightPos = Utils.getInt(offsetsBuffer, temp * Integer.BYTES);
            }
        }


        recordsBuffer.position(leftPos);
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return recordsBuffer.position() < rightPos;
            }

            @Override
            public Record next() {
                ByteBuffer key = read(recordsBuffer);
                ByteBuffer value = read(recordsBuffer);
                if (value == null) {
                    return Record.tombstone(key);
                }
                return Record.of(key, value);
            }
        };
    }

    @Nullable
    private ByteBuffer read(ByteBuffer from) {
        int length = from.getInt();
        if (length == -1) {
            return null;
        }
        ByteBuffer limit = from.slice().limit(length);
        from.position(from.position() + length);
        return limit;
    }

    static SSTable write(Iterator<Record> records, Path tableName) throws IOException {
        String tmpSuffix = "_tmp";
        Path offsetsName = pathWithSuffix(tableName, SUFFIX_INDEX);
        Path tableTmp = pathWithSuffix(tableName, tmpSuffix);
        Path offsetsTmp = pathWithSuffix(offsetsName, tmpSuffix);
        int offset = 0;
        ByteBuffer forLength = ByteBuffer.allocate(Integer.BYTES);
        try (FileChannel tableChannel = open(tableTmp);
             FileChannel offsetsChannel = open(offsetsTmp)) {
            while (records.hasNext()) {
                Record record = records.next();
                writeRecord(forLength, tableChannel, record);
                writeInt(offset, offsetsChannel, forLength);
                offset += LsmDAO.sizeOf(record);
            }
            tableChannel.force(false);
            offsetsChannel.force(false);
        } catch (IOException e) {
            throw new IOException();
        }
        rename(offsetsTmp, offsetsName);
        rename(tableTmp, tableName);

        return new SSTable(tableName);
    }

    public static void rename(Path source, Path target) throws IOException {
        Files.deleteIfExists(target);
        Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
    }

    public static Path pathWithSuffix(Path tableName, String suffixIndex) {
        return tableName.resolveSibling(tableName.getFileName() + suffixIndex);
    }

    private static FileChannel open(Path filename) throws IOException {
        return FileChannel.open(filename,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
    }

    private static void writeRecord(ByteBuffer forLength, FileChannel channel, Record record) throws IOException {
        writeBuffer(record.getKey(), channel, forLength);
        writeBuffer(record.getValue(), channel, forLength);
    }

    private static void writeBuffer(@Nullable ByteBuffer value, WritableByteChannel channel, ByteBuffer forLength) throws IOException {
        forLength.position(0);
        forLength.putInt((value == null) ? -1 : value.remaining());
        forLength.position(0);
        channel.write(forLength);
        if (value != null) {
            channel.write(value);
        }
    }

    private static void writeInt(int value, WritableByteChannel channel, ByteBuffer elementBuffer) throws IOException {
        elementBuffer.position(0);
        elementBuffer.putInt(value);
        elementBuffer.position(0);
        channel.write(elementBuffer);
    }

    public void close() throws IOException {
        IOException exception = null;
        try {
            unmap(nmap);
        } catch (IOException e) {
            exception = e;
        }
        try {
            unmap(offsetsMap);
        } catch (IOException e) {
            if (exception != null) {
                e.addSuppressed(exception);
            }
            throw e;
        }
    }

    private void unmap(@Nullable MappedByteBuffer nmap) throws IOException {
        if (nmap == null) {
            return;
        }
        try {
            CLEAN.invoke(null, nmap);
        } catch (InvocationTargetException | IllegalAccessException e) {
            throw new IOException();
        }
    }
}

