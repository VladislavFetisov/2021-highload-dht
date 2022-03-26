package ru.mail.polis.lsm.vladislav_fetisov;

import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SSTable {
    public static final String SUFFIX_INDEX = "i";
    private final Path file;
    private final Path offsetsName;
    private BigByteBuffer nmap;
    private MappedByteBuffer offsetsMap;
    public static final SSTable DUMMY = new SSTable();


    public SSTable(Path file) {
        this.file = file;
        Path offsetsName = pathWithSuffix(file, SUFFIX_INDEX);
        this.offsetsName = offsetsName;
        try (FileChannel tableChannel = FileChannel.open(file, StandardOpenOption.READ);
             FileChannel offsetChannel = FileChannel.open(offsetsName, StandardOpenOption.READ)) {

            nmap = new BigByteBuffer(tableChannel);
            offsetsMap = offsetChannel.map(FileChannel.MapMode.READ_ONLY, 0, offsetChannel.size());

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private SSTable() {
        file = null;
        offsetsName = null;
    }

    public Path getFile() {
        return file;
    }

    public Path getOffsets() {
        return offsetsName;
    }


    public static List<SSTable> getAllSSTables(Path dir) {
        try (Stream<Path> stream = Files.list(dir)) {
            return stream
                    .filter(path -> !path.toString().endsWith(SUFFIX_INDEX))
                    .mapToInt(path -> Integer.parseInt(path.getFileName().toString()))
                    .sorted()
                    .mapToObj(i -> new SSTable(dir.resolve(String.valueOf(i))))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        BigByteBuffer recordsBuffer = nmap.duplicate();
        ByteBuffer offsetsBuffer = offsetsMap.duplicate();
        long leftPos;
        long rightPos;
        int temp;

        int limit = offsetsBuffer.limit() / Long.BYTES;
        if (fromKey == null) {
            leftPos = 0;
        } else {
            temp = Utils.leftBinarySearch(0, limit, fromKey, recordsBuffer, offsetsBuffer);
            if (temp == -1) {
                return Collections.emptyIterator();
            }
            leftPos = Utils.getLong(offsetsBuffer, temp * Long.BYTES);
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
                rightPos = Utils.getLong(offsetsBuffer, temp * Long.BYTES);
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
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
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
    private ByteBuffer read(BigByteBuffer from) {
        int length = from.getInt();
        if (length == -1) {
            return null;
        }
        return from.getByLength(length);
    }

    static SSTable write(Iterator<Record> records, Path tableName) throws IOException {
        String tmpSuffix = "_tmp";

        Path offsetsName = pathWithSuffix(tableName, SUFFIX_INDEX);
        Path tableTmp = pathWithSuffix(tableName, tmpSuffix);
        Path offsetsTmp = pathWithSuffix(offsetsName, tmpSuffix);

        ByteBuffer forLength = ByteBuffer.allocate(Long.BYTES);
        try (FileChannel tableChannel = open(tableTmp);
             FileChannel offsetsChannel = open(offsetsTmp)) {
            while (records.hasNext()) {
                Record record = records.next();
                writeLong(tableChannel.position(), offsetsChannel, forLength);
                writeRecord(forLength, tableChannel, record);
            }
            tableChannel.force(false);
            offsetsChannel.force(false);
        }
        rename(offsetsTmp, offsetsName);
        rename(tableTmp, tableName);

        return new SSTable(tableName);
    }

    public static void rename(Path source, Path target) throws IOException {
        Files.deleteIfExists(target);
        Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
        Files.deleteIfExists(source);
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
        forLength.flip();
        channel.write(forLength);
        if (value != null) {
            channel.write(value);
        }
        forLength.clear();
    }

    private static void writeLong(long value, WritableByteChannel channel, ByteBuffer elementBuffer) throws IOException {
        elementBuffer.position(0);
        elementBuffer.putLong(value);
        elementBuffer.position(0);
        channel.write(elementBuffer);
    }
}

