package ru.mail.polis.lsm.vladislav_fetisov;

import ru.mail.polis.lsm.DAO;
import ru.mail.polis.lsm.DAOConfig;
import ru.mail.polis.lsm.Record;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class LsmDAO implements DAO {
    private final AtomicInteger memoryConsumption = new AtomicInteger();
    private final List<SSTable> ssTables = new CopyOnWriteArrayList<>();
    private NavigableMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();
    private final DAOConfig config;

    public LsmDAO(DAOConfig config) {
        this.config = config;
        try {
            ssTables.addAll(SSTable.getAllSSTables(config.getDir()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        Iterator<Record> memRange = map(fromKey, toKey, storage);
        Iterator<Record> SSTablesRange = SSTablesRange(fromKey, toKey);
        PeekingIterator<Record> result = mergeTwo(new PeekingIterator<>(SSTablesRange), new PeekingIterator<>(memRange));
        return filteredResult(result);
    }

    private Iterator<Record> filteredResult(PeekingIterator<Record> result) {
        return new Iterator<>() {

            @Override
            public boolean hasNext() {
                if (!result.hasNext()) {
                    return false;
                }
                Record peek = result.peek();
                while (peek.isTombstone()) {
                    if (!result.hasNext()) {
                        return false;
                    }
                    result.next();
                    if (!result.hasNext()) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public Record next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return result.next();
            }
        };
    }

    private Iterator<Record> SSTablesRange(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        List<Iterator<Record>> ranges = new ArrayList<>();
        for (SSTable table : ssTables) {
            ranges.add(table.range(fromKey, toKey));
        }
        return merge(ranges);
    }

    @Override
    public void upsert(Record record) {
        int size = sizeOf(record);
        if (memoryConsumption.addAndGet(size) > DAOConfig.DEFAULT_MEMORY_LIMIT) {
            synchronized (this) {
                int prev = memoryConsumption.get();
                if (prev > DAOConfig.DEFAULT_MEMORY_LIMIT) {
                    memoryConsumption.set(size);
                    try {
                        flush();
                    } catch (IOException e) {
                        memoryConsumption.set(prev);
                        throw new UncheckedIOException(e);
                    }
                }
            }
        }
        storage.put(record.getKey(), record);
    }

    public static int sizeOf(Record record) {
        return 2 * Integer.BYTES + record.getKeySize() + record.getValueSize();
    }

    @Override
    public void closeAndCompact() {
        synchronized (this) {
            Iterator<Record> iterator = SSTablesRange(null, null);
            Path zeroTableName = config.getDir().resolve(String.valueOf(0));
            Path lastTableName = config.getDir().resolve(String.valueOf(ssTables.size()));
            try {
                SSTable bigSSTable = SSTable.write(iterator, lastTableName);
                if (!ssTables.isEmpty()) {
                    ssTables.get(0).close();
                }
                renameSSTable(bigSSTable, zeroTableName);
                for (int i = ssTables.size() - 1; i >= 1; i--) {
                    SSTable table = ssTables.get(i);
                    deleteSSTable(table);
                }
                ssTables.clear();
                ssTables.add(bigSSTable);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    private void deleteSSTable(SSTable table) throws IOException {
        table.close();
        Files.deleteIfExists(table.getOffsetsName());
        Files.deleteIfExists(table.getFileName());
    }

    private static void renameSSTable(SSTable table, Path tableDest) throws IOException {
        SSTable.rename(table.getOffsetsName(), SSTable.pathWithSuffix(tableDest, SSTable.SUFFIX_INDEX)); //there might be problem
        SSTable.rename(table.getFileName(), tableDest);
        table.setFileName(tableDest);
    }

    private void checkMemory() {
        long usedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        System.out.println("our memory " + memoryConsumption.get() / 1024L);
        System.out.println("memory use: " + usedMemory / 1024L);
        long free = Runtime.getRuntime().maxMemory() - usedMemory;
        System.out.println("free: " + free / 1024L);
        System.out.println("all " + (usedMemory + free) / 1024L);
    }

    @Override
    public void close() throws IOException {
        flush();
        for (SSTable ssTable : ssTables) {
            ssTable.close();
        }
    }

    public void flush() throws IOException {
        SSTable ssTable = writeSSTable(ssTables.size());
        ssTables.add(ssTable);
        storage = new ConcurrentSkipListMap<>();
    }

    private SSTable writeSSTable(int count) throws IOException {
        Path tablePath = config.getDir().resolve(String.valueOf(count));
        Iterator<Record> recordIterator = storage.values().iterator();
        return SSTable.write(recordIterator, tablePath);
    }

    public static Iterator<Record> map(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey, NavigableMap<ByteBuffer, Record> storage) {
        if (fromKey == null && toKey == null) {
            return storage.values().iterator();
        }
        return subMap(fromKey, toKey, storage).values().iterator();
    }

    public static SortedMap<ByteBuffer, Record> subMap(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey, NavigableMap<ByteBuffer, Record> storage) {
        if (fromKey == null) {
            return storage.headMap(toKey);
        }
        if (toKey == null) {
            return storage.tailMap(fromKey);
        }
        return storage.subMap(fromKey, toKey);
    }

    private static Iterator<Record> merge(List<Iterator<Record>> iterators) {
        switch (iterators.size()) {
            case 0:
                return Collections.emptyIterator();
            case 1:
                return iterators.get(0);
            case 2:
                return mergeTwo(new PeekingIterator<>(iterators.get(0)), new PeekingIterator<>(iterators.get(1)));
            default:
                return mergeList(iterators);
        }
    }

    private static Iterator<Record> mergeList(List<Iterator<Record>> iterators) {
        return iterators
                .stream()
                .map(PeekingIterator::new)
                .reduce(LsmDAO::mergeTwo)
                .orElse(new PeekingIterator<>(Collections.emptyIterator()));
    }

    private static PeekingIterator<Record> mergeTwo(PeekingIterator<Record> it1, PeekingIterator<Record> it2) {
        return new PeekingIterator<>(new Iterator<>() {

            @Override
            public boolean hasNext() {
                return it1.hasNext() || it2.hasNext();
            }

            @Override
            public Record next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                if (!it1.hasNext()) {
                    return it2.next();
                }
                if (!it2.hasNext()) {
                    return it1.next();
                }
                Record record1 = it1.peek();
                Record record2 = it2.peek();
                int compare = record1.getKey().compareTo(record2.getKey());
                if (compare < 0) {
                    it1.next();
                    return record1;
                } else if (compare == 0) {
                    it1.next();
                    it2.next();
                    return record2;
                } else {
                    it2.next();

                    return record2;
                }
            }
        });
    }


    private static class PeekingIterator<T> implements Iterator<T> {
        private final Iterator<T> iterator;
        private T current = null;

        public PeekingIterator(Iterator<T> iterator) {
            this.iterator = iterator;
        }


        public T peek() {
            if (current == null) {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                current = iterator.next();
                return current;
            }
            return current;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext() || current != null;
        }

        @Override
        public T next() {
            T res = peek();
            current = null;
            return res;
        }

    }
}
