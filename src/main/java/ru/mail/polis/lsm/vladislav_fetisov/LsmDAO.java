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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LsmDAO implements DAO {
    private final AtomicInteger memoryConsumption = new AtomicInteger();
    private final List<SSTable> ssTables = Collections.synchronizedList(new LinkedList<>());
    private final AtomicInteger ssTableNum = new AtomicInteger();
    private NavigableMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();
    private final DAOConfig config;
    private final ExecutorService service = Executors.newSingleThreadScheduledExecutor();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public LsmDAO(DAOConfig config) {
        this.config = config;
        ssTables.addAll(SSTable.getAllSSTables(config.getDir()));
        ssTableNum.set(ssTables.size());
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        lock.readLock().lock();
        try {
            Iterator<Record> memRange = map(fromKey, toKey, storage);
            Iterator<Record> SSTablesRange = SSTablesRange(fromKey, toKey, ssTables);
            PeekingIterator<Record> result = mergeTwo(new PeekingIterator<>(SSTablesRange), new PeekingIterator<>(memRange));
            return filteredResult(result);
        } finally {
            lock.readLock().unlock();
        }
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

    private Iterator<Record> SSTablesRange(@Nullable ByteBuffer fromKey,
                                           @Nullable ByteBuffer toKey,
                                           List<SSTable> ssTables) {
        List<Iterator<Record>> ranges = new ArrayList<>();
        for (SSTable table : ssTables) {
            if (table == SSTable.DUMMY) {
                continue;
            }
            ranges.add(table.range(fromKey, toKey));
        }
        return merge(ranges);
    }

    @Override
    public void upsert(Record record) {
        lock.readLock().lock();
        int size = sizeOf(record);
        if (memoryConsumption.addAndGet(size) > DAOConfig.DEFAULT_MEMORY_LIMIT) {
            synchronized (this) {
                int prev = memoryConsumption.get();
                if (prev > DAOConfig.DEFAULT_MEMORY_LIMIT) {
                    memoryConsumption.set(size);
                    lock.readLock().unlock();
                    service.execute(() -> {
                        lock.writeLock().lock();
                        try {
                            flush();
                        } catch (IOException e) {
                            memoryConsumption.set(prev);
                            throw new UncheckedIOException(e);
                        } finally {
                            storage.put(record.getKey(), record);
                            lock.writeLock().unlock();
                        }
                    });
                    return;
                }
            }
        }
        storage.put(record.getKey(), record);
        lock.readLock().unlock();
    }

    public static int sizeOf(Record record) {
        return 2 * Integer.BYTES + record.getKeySize() + record.getValueSize();
    }


    @Override
    public void compact() {
        List<SSTable> tablesForCompact;
        synchronized (ssTables) {
            ssTables.add(SSTable.DUMMY);
            ssTableNum.getAndIncrement();
            tablesForCompact = new ArrayList<>(ssTables);
        }
        int i = tablesForCompact.size() - 1;
        Path tableName = config.getDir().resolve(String.valueOf(i));
        Iterator<Record> iterator = SSTablesRange(null, null, tablesForCompact);

        lock.writeLock().lock();
        try {
            SSTable compacted = SSTable.write(iterator, tableName);
            if (i < ssTables.size() && ssTables.get(i) == SSTable.DUMMY) {
                ssTables.set(i, compacted);
                for (int j = 0; j < i; j++) {
                    ssTables.remove(0);
                }
            }
            deleteSSTables(tablesForCompact, i);//FIXME
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void deleteSSTables(List<SSTable> ssTables, int i) throws IOException {
        for (int j = 0; j < i; j++) {
            deleteSSTable(ssTables.get(j));
        }
    }

    private void deleteSSTable(SSTable table) throws IOException {
        Files.deleteIfExists(table.getFileName());
        Files.deleteIfExists(table.getOffsetsName());
    }


    @Override
    public void close() throws IOException {
        flush();
    }

    public void flush() throws IOException {
        SSTable ssTable = writeSSTable(ssTableNum.getAndIncrement());
        ssTables.add(ssTable);
        storage = new ConcurrentSkipListMap<>();
    }

    private SSTable writeSSTable(int count) throws IOException {
        Path tablePath = config.getDir().resolve(String.valueOf(count));
        Iterator<Record> recordIterator = storage.values().iterator();
        return SSTable.write(recordIterator, tablePath);
    }

    public static Iterator<Record> map(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer
            toKey, NavigableMap<ByteBuffer, Record> storage) {
        if (fromKey == null && toKey == null) {
            return storage.values().iterator();
        }
        return subMap(fromKey, toKey, storage).values().iterator();
    }

    public static SortedMap<ByteBuffer, Record> subMap(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer
            toKey, NavigableMap<ByteBuffer, Record> storage) {
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
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            T res = peek();
            current = null;
            return res;
        }

    }
}
