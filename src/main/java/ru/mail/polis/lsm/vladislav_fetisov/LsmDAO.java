package ru.mail.polis.lsm.vladislav_fetisov;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LsmDAO implements DAO {
    private final DAOConfig config;
    private final AtomicInteger memoryConsumption = new AtomicInteger();
    private volatile List<SSTable> ssTables = new ArrayList<>();
    private List<SSTable> duringCompactionTables = new ArrayList<>();
    private final AtomicInteger ssTableNum = new AtomicInteger();
    private final AtomicBoolean isCompacting = new AtomicBoolean();
    private volatile NavigableMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();
    private volatile NavigableMap<ByteBuffer, Record> readOnlyStorage = Collections.emptyNavigableMap();
    private final ExecutorService service = Executors.newFixedThreadPool(2);
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private static final Logger logger = LoggerFactory.getLogger(LsmDAO.class);
    private final Object compactionSynchronized = new Object();

    public LsmDAO(DAOConfig config) {
        this.config = config;
        ssTables.addAll(SSTable.getAllSSTables(config.getDir()));
        if (ssTables.isEmpty()) {
            ssTableNum.set(0);
            return;
        }
        int maxNum = Integer.parseInt(ssTables.get(ssTables.size() - 1).getFile().getFileName().toString());
        ssTableNum.set(maxNum + 1);
    }


    private Iterator<Record> SSTablesRange(@Nullable ByteBuffer fromKey,
                                           @Nullable ByteBuffer toKey,
                                           List<SSTable> ssTables) {
        List<Iterator<Record>> ranges = new ArrayList<>();
        for (SSTable table : ssTables) {
            ranges.add(table.range(fromKey, toKey));
        }
        return Iterators.merge(ranges);
    }

    @Override
    public Iterator<Record> range(@Nullable ByteBuffer fromKey, @Nullable ByteBuffer toKey) {
        Iterator<Record> memRange = map(fromKey, toKey, storage);
        Iterator<Record> readOnly = map(fromKey, toKey, readOnlyStorage);
        Iterator<Record> SSTablesRange = SSTablesRange(fromKey, toKey, ssTables);
        Iterators.PeekingIterator<Record> result = Iterators.mergeList(List.of(SSTablesRange, readOnly, memRange));
        return Iterators.filteredResult(result);
    }

    @Override
    public void upsert(Record record) {
        int size = sizeOf(record);
        if (memoryConsumption.addAndGet(size) > config.memoryLimit) {
            synchronized (this) {
                int prev = memoryConsumption.get();
                if (prev > config.memoryLimit) {
                    this.readOnlyStorage = this.storage;
                    this.storage = new ConcurrentSkipListMap<>();
                    logger.info("Starting flush memory: {}", memoryConsumption.get());
                    memoryConsumption.set(size);
                    try {
                        flush(readOnlyStorage);
                        this.readOnlyStorage = Collections.emptyNavigableMap();
                    } catch (IOException e) {
                        memoryConsumption.set(prev);
                        throw new UncheckedIOException(e);
                    }
                }
            }
        }
        storage.put(record.getKey(), record);
    }

    public void flush(NavigableMap<ByteBuffer, Record> storage) throws IOException {
        logger.info("start flush");
        int num = ssTableNum.getAndIncrement();
        SSTable ssTable = writeSSTable(num, storage);

        lock.writeLock().lock();
        try {
            atomicAdd(ssTable);
            if (isCompacting.get()) {
                duringCompactionTables.add(ssTable);
            }
        } finally {
            lock.writeLock().unlock();
        }

        if (ssTables.size() >= config.tableCount) {
            synchronized (compactionSynchronized) {
                if (ssTables.size() >= config.tableCount) {
                    compact();
                }
            }
        }
        logger.info("flush is finished");
    }

    @Override
    public void compact() {
        service.execute(() -> {
            try {
                synchronized (compactionSynchronized) {
                    compaction();
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

        });
    }

    private void compaction() throws IOException {
        logger.info("Starting compact tableCount: {}", ssTables.size());
        int num = ssTableNum.getAndIncrement();
        logger.info("compact table num:{}", num);
        duringCompactionTables = new ArrayList<>(); //because isCompacting false
        duringCompactionTables.add(SSTable.DUMMY);

        List<SSTable> fixed = this.ssTables;

        isCompacting.set(true);
        Iterator<Record> iterator = SSTablesRange(null, null, fixed);
        Path tableName = tableName(num);
        SSTable compacted = SSTable.write(iterator, tableName);

        lock.writeLock().lock();
        try {
            duringCompactionTables.set(0, compacted);
            this.ssTables = duringCompactionTables;
        } finally {
            isCompacting.set(false);
            lock.writeLock().unlock();
        }
        deleteDiscTables(fixed);
        logger.info("Compact is finished tableCount: {}", ssTables.size());
    }

    private Path tableName(int num) {
        return config.getDir().resolve(String.valueOf(num));
    }

    private void deleteDiscTables(List<SSTable> fixed) throws IOException {
        for (SSTable table : fixed) {
            Files.deleteIfExists(table.getFile());
            Files.deleteIfExists(table.getOffsets());
        }
    }

    private void atomicAdd(SSTable ssTable) {
        ArrayList<SSTable> newTables = new ArrayList<>(ssTables.size() + 1);
        newTables.addAll(ssTables);
        newTables.add(ssTable);
        this.ssTables = newTables;
    }

    @Override
    public void close() throws IOException {
        logger.info("Closing table");
        synchronized (this) {
            flush(storage);
        }
        logger.info("Table is closed");
        service.shutdown();
        try {
            if (!service.awaitTermination(10L, TimeUnit.MINUTES)) {
                throw new IllegalStateException("Cant await termination");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Cant await termination");
        }

    }

    private SSTable writeSSTable(int count, NavigableMap<ByteBuffer, Record> storage) throws IOException {
        Path tablePath = tableName(count);
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


    public static int sizeOf(Record record) {
        return 2 * Integer.BYTES + record.getKeySize() + record.getValueSize();
    }
}
