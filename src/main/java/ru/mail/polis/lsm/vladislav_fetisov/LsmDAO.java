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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class LsmDAO implements DAO {
    private static final int EXCLUSIVE_PERMIT = 1;

    private final DAOConfig config;
    private final AtomicInteger memoryConsumption = new AtomicInteger();
    private final AtomicInteger tablesCount = new AtomicInteger();
    private volatile List<SSTable> ssTables = new ArrayList<>();
    private List<SSTable> duringCompactionTables = new ArrayList<>();
    private final AtomicInteger ssTableNum = new AtomicInteger();
    private final AtomicBoolean isCompacting = new AtomicBoolean();
    private volatile NavigableMap<ByteBuffer, Record> storage = new ConcurrentSkipListMap<>();
    private volatile NavigableMap<ByteBuffer, Record> readOnlyStorage = Collections.emptyNavigableMap();
    private final ExecutorService service = Executors.newFixedThreadPool(2);
    private static final Logger logger = LoggerFactory.getLogger(LsmDAO.class);

    private final Semaphore compactSemaphore = new Semaphore(EXCLUSIVE_PERMIT);
    private final Semaphore flushSemaphore = new Semaphore(EXCLUSIVE_PERMIT);

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


    private Iterator<Record> ssTablesRange(@Nullable ByteBuffer fromKey,
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
        Iterator<Record> ssTablesRange = ssTablesRange(fromKey, toKey, ssTables);
        Iterators.PeekingIterator<Record> result = Iterators.mergeList(List.of(ssTablesRange, readOnly, memRange));
        return Iterators.filteredResult(result);
    }

    @Override
    public void upsert(Record record) {
        int size = sizeOf(record);
        if (memoryConsumption.addAndGet(size) > config.memoryLimit) {
            try {
                flushSemaphore.acquire();
            } catch (InterruptedException e) {
                logger.error("flushSemaphore was interrupted");
                Thread.currentThread().interrupt();
            }
            int prev = memoryConsumption.get();
            if (prev > config.memoryLimit) {
                this.readOnlyStorage = this.storage;
                this.storage = new ConcurrentSkipListMap<>();
                logger.info("Starting flush memory: {} kb", memoryConsumption.get() / 1024L);
                memoryConsumption.set(size);
                service.execute(() -> {
                    try {
                        flush(readOnlyStorage, true);
                        this.readOnlyStorage = Collections.emptyNavigableMap();
                    } catch (IOException e) {
                        memoryConsumption.set(prev);
                        throw new UncheckedIOException(e);
                    } finally {
                        flushSemaphore.release();
                    }
                });
            }
        }
        storage.put(record.getKey(), record);
    }


    public void flush(NavigableMap<ByteBuffer, Record> storage, boolean needCompact) throws IOException {
        logger.info("start flush");
        int num = ssTableNum.getAndIncrement();
        logger.info("flush table num:{}", num);
        SSTable ssTable = writeSSTable(num, storage);

        synchronized (isCompacting) {
            atomicAdd(ssTable); //atomic need for reading
            if (isCompacting.get()) {
                duringCompactionTables.add(ssTable);
            }
            tablesCount.incrementAndGet();
        }
        logger.info("flush is finished");

        if (tablesCount.get() >= config.tableCount && needCompact) {
            try {
                compactSemaphore.acquire();
            } catch (InterruptedException e) {
                logger.error("compactSemaphore was interrupted");
                Thread.currentThread().interrupt();
            }
            boolean b = tablesCount.get() >= config.tableCount;
            if (!b) {
                compactSemaphore.release();
                return;
            }
            compact();
        }
    }

    @Override
    public void compact() {
        service.execute(() -> {
            try {
                compaction();
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
        tablesCount.set(1);

        isCompacting.set(true);
        List<SSTable> fixed = this.ssTables;

        Iterator<Record> iterator = ssTablesRange(null, null, fixed);
        Path tableName = tableName(num);
        SSTable compacted = SSTable.write(iterator, tableName);

        synchronized (isCompacting) {
            duringCompactionTables.set(0, compacted);
            this.ssTables = duringCompactionTables;
            isCompacting.set(false);
        }
        compactSemaphore.release();
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
        service.shutdown();
        try {
            if (!service.awaitTermination(10L, TimeUnit.MINUTES)) {
                throw new IllegalStateException("Cant await termination");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Cant await termination");
        }
        logger.info("Closing table");
        synchronized (this) {
            flush(storage, false);
        }
        logger.info("Table is closed");
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
