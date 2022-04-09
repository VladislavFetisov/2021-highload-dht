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
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class LsmDAO implements DAO {
    private static final int EXCLUSIVE_PERMIT = 1;
    public static final int THREADS_COUNT = 2; //1 for compact, 1 for flush
    private final DAOConfig config;
    private final AtomicInteger memoryConsumption = new AtomicInteger();
    private final AtomicInteger tablesCount = new AtomicInteger();
    private final AtomicInteger ssTableNum;
    private final AtomicBoolean isCompacting = new AtomicBoolean();
    private MemTable memTable;

    private List<SSTable> duringCompactionTables = new ArrayList<>();

    private final ExecutorService service = Executors.newFixedThreadPool(THREADS_COUNT);

    private final Semaphore compactSemaphore = new Semaphore(EXCLUSIVE_PERMIT);
    private final Semaphore flushSemaphore = new Semaphore(EXCLUSIVE_PERMIT);

    public static final Logger logger = LoggerFactory.getLogger(LsmDAO.class);

    public LsmDAO(DAOConfig config) {
        this.config = config;
        List<SSTable> discTables = SSTable.getAllSSTables(config.getDir());
        memTable = new MemTable(discTables);
        if (discTables.isEmpty()) {
            ssTableNum = new AtomicInteger();
            return;
        }
        SSTable lastTable = discTables.get(discTables.size() - 1);
        int maxNum = Integer.parseInt(lastTable.getFile().getFileName().toString());
        ssTableNum = new AtomicInteger(maxNum + 1);
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
        Iterator<Record> memRange = map(fromKey, toKey, memTable.storage);
        Iterator<Record> readOnly = map(fromKey, toKey, memTable.readOnlyStorage);
        Iterator<Record> ssTablesRange = ssTablesRange(fromKey, toKey, memTable.ssTables);
        Iterators.PeekingIterator<Record> result = Iterators.mergeList(List.of(ssTablesRange, readOnly, memRange));
        return Iterators.filteredResult(result);
    }

    @Override
    public boolean upsert(Record record) {
        int size = Utils.sizeOf(record);
        if (memoryConsumption.addAndGet(size) > config.memoryLimit) {
            try {
                flushSemaphore.acquire();
                int prev = memoryConsumption.get();
                if (prev > config.memoryLimit) {
                    performFlush(size, prev);
                } else {
                    flushSemaphore.release();
                }
            } catch (InterruptedException e) {
                logger.error("flushSemaphore was interrupted");
                Thread.currentThread().interrupt();
            }
        }
        memTable.storage.put(record.getKey(), record);
        return true;
    }

    private void performFlush(int size, int prev) {
        memTable = memTable.beforeFlush();
        logger.info("Starting flush memory: {} kb", memoryConsumption.get() / 1024L);
        memoryConsumption.set(size);
        service.execute(() -> {
            try {
                flush(memTable.readOnlyStorage, true);
                memTable = memTable.afterFlush();
            } catch (IOException e) {
                memoryConsumption.set(prev);
                throw new UncheckedIOException(e);
            } finally {
                flushSemaphore.release();
            }
        });
    }


    @Override
    public void compact() {
        service.execute(() -> {
            try {
                synchronized (this) {
                    compaction(); //only one compaction per time
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }


    public void flush(NavigableMap<ByteBuffer, Record> storage, boolean needCompact) throws IOException {
        logger.info("start flush");
        int num = ssTableNum.getAndIncrement();
        logger.info("flush table num:{}", num);
        SSTable ssTable = writeSSTable(num, storage);

        synchronized (isCompacting) {
            memTable = memTable.add(ssTable); //atomic need for reading
            if (isCompacting.get()) {
                duringCompactionTables.add(ssTable); //we don't need there atomic, because only 1 flush per time.
            }
        }
        tablesCount.incrementAndGet();
        logger.info("flush is finished");

        if (tablesCount.get() >= config.tableCount && needCompact) {
            try {
                compactSemaphore.acquire();
                boolean b = tablesCount.get() >= config.tableCount;
                if (!b) {
                    compactSemaphore.release();
                    return;
                }
                compact();
            } catch (InterruptedException e) {
                logger.error("compactSemaphore was interrupted");
                Thread.currentThread().interrupt();
            }
        }
    }


    private void compaction() throws IOException {
        logger.info("Starting compact tableCount: {}", memTable.ssTables.size());
        int num = ssTableNum.getAndIncrement();
        logger.info("compact table num:{}", num);

        duringCompactionTables = new ArrayList<>(); //because isCompacting false
        duringCompactionTables.add(SSTable.DUMMY);
        tablesCount.set(1); //same promise as in flush
        isCompacting.set(true);

        List<SSTable> fixed = memTable.ssTables;

        Iterator<Record> iterator = ssTablesRange(null, null, fixed);
        Path tableName = tableName(num);
        SSTable compacted = SSTable.write(iterator, tableName);

        synchronized (isCompacting) {
            duringCompactionTables.set(0, compacted);
            memTable = memTable.afterCompact(duringCompactionTables);
            isCompacting.set(false);
        }
        compactSemaphore.release();
        Utils.deleteDiscTables(fixed);
        logger.info("Compact is finished tableCount: {}", memTable.ssTables.size());
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
            flush(memTable.storage, false);
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


    private Path tableName(int num) {
        return config.getDir().resolve(String.valueOf(num));
    }
}
