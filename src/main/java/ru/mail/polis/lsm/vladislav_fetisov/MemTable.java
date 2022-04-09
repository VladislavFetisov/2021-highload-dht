package ru.mail.polis.lsm.vladislav_fetisov;

import ru.mail.polis.lsm.Record;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemTable {
    public final NavigableMap<ByteBuffer, Record> storage;
    public final NavigableMap<ByteBuffer, Record> readOnlyStorage;
    public final List<SSTable> ssTables;

    public MemTable(NavigableMap<ByteBuffer, Record> storage,
                    NavigableMap<ByteBuffer, Record> readOnlyStorage,
                    List<SSTable> ssTables) {
        this.storage = storage;
        this.readOnlyStorage = readOnlyStorage;
        this.ssTables = ssTables;
    }

    public MemTable(List<SSTable> ssTables) {
        this.storage = new ConcurrentSkipListMap<>();
        this.readOnlyStorage = getEmptyMap();
        this.ssTables = ssTables;
    }

    public MemTable beforeFlush() {
        return new MemTable(new ConcurrentSkipListMap<>(), this.storage, this.ssTables);
    }

    public MemTable afterFlush() {
        return new MemTable(this.storage, getEmptyMap(), this.ssTables);
    }

    public MemTable afterCompact(List<SSTable> duringCompactTables) {
        return new MemTable(this.storage, this.readOnlyStorage, duringCompactTables);
    }

    private NavigableMap<ByteBuffer, Record> getEmptyMap() {
        return Collections.emptyNavigableMap();
    }

    public MemTable add(SSTable ssTable) {
        ArrayList<SSTable> newTables = new ArrayList<>(ssTables.size() + 1);
        newTables.addAll(ssTables);
        newTables.add(ssTable);
        return new MemTable(this.storage, this.readOnlyStorage, newTables);
    }
}
