package ru.mail.polis.lsm.vladislav_fetisov;

import ru.mail.polis.lsm.Record;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class Storage {
    public final NavigableMap<ByteBuffer, Record> memTable;
    public final NavigableMap<ByteBuffer, Record> readOnlyMemTable;
    public final List<SSTable> ssTables;

    public Storage(NavigableMap<ByteBuffer, Record> memTable,
                   NavigableMap<ByteBuffer, Record> readOnlyMemTable,
                   List<SSTable> ssTables) {
        this.memTable = memTable;
        this.readOnlyMemTable = readOnlyMemTable;
        this.ssTables = ssTables;
    }

    public Storage(List<SSTable> ssTables) {
        this.memTable = new ConcurrentSkipListMap<>();
        this.readOnlyMemTable = getEmptyMap();
        this.ssTables = ssTables;
    }

    public Storage beforeFlush() {
        return new Storage(new ConcurrentSkipListMap<>(), this.memTable, this.ssTables);
    }

    public Storage afterFlush() {
        return new Storage(this.memTable, getEmptyMap(), this.ssTables);
    }

    public Storage afterCompact(List<SSTable> duringCompactTables) {
        return new Storage(this.memTable, this.readOnlyMemTable, duringCompactTables);
    }

    private NavigableMap<ByteBuffer, Record> getEmptyMap() {
        return Collections.emptyNavigableMap();
    }

    public Storage add(SSTable ssTable) {
        ArrayList<SSTable> newTables = new ArrayList<>(ssTables.size() + 1);
        newTables.addAll(ssTables);
        newTables.add(ssTable);
        return new Storage(this.memTable, this.readOnlyMemTable, newTables);
    }
}
