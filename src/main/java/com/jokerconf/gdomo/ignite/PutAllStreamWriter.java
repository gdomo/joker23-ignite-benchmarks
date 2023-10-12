package com.jokerconf.gdomo.ignite;

import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

@SuppressWarnings("DuplicatedCode")
public class PutAllStreamWriter implements StreamWriter {
    private final int batchSize;

    public PutAllStreamWriter(int batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public void write(List<Entry<Long, StockOrder>> data, IgniteCache<Long, StockOrder> cache) {
        Map<Long, StockOrder> batch = new HashMap<>(batchSize * 2);
        int pojoCounter = 0;

        for (Entry<Long, StockOrder> entry : data) {
            batch.put(entry.getKey(), entry.getValue());
            if (++pojoCounter >= batchSize) {
                cache.putAll(batch);
                batch = new HashMap<>(batchSize * 2);
                pojoCounter = 0;
            }
        }

        cache.putAll(batch);
    }

    @Override
    public void prepareAndWriteBinary(List<Entry<Long, StockOrder>> data, IgniteCache<Long, BinaryObject> cache, BinaryObjectBuilder binaryBuilder) {
        Map<Long, BinaryObject> batch = new HashMap<>();
        int pojoCounter = 0;

        for (Entry<Long, StockOrder> entry : data) {
            BinaryObject value = entry.getValue().toBinaryByFields(binaryBuilder);
            batch.put(entry.getKey(), value);
            if (++pojoCounter >= batchSize) {
                cache.putAll(batch);
                batch = new HashMap<>();
                pojoCounter = 0;
            }
        }

        cache.putAll(batch);
    }

    @Override
    public void prepareAndWriteBinary(List<Entry<Long, StockOrder>> data, IgniteCache<Long, BinaryObject> cache, IgniteBinary igniteBinary) {
        Map<Long, BinaryObject> batch = new HashMap<>();
        int pojoCounter = 0;

        for (Entry<Long, StockOrder> entry : data) {
            BinaryObject value = igniteBinary.toBinary(entry.getValue());
            batch.put(entry.getKey(), value);
            if (++pojoCounter >= batchSize) {
                cache.putAll(batch);
                batch = new HashMap<>();
                pojoCounter = 0;
            }
        }

        cache.putAll(batch);
    }

    @Override
    public void writeBinary(List<Entry<Long, BinaryObject>> data, IgniteCache<Long, BinaryObject> cache) {
        Map<Long, BinaryObject> batch = new HashMap<>();
        int pojoCounter = 0;

        for (Entry<Long, BinaryObject> entry : data) {
            batch.put(entry.getKey(), entry.getValue());
            if (++pojoCounter >= batchSize) {
                cache.putAll(batch);
                batch = new HashMap<>();
                pojoCounter = 0;
            }
        }

        cache.putAll(batch);
    }
}
