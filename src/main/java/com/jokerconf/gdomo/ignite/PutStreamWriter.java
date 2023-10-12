package com.jokerconf.gdomo.ignite;

import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;

import java.util.List;
import java.util.Map.Entry;

public class PutStreamWriter implements StreamWriter {
    @Override
    public void write(List<Entry<Long, StockOrder>> data, IgniteCache<Long, StockOrder> cache) {
        data.forEach(entry -> cache.put(entry.getKey(), entry.getValue()));
    }

    @Override
    public void prepareAndWriteBinary(List<Entry<Long, StockOrder>> data, IgniteCache<Long, BinaryObject> cache, BinaryObjectBuilder binaryBuilder) {
        data.forEach(entry -> cache.put(entry.getKey(), entry.getValue().toBinaryByFields(binaryBuilder)));
    }

    @Override
    public void prepareAndWriteBinary(List<Entry<Long, StockOrder>> data, IgniteCache<Long, BinaryObject> cache, IgniteBinary igniteBinary) {
        data.forEach(entry -> cache.put(entry.getKey(), igniteBinary.toBinary(entry.getValue())));
    }

    @Override
    public void writeBinary(List<Entry<Long, BinaryObject>> data, IgniteCache<Long, BinaryObject> cache) {
        data.forEach(entry -> cache.put(entry.getKey(), entry.getValue()));
    }
}
