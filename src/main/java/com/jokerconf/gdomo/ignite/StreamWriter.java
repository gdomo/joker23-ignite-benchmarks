package com.jokerconf.gdomo.ignite;

import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;

import java.util.List;
import java.util.Map.Entry;

public interface StreamWriter {
    void write(List<Entry<Long, StockOrder>> data, IgniteCache<Long, StockOrder> cache);

    void prepareAndWriteBinary(List<Entry<Long, StockOrder>> data, IgniteCache<Long, BinaryObject> cache, BinaryObjectBuilder binaryBuilder);
    void prepareAndWriteBinary(List<Entry<Long, StockOrder>> data, IgniteCache<Long, BinaryObject> cache, IgniteBinary igniteBinary);

    void writeBinary(List<Entry<Long, BinaryObject>> data, IgniteCache<Long, BinaryObject> cache);

    default void allowOverwrite(boolean allow) {
    }
}
