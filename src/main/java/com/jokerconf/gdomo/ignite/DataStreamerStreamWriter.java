package com.jokerconf.gdomo.ignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerCacheUpdaters;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

@SuppressWarnings("DuplicatedCode")
public class DataStreamerStreamWriter implements StreamWriter {
    private final Ignite ignite;
    private final int batchSize;

    private final boolean forceBatchedStreamReceiver;
    private boolean allowOverwrite = false;

    public DataStreamerStreamWriter(Ignite ignite, int batchSize, boolean forceBatchedStreamReceiver) {
        this.ignite = ignite;
        this.batchSize = batchSize;
        this.forceBatchedStreamReceiver = forceBatchedStreamReceiver;
    }

    @Override
    public void allowOverwrite(boolean allow) {
        this.allowOverwrite = allow;
    }

    @Override
    public void write(List<Entry<Long, StockOrder>> data, IgniteCache<Long, StockOrder> cache) {
        try (IgniteDataStreamer<Long, StockOrder> dataStreamer = ignite.dataStreamer(cache.getName())) {
            writePrepared(data.stream(), dataStreamer);
        }
    }

    @Override
    public void prepareAndWriteBinary(List<Entry<Long, StockOrder>> data, IgniteCache<Long, BinaryObject> cache, BinaryObjectBuilder binaryBuilder) {
        try (IgniteDataStreamer<Long, BinaryObject> dataStreamer = ignite.dataStreamer(cache.getName())) {
            writePrepared(data.stream().map(e -> Map.entry(e.getKey(), e.getValue().toBinaryByFields(binaryBuilder))), dataStreamer);
        }
    }

    @Override
    public void prepareAndWriteBinary(List<Entry<Long, StockOrder>> data, IgniteCache<Long, BinaryObject> cache, IgniteBinary igniteBinary) {
        try (IgniteDataStreamer<Long, BinaryObject> dataStreamer = ignite.dataStreamer(cache.getName())) {
            writePrepared(data.stream().map(e -> Map.entry(e.getKey(), igniteBinary.toBinary(e.getValue()))), dataStreamer);
        }
    }

    @Override
    public void writeBinary(List<Entry<Long, BinaryObject>> data, IgniteCache<Long, BinaryObject> cache) {
        try (IgniteDataStreamer<Long, BinaryObject> dataStreamer = ignite.dataStreamer(cache.getName())) {
            writePrepared(data.stream(), dataStreamer);
        }
    }

    private <K, V> void writePrepared(Stream<Entry<K, V>> data, IgniteDataStreamer<K, V> dataStreamer) {
        dataStreamer.allowOverwrite(allowOverwrite);
        if (forceBatchedStreamReceiver) {
            dataStreamer.receiver(DataStreamerCacheUpdaters.batched());
        }

        final AtomicLong pojosUnflushed = new AtomicLong(0);
        data.forEach(entry -> {
            dataStreamer.addData(entry.getKey(), entry.getValue());
            if (pojosUnflushed.incrementAndGet() >= batchSize) {
                dataStreamer.flush();
                pojosUnflushed.set(0);
            }
        });
        if (pojosUnflushed.get() > 0) {
            dataStreamer.flush();
        }
    }
}
