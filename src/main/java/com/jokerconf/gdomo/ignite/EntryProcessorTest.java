package com.jokerconf.gdomo.ignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;

import java.util.Map;

public class EntryProcessorTest {
    public static void main(String[] args) {
        try (Ignite ignite = Ignition.start()) {
            IgniteCache<Long, StockOrder> cache = ignite.getOrCreateCache("test");
            cache.put(1L, new StockOrder());
            cache.invokeAll(Map.of(1L, new QuantityUpdateEntryProcessor(1)));
            System.out.println(cache.get(1L).getQuantity());
            cache.<Long, BinaryObject>withKeepBinary().invoke(1L, new FieldUpdateEntryProcessor<>("quantity", 2));
            System.out.println(cache.get(1L).getQuantity());
        }
    }
}
