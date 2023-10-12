package com.jokerconf.gdomo.ignite;

import org.apache.ignite.cache.CacheEntryProcessor;

import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

public class QuantityUpdateEntryProcessor implements CacheEntryProcessor<Long, StockOrder, Void> {
    private final int newQuantity;

    public QuantityUpdateEntryProcessor(int newQuantity) {
        this.newQuantity = newQuantity;
    }

    @Override
    public Void process(MutableEntry<Long, StockOrder> mutableEntry, Object... objects) throws EntryProcessorException {
        StockOrder currentValue = mutableEntry.getValue();
        currentValue.setQuantity(newQuantity);
        mutableEntry.setValue(currentValue);
        return null;
    }
}
