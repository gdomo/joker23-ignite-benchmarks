package com.jokerconf.gdomo.ignite;

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheEntryProcessor;

import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

public class FieldUpdateEntryProcessor<K> implements CacheEntryProcessor<K, BinaryObject, Void> {
    private final String fieldName;
    private final Object fieldValue;

    public FieldUpdateEntryProcessor(String fieldName, Object fieldValue) {
        this.fieldName = fieldName;
        this.fieldValue = fieldValue;
    }

    @Override
    public Void process(MutableEntry<K, BinaryObject> mutableEntry, Object... objects) throws EntryProcessorException {
        BinaryObject currentValue = mutableEntry.getValue();
        BinaryObject updatedValue = currentValue.toBuilder()
                .setField(fieldName, fieldValue)
                .build();
        mutableEntry.setValue(updatedValue);
        return null;
    }
}
