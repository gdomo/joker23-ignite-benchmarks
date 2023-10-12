package com.jokerconf.gdomo.ignite;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObjectBuilder;

public class LocalBinaryApiBenchmark {

    public static void main(String[] args) {
        try (Ignite ignite = Ignition.start()) {
            IgniteBinary igniteBinary = ignite.binary();

            byFieldsWithNewBuilder(igniteBinary);
            byFieldsWithCachedBuilder(igniteBinary);
            byPojo(igniteBinary);
        }
    }

    private static void byFieldsWithNewBuilder(IgniteBinary igniteBinary) {
        StockOrder stockOrder = new StockOrder();
        int objectCount = 10_000_000;
        StopWatch stopWatch = StopWatch.createStarted();
        for (int i = 0; i < objectCount; i++) {
            BinaryObjectBuilder binaryBuilder = igniteBinary.builder(StockOrder.class.getName());
            stockOrder.toBinaryByFields(binaryBuilder);
        }
        stopWatch.stop();
        System.out.printf("Creating by fields and new builder, time: %s, object per sec: %d\n",
                stopWatch,
                (1000L * objectCount) / stopWatch.getTime());
    }

    private static void byFieldsWithCachedBuilder(IgniteBinary igniteBinary) {
        StockOrder stockOrder = new StockOrder();
        BinaryObjectBuilder binaryBuilder = igniteBinary.builder(StockOrder.class.getName());
        int objectCount = 10_000_000;
        StopWatch stopWatch = StopWatch.createStarted();
        for (int i = 0; i < objectCount; i++) {
            stockOrder.toBinaryByFields(binaryBuilder);
        }
        stopWatch.stop();
        System.out.printf("Creating by fields and cached builder, time: %s, object per sec: %d\n",
                stopWatch,
                (1000L * objectCount) / stopWatch.getTime());
    }

    private static void byPojo(IgniteBinary igniteBinary) {
        StockOrder stockOrder = new StockOrder();
        int objectCount = 10_000_000;
        StopWatch stopWatch = StopWatch.createStarted();
        for (int i = 0; i < objectCount; i++) {
            igniteBinary.toBinary(stockOrder);
        }
        stopWatch.stop();
        System.out.printf("Creating by pojo, time: %s, object per sec: %d\n",
                stopWatch,
                (1000L * objectCount) / stopWatch.getTime());
    }
}
