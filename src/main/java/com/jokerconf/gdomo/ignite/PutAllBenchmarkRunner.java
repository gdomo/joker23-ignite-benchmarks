package com.jokerconf.gdomo.ignite;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class PutAllBenchmarkRunner {
    public static void main(String[] args) throws InterruptedException {
        rawBenchmark();

        final BenchmarkListIgniteExecutor benchmarkExecutor = new BenchmarkListIgniteExecutor();
        benchmarkExecutor.execute(args, List.of(
                // case 1, default
                new Benchmark(StreamWriterType.DS_DFLT_FULL_SYNC, 100_000, BinaryMode.NO_BINARY, false, 0, 0, 64, 5_000_000, true, 5),
                new Benchmark(StreamWriterType.PUT_ALL, 100_000, BinaryMode.NO_BINARY, false, 0, 0, 64, 5_000_000, true, 5),

                // case 2, low performance
                new Benchmark(StreamWriterType.DS_DFLT_FULL_SYNC, 10_000, BinaryMode.NO_BINARY, true, 2, 0, 500, 1_500_000, true, 3),
                new Benchmark(StreamWriterType.PUT_ALL, 10_000, BinaryMode.NO_BINARY, true, 2, 0, 500, 1_500_000, true, 3),

                // case 3, real life, medium performance
                new Benchmark(StreamWriterType.DS_BATCH, 100_000, BinaryMode.PREPARED_BINARY, true, 1, 0.01, 200, 5_000_000, true, 3),
                new Benchmark(StreamWriterType.PUT_ALL, 100_000, BinaryMode.PREPARED_BINARY, true, 1, 0.01, 200, 2_000_000, true, 3),

                // case 4, high performance
                new Benchmark(StreamWriterType.DS_DFLT_FULL_SYNC, 100_000, BinaryMode.PREPARED_BINARY, false, 0, 0, 64, 5_000_000, true, 5),
                new Benchmark(StreamWriterType.PUT_ALL, 100_000, BinaryMode.PREPARED_BINARY, false, 0, 0, 64, 5_000_000, true, 5),

                // unique keys, 1 backup
                new Benchmark(StreamWriterType.DS_DFLT_FULL_SYNC, 100_000, BinaryMode.NO_BINARY, false, 0, 0, 64, 4_000_000, true, 5),
                new Benchmark(StreamWriterType.PUT_ALL, 100_000, BinaryMode.NO_BINARY, false, 0, 0, 64, 4_000_000, true, 5),

                // 10%, 1 backup
                new Benchmark(StreamWriterType.DS_BATCH, 100_000, BinaryMode.NO_BINARY, false, 0, 0.1, 64, 4_000_000, true, 5),
                new Benchmark(StreamWriterType.PUT_ALL, 100_000, BinaryMode.NO_BINARY, false, 0, 0.1, 64, 4_000_000, true, 5),

                // 20%, 1 backup
                new Benchmark(StreamWriterType.DS_BATCH, 100_000, BinaryMode.NO_BINARY, false, 0, 0.2, 64, 4_000_000, true, 5),
                new Benchmark(StreamWriterType.PUT_ALL, 100_000, BinaryMode.NO_BINARY, false, 0, 0.2, 64, 4_000_000, true, 5),

                // 30%, 1 backup
                new Benchmark(StreamWriterType.DS_BATCH, 100_000, BinaryMode.NO_BINARY, false, 0, 0.3, 64, 4_000_000, true, 5),
                new Benchmark(StreamWriterType.PUT_ALL, 100_000, BinaryMode.NO_BINARY, false, 0, 0.3, 64, 4_000_000, true, 5),

                // 40%, 1 backup
                new Benchmark(StreamWriterType.DS_BATCH, 100_000, BinaryMode.NO_BINARY, false, 0, 0.4, 64, 4_000_000, true, 5),
                new Benchmark(StreamWriterType.PUT_ALL, 100_000, BinaryMode.NO_BINARY, false, 0, 0.4, 64, 4_000_000, true, 5),

                // 50%, 1 backup
                new Benchmark(StreamWriterType.DS_BATCH, 100_000, BinaryMode.NO_BINARY, false, 0, 0.5, 64, 4_000_000, true, 5),
                new Benchmark(StreamWriterType.PUT_ALL, 100_000, BinaryMode.NO_BINARY, false, 0, 0.5, 64, 5_000_000, true, 5),

                // 60%, 1 backup
                new Benchmark(StreamWriterType.DS_BATCH, 100_000, BinaryMode.NO_BINARY, false, 0, 0.6, 64, 4_000_000, true, 5),
                new Benchmark(StreamWriterType.PUT_ALL, 100_000, BinaryMode.NO_BINARY, false, 0, 0.6, 64, 6_000_000, true, 5),

                // 70%, 1 backup
                new Benchmark(StreamWriterType.DS_BATCH, 100_000, BinaryMode.NO_BINARY, false, 0, 0.7, 64, 4_000_000, true, 5),
                new Benchmark(StreamWriterType.PUT_ALL, 100_000, BinaryMode.NO_BINARY, false, 0, 0.7, 64, 10_000_000, true, 5),

                // 80%, 1 backup
                new Benchmark(StreamWriterType.DS_BATCH, 100_000, BinaryMode.NO_BINARY, false, 0, 0.8, 64, 4_000_000, true, 5),
                new Benchmark(StreamWriterType.PUT_ALL, 100_000, BinaryMode.NO_BINARY, false, 0, 0.8, 64, 15_000_000, true, 5),

                // 90%, 1 backup
                new Benchmark(StreamWriterType.DS_BATCH, 100_000, BinaryMode.NO_BINARY, false, 0, 0.9, 64, 4_000_000, true, 5),
                new Benchmark(StreamWriterType.PUT_ALL, 100_000, BinaryMode.NO_BINARY, false, 0, 0.9, 64, 30_000_000, true, 5),

                // 99%, 1 backup
                new Benchmark(StreamWriterType.DS_BATCH, 100_000, BinaryMode.NO_BINARY, false, 0, 0.99, 64, 4_000_000, true, 5),
                new Benchmark(StreamWriterType.PUT_ALL, 100_000, BinaryMode.NO_BINARY, false, 0, 0.99, 64, 60_000_000, true, 5),

                // 100%, 1 backup
                new Benchmark(StreamWriterType.DS_BATCH, 100_000, BinaryMode.NO_BINARY, false, 0, 1, 64, 500_000, true, 5),
                new Benchmark(StreamWriterType.PUT_ALL, 100_000, BinaryMode.NO_BINARY, false, 0, 1, 64, 100_000_000, true, 5)
        ), 2);

    }

    private static void rawBenchmark() {
        StockOrder stockOrder = new StockOrder();
        List<Map.Entry<Long, StockOrder>> entryList = Stream.iterate(0, i -> i + 1)
                .limit(100_000_000)
                .map(i -> Map.entry((long) i, stockOrder))
                .toList();

        long start = System.currentTimeMillis();
        HashMap<Long, StockOrder> batch = new HashMap<>();
        for (Map.Entry<Long, StockOrder> entry : entryList) {
            batch.put(entry.getKey(), entry.getValue());
            if (batch.size() == 100_000) {
                batch = new HashMap<>(200_000);
            }
        }
        long elapsed = System.currentTimeMillis() - start;
        System.out.println("Raw map creation took " + elapsed);
        System.out.println("wps: " + (1000L * entryList.size()) / elapsed);
    }
}
