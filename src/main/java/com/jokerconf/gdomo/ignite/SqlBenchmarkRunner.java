package com.jokerconf.gdomo.ignite;

import java.util.List;

public class SqlBenchmarkRunner {
    public static void main(String[] args) throws InterruptedException {

        final BenchmarkListIgniteExecutor benchmarkExecutor = new BenchmarkListIgniteExecutor();
        for (Boolean sql : List.of(false, true)) {
            List<Benchmark> benchmarks = List.of(
                    // case 1, default
                    new Benchmark(StreamWriterType.PUT_ALL, 100_000, BinaryMode.NO_BINARY, sql, 0, 0, 64, 5_000_000, true, 5),

                    // case 2, low performance
                    new Benchmark(StreamWriterType.PUT_ALL, 10_000, BinaryMode.NO_BINARY, sql, 2, 0, 500, 1_500_000, true, 3),

                    // case 3, real life, medium performance
                    new Benchmark(StreamWriterType.DS_BATCH, 100_000, BinaryMode.PREPARED_BINARY, sql, 1, 0.01, 200, 5_000_000, true, 3),

                    // case 4, high performance
                    new Benchmark(StreamWriterType.DS_DFLT_FULL_SYNC, 100_000, BinaryMode.PREPARED_BINARY, sql, 0, 0, 64, 8_000_000, true, 5)
                    );
            benchmarkExecutor.execute(args, benchmarks, 3);
        }

    }
}
