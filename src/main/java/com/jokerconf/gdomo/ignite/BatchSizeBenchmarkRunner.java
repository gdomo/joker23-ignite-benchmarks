package com.jokerconf.gdomo.ignite;

import java.util.ArrayList;
import java.util.List;

public class BatchSizeBenchmarkRunner {
    public static void main(String[] args) throws InterruptedException {
        final BenchmarkListIgniteExecutor benchmarkExecutor = new BenchmarkListIgniteExecutor();
        final List<List<BenchmarkResult>> allResults = new ArrayList<>();
        allResults.add(benchmarkExecutor.execute(
                args,
                List.of(
                        new Benchmark(StreamWriterType.PUT_ALL, 100, BinaryMode.NO_BINARY, false, 0, 0, 64, 5_000_000, true, 5),
                        new Benchmark(StreamWriterType.PUT_ALL, 1_000, BinaryMode.NO_BINARY, false, 0, 0, 64, 5_000_000, true, 5),
                        new Benchmark(StreamWriterType.PUT_ALL, 10_000, BinaryMode.NO_BINARY, false, 0, 0, 64, 5_000_000, true, 5),
                        new Benchmark(StreamWriterType.PUT_ALL, 100_000, BinaryMode.NO_BINARY, false, 0, 0, 64, 5_000_000, true, 5),
                        new Benchmark(StreamWriterType.PUT_ALL, 1000_000, BinaryMode.NO_BINARY, false, 0, 0, 64, 5_000_000, true, 5)
                ),
                2
        ));

        allResults.add(benchmarkExecutor.execute(
                args,
                List.of(
                        new Benchmark(StreamWriterType.PUT_ALL, 100, BinaryMode.NO_BINARY, true, 2, 0, 500, 1_500_000, true, 3),
                        new Benchmark(StreamWriterType.PUT_ALL, 1_000, BinaryMode.NO_BINARY, true, 2, 0, 500, 1_500_000, true, 3),
                        new Benchmark(StreamWriterType.PUT_ALL, 10_000, BinaryMode.NO_BINARY, true, 2, 0, 500, 1_500_000, true, 3),
                        new Benchmark(StreamWriterType.PUT_ALL, 100_000, BinaryMode.NO_BINARY, true, 2, 0, 500, 1_500_000, true, 3),
                        new Benchmark(StreamWriterType.PUT_ALL, 1000_000, BinaryMode.NO_BINARY, true, 2, 0, 500, 1_500_000, true, 3)
                        ),
                2
        ));

        allResults.add(benchmarkExecutor.execute(
                args,
                List.of(
                        new Benchmark(StreamWriterType.DS_BATCH, 100, BinaryMode.PREPARED_BINARY, true, 1, 0.01, 200, 1_000_000, true, 3),
                        new Benchmark(StreamWriterType.DS_BATCH, 1_000, BinaryMode.PREPARED_BINARY, true, 1, 0.01, 200, 5_000_000, true, 3),
                        new Benchmark(StreamWriterType.DS_BATCH, 10_000, BinaryMode.PREPARED_BINARY, true, 1, 0.01, 200, 5_000_000, true, 3),
                        new Benchmark(StreamWriterType.DS_BATCH, 100_000, BinaryMode.PREPARED_BINARY, true, 1, 0.01, 200, 5_000_000, true, 3),
                        new Benchmark(StreamWriterType.DS_BATCH, 1000_000, BinaryMode.PREPARED_BINARY, true, 1, 0.01, 200, 5_000_000, true, 3)
                ),
                2
        ));

        allResults.add(benchmarkExecutor.execute(
                args,
                List.of(
                        new Benchmark(StreamWriterType.DS_DFLT_FULL_SYNC, 100, BinaryMode.PREPARED_BINARY, false, 0, 0, 64, 5_000_000, true, 5),
                        new Benchmark(StreamWriterType.DS_DFLT_FULL_SYNC, 1_000, BinaryMode.PREPARED_BINARY, false, 0, 0, 64, 5_000_000, true, 5),
                        new Benchmark(StreamWriterType.DS_DFLT_FULL_SYNC, 10_000, BinaryMode.PREPARED_BINARY, false, 0, 0, 64, 5_000_000, true, 5),
                        new Benchmark(StreamWriterType.DS_DFLT_FULL_SYNC, 100_000, BinaryMode.PREPARED_BINARY, false, 0, 0, 64, 5_000_000, true, 5),
                        new Benchmark(StreamWriterType.DS_DFLT_FULL_SYNC, 1000_000, BinaryMode.PREPARED_BINARY, false, 0, 0, 64, 5_000_000, true, 5)
                ),
                2
        ));

        System.out.println("All results:");
        allResults.forEach(benchmarkExecutor::printResults);

    }
}
