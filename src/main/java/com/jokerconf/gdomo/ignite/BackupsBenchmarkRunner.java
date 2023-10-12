package com.jokerconf.gdomo.ignite;

import java.util.ArrayList;
import java.util.List;

public class BackupsBenchmarkRunner {
    public static void main(String[] args) throws InterruptedException {

        final BenchmarkListIgniteExecutor benchmarkExecutor = new BenchmarkListIgniteExecutor();
        final List<List<BenchmarkResult>> allResults = new ArrayList<>();
        allResults.add(benchmarkExecutor.execute(
                args,
                List.of(
                        new Benchmark(StreamWriterType.PUT_ALL, 100_000, BinaryMode.NO_BINARY, false, 0, 0, 64, 5_000_000, true, 5),
                        new Benchmark(StreamWriterType.PUT_ALL, 100_000, BinaryMode.NO_BINARY, false, 1, 0, 64, 5_000_000, true, 5),
                        new Benchmark(StreamWriterType.PUT_ALL, 100_000, BinaryMode.NO_BINARY, false, 2, 0, 64, 5_000_000, true, 5),
                        new Benchmark(StreamWriterType.PUT_ALL, 100_000, BinaryMode.NO_BINARY, false, 3, 0, 64, 5_000_000, true, 5),
                        new Benchmark(StreamWriterType.PUT_ALL, 100_000, BinaryMode.NO_BINARY, false, 4, 0, 64, 5_000_000, true, 5)
                ),
                2)
        );
        allResults.add(benchmarkExecutor.execute(
                args,
                List.of(
                        new Benchmark(StreamWriterType.PUT_ALL, 10_000, BinaryMode.NO_BINARY, true, 0, 0, 500, 2_000_000, true, 5),
                        new Benchmark(StreamWriterType.PUT_ALL, 10_000, BinaryMode.NO_BINARY, true, 1, 0, 500, 2_000_000, true, 5),
                        new Benchmark(StreamWriterType.PUT_ALL, 10_000, BinaryMode.NO_BINARY, true, 2, 0, 500, 2_000_000, true, 5),
                        new Benchmark(StreamWriterType.PUT_ALL, 10_000, BinaryMode.NO_BINARY, true, 3, 0, 500, 2_000_000, true, 5),
                        new Benchmark(StreamWriterType.PUT_ALL, 10_000, BinaryMode.NO_BINARY, true, 4, 0, 500, 2_000_000, true, 5)
                ),
                2)
        );

        allResults.add(benchmarkExecutor.execute(
                args,
                List.of(
                        new Benchmark(StreamWriterType.DS_BATCH, 100_000, BinaryMode.PREPARED_BINARY, true, 0, 0.01, 200, 5_000_000, true, 5),
                        new Benchmark(StreamWriterType.DS_BATCH, 100_000, BinaryMode.PREPARED_BINARY, true, 1, 0.01, 200, 5_000_000, true, 5),
                        new Benchmark(StreamWriterType.DS_BATCH, 100_000, BinaryMode.PREPARED_BINARY, true, 2, 0.01, 200, 5_000_000, true, 5),
                        new Benchmark(StreamWriterType.DS_BATCH, 100_000, BinaryMode.PREPARED_BINARY, true, 3, 0.01, 200, 5_000_000, true, 5),
                        new Benchmark(StreamWriterType.DS_BATCH, 100_000, BinaryMode.PREPARED_BINARY, true, 4, 0.01, 200, 5_000_000, true, 5)
                ),
                2)
        );

        allResults.add(benchmarkExecutor.execute(
                args,
                List.of(
                        new Benchmark(StreamWriterType.DS_DFLT_FULL_SYNC, 100_000, BinaryMode.PREPARED_BINARY, false, 0, 0, 64, 5_000_000, true, 5),
                        new Benchmark(StreamWriterType.DS_DFLT_FULL_SYNC, 100_000, BinaryMode.PREPARED_BINARY, false, 1, 0, 64, 5_000_000, true, 5),
                        new Benchmark(StreamWriterType.DS_DFLT_FULL_SYNC, 100_000, BinaryMode.PREPARED_BINARY, false, 2, 0, 64, 5_000_000, true, 5),
                        new Benchmark(StreamWriterType.DS_DFLT_FULL_SYNC, 100_000, BinaryMode.PREPARED_BINARY, false, 3, 0, 64, 5_000_000, true, 5),
                        new Benchmark(StreamWriterType.DS_DFLT_FULL_SYNC, 100_000, BinaryMode.PREPARED_BINARY, false, 4, 0, 64, 5_000_000, true, 5)
                ),
                2)
        );

        System.out.println("All results:");
        allResults.forEach(benchmarkExecutor::printResults);
    }
}
