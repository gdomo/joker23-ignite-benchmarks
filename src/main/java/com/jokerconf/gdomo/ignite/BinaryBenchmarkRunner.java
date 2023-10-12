package com.jokerconf.gdomo.ignite;

import java.util.ArrayList;
import java.util.List;

public class BinaryBenchmarkRunner {
    public static void main(String[] args) throws InterruptedException {

        final BenchmarkListIgniteExecutor benchmarkExecutor = new BenchmarkListIgniteExecutor();
        final List<List<BenchmarkResult>> allResults = new ArrayList<>();
        for (BinaryMode binaryMode : List.of(BinaryMode.NO_BINARY, BinaryMode.CREATE_BY_FIELDS, BinaryMode.CREATE_BY_POJO, BinaryMode.PREPARED_BINARY)) {
            List<Benchmark> benchmarks = List.of(
                    new Benchmark(StreamWriterType.PUT_ALL, 100_000, binaryMode, false, 0, 0, 64, 5_000_000, true, 5),

                    // case 2, low performance
                    new Benchmark(StreamWriterType.PUT_ALL, 10_000, binaryMode, true, 2, 0, 500, 1_500_000, true, 3),

                    // case 3, real life, medium performance
                    new Benchmark(StreamWriterType.DS_BATCH, 100_000, binaryMode, true, 1, 0.01, 200, 5_000_000, true, 3),

                    // case 4, high performance
                    new Benchmark(StreamWriterType.DS_DFLT_FULL_SYNC, 100_000, binaryMode, false, 0, 0, 64, 7_000_000, true, 5)
            );
            allResults.add(benchmarkExecutor.execute(args, benchmarks, 2));
        }

        System.out.println("All results:");
        allResults.forEach(benchmarkExecutor::printResults);
    }
}
