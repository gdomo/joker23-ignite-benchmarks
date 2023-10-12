package com.jokerconf.gdomo.ignite;

import java.util.ArrayList;
import java.util.List;

public class ServerBenchmarkRunner {
    public static void main(String[] args) throws InterruptedException {

        final BenchmarkListIgniteExecutor benchmarkExecutor = new BenchmarkListIgniteExecutor();
        final List<List<BenchmarkResult>> allResults = new ArrayList<>();
        List<Benchmark> benchmarkList = new ArrayList<>();
        for (int servers = 6; servers >= 1; servers--) {
            benchmarkList.add(new Benchmark(StreamWriterType.PUT_ALL, 100_000, BinaryMode.NO_BINARY, false, 0, 0, 64, 5_000_000, true, servers));
        }
        allResults.add(benchmarkExecutor.execute(args, benchmarkList, 2));
        benchmarkList.clear();
        for (int servers = 6; servers >= 2; servers--) {
            benchmarkList.add(new Benchmark(StreamWriterType.PUT_ALL, 10_000, BinaryMode.NO_BINARY, true, 2, 0, 500, 1_500_000, true, servers));
        }
        allResults.add(benchmarkExecutor.execute(args, benchmarkList, 2));
        benchmarkList.clear();
        for (int servers = 6; servers >= 2; servers--) {
            benchmarkList.add(new Benchmark(StreamWriterType.DS_BATCH, 100_000, BinaryMode.PREPARED_BINARY, true, 1, 0.01, 200, 5_000_000, true, servers));
        }
        allResults.add(benchmarkExecutor.execute(args, benchmarkList, 2));
        benchmarkList.clear();
        for (int servers = 6; servers >= 1; servers--) {
            benchmarkList.add(new Benchmark(StreamWriterType.DS_DFLT_FULL_SYNC, 100_000, BinaryMode.PREPARED_BINARY, false, 0, 0, 64, 8_000_000, true, servers));
        }
        allResults.add(benchmarkExecutor.execute(args, benchmarkList, 2));

        System.out.println("All results:");
        allResults.forEach(benchmarkExecutor::printResults);
    }
}
