package com.jokerconf.gdomo.ignite;

import java.util.List;

public class BenchmarkRunner {
    public static void main(String[] args) throws InterruptedException {

        final BenchmarkListIgniteExecutor benchmarkExecutor = new BenchmarkListIgniteExecutor();
        benchmarkExecutor.execute(args, List.of(
                        new Benchmark(StreamWriterType.PUT, 1, BinaryMode.NO_BINARY, false, 0, 0.0, 64, 40_000, true, 5),

                        // unique keys
                        new Benchmark(StreamWriterType.DS_DFLT_FULL_SYNC, 100000, BinaryMode.NO_BINARY, false, 0, 0, 64, 6_000_000, true, 5),

                        // allow overwrite, 0 backups
                        new Benchmark(StreamWriterType.DS_DFLT_FULL_SYNC, 100000, BinaryMode.NO_BINARY, false, 0, 0.01, 64, 6_000_000, true, 5),

                        // allow overwrite, 1 backup
                        new Benchmark(StreamWriterType.DS_DFLT_FULL_SYNC, 100000, BinaryMode.NO_BINARY, false, 1, 0.01, 64, 1_000_000, true, 5),

                        // allow overwrite, 1 backup, primary sync
                        new Benchmark(StreamWriterType.DS_DFLT_PR_SYNC, 100000, BinaryMode.NO_BINARY, false, 1, 0.01, 64, 6_000_000, true, 5),

                        //allow overwrite, 1 backup, batched receiver
                        new Benchmark(StreamWriterType.DS_BATCH, 100000, BinaryMode.NO_BINARY, false, 1, 0.01, 64, 6_000_000, true, 5)
                ),
                2);
//

    }
}
