package com.jokerconf.gdomo.ignite;

import java.util.List;

public class BinaryJfrBenchmarkRunner {
    // run with -XX:StartFlightRecording:filename=binary.jfr
    public static void main(String[] args) throws InterruptedException {

        final BenchmarkListIgniteExecutor benchmarkExecutor = new BenchmarkListIgniteExecutor();
        noBinary(args, benchmarkExecutor);
        createBinary(args, benchmarkExecutor);
        preparedBinary(args, benchmarkExecutor);
    }

    private static void noBinary(String[] args, BenchmarkListIgniteExecutor benchmarkExecutor) throws InterruptedException {
        benchmarkExecutor.execute(
                args,
                List.of(
                        new Benchmark(StreamWriterType.PUT_ALL, 1_000, BinaryMode.NO_BINARY, false, 0, 0.01, 500, 3_000_000, true, 5)
                ),
                2
        );
    }

    private static void createBinary(String[] args, BenchmarkListIgniteExecutor benchmarkExecutor) throws InterruptedException {
        benchmarkExecutor.execute(
                args,
                List.of(
                        new Benchmark(StreamWriterType.PUT_ALL, 1_000, BinaryMode.CREATE_BY_FIELDS, false, 0, 0.01, 500, 3_000_000, true, 5)
                ),
                2
        );
    }

    private static void preparedBinary(String[] args, BenchmarkListIgniteExecutor benchmarkExecutor) throws InterruptedException {
        benchmarkExecutor.execute(
                args,
                List.of(
                        new Benchmark(StreamWriterType.PUT_ALL, 1_000, BinaryMode.PREPARED_BINARY, false, 0, 0.01, 500, 3_000_000, true, 5)
                ),
                2
        );
    }
}
