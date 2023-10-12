package com.jokerconf.gdomo.ignite;

public record Benchmark(
        StreamWriterType streamWriterType,
        int batchSize,
        BinaryMode binaryMode,
        boolean sql,
        int backups,
        double keyCollision,
        int pojoBytes,
        int pojoCount,
        boolean clientNode,
        int servers
) {
}
