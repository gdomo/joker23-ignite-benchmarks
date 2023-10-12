package com.jokerconf.gdomo.ignite;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.PRIMARY_SYNC;

public class BenchmarkListIgniteExecutor {
    private record PreparedBenchmark(
            BinaryMode binaryMode,
            StreamWriter streamWriter,
            List<Map.Entry<Long, StockOrder>> pojos,
            List<Map.Entry<Long, BinaryObject>> binaryPojos
    ) {
    }

    public BenchmarkListIgniteExecutor() {
        GcLogger.startLoggingGc();
    }

    public List<BenchmarkResult> execute(String[] serverList, List<Benchmark> benchmarks, int repeats) throws InterruptedException {
        final List<BenchmarkResult> benchmarkResults = new ArrayList<>();
        Ignite ignite = null;
        try {
            for (Benchmark benchmark : benchmarks) {
                if (ignite == null || ignite.configuration().isClientMode() != benchmark.clientNode()) {
                    if (ignite != null) {
                        ignite.close();
                    }
                    ignite = getIgnite(serverList, benchmark.clientNode());
                }

                List<Double> results = runBenchmark(benchmark, ignite, repeats);
                final double averageWps = results.stream().mapToDouble(s -> s).average().orElseThrow();

                benchmarkResults.add(new BenchmarkResult(benchmark, averageWps, repeats));
            }
        } finally {
            if (ignite != null) {
                ignite.close();
            }
        }

        printResults(benchmarkResults);
        return benchmarkResults;
    }

    private List<Double> runBenchmark(Benchmark benchmark, Ignite ignite, int repeats) throws InterruptedException {
        final IgniteCache<Long, StockOrder> cache = getCache(
                ignite,
                benchmark.servers(),
                benchmark.backups(),
                benchmark.sql(),
                benchmark.streamWriterType() == StreamWriterType.DS_DFLT_PR_SYNC ? PRIMARY_SYNC : FULL_SYNC
        );
        final List<Double> results = new ArrayList<>();
        try {
            final long serversOnStart = ignite.cluster().nodes().stream().filter(n -> !n.isClient()).count();
            final PreparedBenchmark preparedBenchmark = generate(ignite, benchmark);

            for (int i = 0; i < repeats; i++) {
                cache.clear();
                System.gc();
                Thread.sleep(2000);
                System.out.print(benchmark);

                final StopWatch stopWatch = StopWatch.createStarted();
                final double wps = write(ignite, cache, preparedBenchmark);
                System.out.println(". Finished in " + stopWatch + ". Wps: " + Double.valueOf(wps).intValue());
                if (ignite.cluster().forServers().nodes().size() != serversOnStart) {
                    throw new RuntimeException("Server count changed");
                }
                if (ignite.cluster().forServers().nodes().size() < benchmark.servers()) {
                    throw new RuntimeException("At least " + benchmark.servers() + " server nodes required");
                }

                results.add(wps);
            }
        } finally {
            cache.destroy();
        }

        return results;
    }

    private PreparedBenchmark generate(Ignite ignite, Benchmark benchmark) {
        double keyCollision = benchmark.keyCollision();
        int batchSize = benchmark.batchSize();
        int pojoSize = benchmark.pojoBytes();
        int pojoCount = benchmark.pojoCount();
        final Stream<Map.Entry<Long, StockOrder>> pojoStream = keyCollision == 0 ?
                generateUniqueKeysData(pojoSize, pojoCount) :
                generateRandomKeysData(pojoSize, pojoCount, batchSize, keyCollision);
        final IgniteBinary igniteBinary = ignite.binary();
        BinaryObjectBuilder binaryBuilder = igniteBinary.builder(StockOrder.class.getName());
        final StreamWriter streamWriter = switch (benchmark.streamWriterType()) {
            case PUT -> new PutStreamWriter();
            case PUT_ALL -> new PutAllStreamWriter(batchSize);
            case DS_DFLT_PR_SYNC, DS_DFLT_FULL_SYNC -> new DataStreamerStreamWriter(ignite, batchSize, false);
            case DS_BATCH -> new DataStreamerStreamWriter(ignite, batchSize, true);
        };
        streamWriter.allowOverwrite(keyCollision != 0);

        final Function<Map.Entry<Long, StockOrder>, Map.Entry<Long, BinaryObject>> toBinary = entry -> Map.entry(entry.getKey(), entry.getValue().toBinaryByFields(binaryBuilder));

        return switch (benchmark.binaryMode()) {
            case NO_BINARY, CREATE_BY_FIELDS, CREATE_BY_POJO -> new PreparedBenchmark(
                    benchmark.binaryMode(),
                    streamWriter,
                    pojoStream.toList(),
                    null);
            case PREPARED_BINARY -> new PreparedBenchmark(
                    benchmark.binaryMode(),
                    streamWriter,
                    null,
                    pojoStream.map(toBinary).collect(Collectors.toList()));
        };
    }

    private double write(Ignite ignite, IgniteCache<Long, StockOrder> cache, PreparedBenchmark preparedBenchmark) {
        final BinaryMode binaryMode = preparedBenchmark.binaryMode;
        final StreamWriter streamWriter = preparedBenchmark.streamWriter;
        final List<Map.Entry<Long, StockOrder>> pojos = preparedBenchmark.pojos;
        final List<Map.Entry<Long, BinaryObject>> binaryPojos = preparedBenchmark.binaryPojos;

        final IgniteCache<Long, BinaryObject> binaryView = cache.withKeepBinary();
        final IgniteBinary igniteBinary = ignite.binary();
        final BinaryObjectBuilder binaryBuilder = igniteBinary.builder(StockOrder.class.getName());

        final long startTime = System.currentTimeMillis();
        final int entriesWritten = switch (binaryMode) {
            case NO_BINARY -> {
                streamWriter.write(pojos, cache);
                yield pojos.size();
            }
            case PREPARED_BINARY -> {
                streamWriter.writeBinary(binaryPojos, binaryView);
                yield binaryPojos.size();
            }
            case CREATE_BY_FIELDS -> {
                streamWriter.prepareAndWriteBinary(pojos, binaryView, binaryBuilder);
                yield pojos.size();
            }
            case CREATE_BY_POJO -> {
                streamWriter.prepareAndWriteBinary(pojos, binaryView, igniteBinary);
                yield pojos.size();
            }
        };
        final long millisElapsed = System.currentTimeMillis() - startTime;

        return (1000d * entriesWritten / millisElapsed);
    }

    private Stream<Map.Entry<Long, StockOrder>> generateRandomKeysData(int pojoSize, int pojoCount, int batchSize, double keyCollision) {
        final int uniqueKeysInBatch = Math.max(batchSize - (int) (keyCollision * batchSize), 1);
        final StockOrder pojo = StockOrder.ofBytes(pojoSize);
        return LongStream.range(0, pojoCount).mapToObj(i -> Map.entry(i % uniqueKeysInBatch, pojo));
    }

    private Stream<Map.Entry<Long, StockOrder>> generateUniqueKeysData(int pojoSize, int pojoCount) {
        final StockOrder pojo = StockOrder.ofBytes(pojoSize);
        return LongStream.range(0, pojoCount).mapToObj(i -> Map.entry(i, pojo));
    }

    public void printResults(List<BenchmarkResult> results) {
        record BenchmarkValue(
                Function<BenchmarkResult, Object> extractor,
                int maxExpectedLength, String formatSuffix) {
        }
        LinkedHashMap<String, BenchmarkValue> columns = new LinkedHashMap<>();
        columns.put("wps", new BenchmarkValue(BenchmarkResult::resultWps, 8, ".0f"));
        columns.put("method", new BenchmarkValue(
                r -> r.benchmark().streamWriterType(),
                Arrays.stream(StreamWriterType.values()).map(Enum::name).mapToInt(String::length).max().orElse(10),
                "s")
        );
        columns.put("servers", new BenchmarkValue(r -> r.benchmark().servers(), 2, "d"));
        columns.put("backups", new BenchmarkValue(r -> r.benchmark().backups(), 2, "d"));
        columns.put("key collision, %", new BenchmarkValue(r -> (int) (r.benchmark().keyCollision() * 100), 3, "d"));
        columns.put("batch size", new BenchmarkValue(r -> r.benchmark().batchSize(), 7, "d"));
        columns.put("pojo bytes", new BenchmarkValue(r -> r.benchmark().pojoBytes(), 5, "d"));
        columns.put("binary", new BenchmarkValue(
                r -> r.benchmark().binaryMode(),
                Arrays.stream(BinaryMode.values()).map(Enum::name).mapToInt(String::length).max().orElse(10),
                "s")
        );
        columns.put("sql", new BenchmarkValue(r -> r.benchmark().sql(), 7, "b"));
        columns.put("client", new BenchmarkValue(r -> r.benchmark().clientNode(), 7, "b"));
        columns.put("runs", new BenchmarkValue(BenchmarkResult::runs, 1, "d"));
        final String titleFormatString = columns.entrySet().stream().map(e -> "%" + Math.max(e.getKey().length(), e.getValue().maxExpectedLength) + "s").collect(Collectors.joining("\t"));
        System.out.printf(titleFormatString + "\n", columns.keySet().toArray());
        final String resultFormatString =
                "%" + columns.entrySet().stream()
                        .map(e -> Math.max(e.getKey().length(), e.getValue().maxExpectedLength) + e.getValue().formatSuffix)
                        .collect(Collectors.joining("\t%"));
        results.forEach(result -> System.out.printf(resultFormatString + "\n",
                columns.values().stream().map(e -> e.extractor.apply(result)).toArray()));
    }

    private Ignite getIgnite(String[] addresses, boolean isClientNode) {
        final DiscoverySpi discoverySpi = addresses.length == 0 ?
                null :
                new TcpDiscoverySpi().setIpFinder(new TcpDiscoveryVmIpFinder().setAddresses(Arrays.asList(addresses)));
        final Ignite ignite = Ignition.start(new IgniteConfiguration()
                .setClientMode(isClientNode)
                .setDataStorageConfiguration(
                        new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                                new DataRegionConfiguration()
                                        .setMaxSize(6500000000L)
                                        .setInitialSize(6500000000L)
                                        .setName("Default_Region")
                        )
                )
                .setMetricsLogFrequency(0)
                .setDiscoverySpi(discoverySpi)
        );
        for (String cacheName : ignite.cacheNames()) {
            ignite.destroyCache(cacheName);
        }
        return ignite;
    }

    private IgniteCache<Long, StockOrder> getCache(
            Ignite ignite,
            int nodesCount,
            int backups,
            boolean withSql,
            CacheWriteSynchronizationMode writeSynchronizationMode
    ) {
        final Set<UUID> selectedNodes = ignite.cluster().forServers().nodes().stream().sorted((n1, n2) -> {
            if (n1.isLocal()) return -1;
            if (n2.isLocal()) return 1;
            else return (int) (n2.order() - n1.order());
        }).map(ClusterNode::id).limit(nodesCount).collect(Collectors.toSet());
        final IgnitePredicate<ClusterNode> clusterNodeIgnitePredicate = ignite.cluster().forNodeIds(selectedNodes).predicate();
        final CacheConfiguration<Long, StockOrder> cacheConfiguration = new CacheConfiguration<Long, StockOrder>("benchmark-" + UUID.randomUUID())
                .setBackups(backups)
                .setNodeFilter(clusterNodeIgnitePredicate)
                .setWriteSynchronizationMode(writeSynchronizationMode)
                .setAtomicityMode(CacheAtomicityMode.ATOMIC)
                .setCacheMode(CacheMode.PARTITIONED);
        if (withSql) {
            LinkedHashMap<String, String> fields = StockOrder.fields();
            cacheConfiguration.setQueryEntities(Collections.singleton(new QueryEntity()
                    .setKeyType(Long.class.getName())
                    .setValueType(StockOrder.class.getName())
                    .setTableName(StockOrder.class.getSimpleName())
                    .setFields(fields)));
        }
        final IgniteCache<Object, Object> existingCache = ignite.cache(cacheConfiguration.getName());
        if (existingCache != null) {
            existingCache.destroy();
        }
        return ignite.createCache(cacheConfiguration);
    }
}
