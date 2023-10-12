package com.jokerconf.gdomo.ignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class DataStreamerParametersBenchmark {
    public static void main(String[] args) {
        final DiscoverySpi discoverySpi = args.length == 0 ?
                null :
                new TcpDiscoverySpi().setIpFinder(new TcpDiscoveryVmIpFinder().setAddresses(Arrays.asList(args)));
        IgniteConfiguration igniteConfiguration = new IgniteConfiguration()
                .setClientMode(true)
                .setMetricsLogFrequency(0)
                .setDiscoverySpi(discoverySpi);
        try (final Ignite ignite = Ignition.start(igniteConfiguration)) {
            long nodesCount = 5;
            int backups = 0;
            CacheWriteSynchronizationMode writeSynchronizationMode = CacheWriteSynchronizationMode.FULL_SYNC;
            final Set<UUID> selectedNodes = ignite.cluster().forServers().nodes().stream().sorted((n1, n2) -> {
                if (n1.isLocal()) return -1;
                else return (int) (n2.order() - n1.order());
            }).map(ClusterNode::id).limit(nodesCount).collect(Collectors.toSet());
            final IgnitePredicate<ClusterNode> clusterNodeIgnitePredicate = ignite.cluster().forNodeIds(selectedNodes).predicate();
            final CacheConfiguration<Long, StockOrder> cacheConfiguration = new CacheConfiguration<Long, StockOrder>("benchmark-" + UUID.randomUUID())
                    .setBackups(backups)
                    .setNodeFilter(clusterNodeIgnitePredicate)
                    .setWriteSynchronizationMode(writeSynchronizationMode)
                    .setAtomicityMode(CacheAtomicityMode.ATOMIC)
                    .setCacheMode(CacheMode.PARTITIONED);

            final IgniteCache<Object, Object> existingCache = ignite.cache(cacheConfiguration.getName());
            if (existingCache != null) {
                existingCache.destroy();
            }
            IgniteCache<Long, StockOrder> cache = ignite.createCache(cacheConfiguration);
            try {
                try (IgniteDataStreamer<Long, StockOrder> dataStreamer = ignite.dataStreamer(cacheConfiguration.getName())) {
                    StockOrder stockOrder = new StockOrder();
                    long start = System.currentTimeMillis();
                    for (long i = 0; i < 5_000_000; i++) {
                        if (i % 100_000 == 0) {
                            dataStreamer.flush();
                        }
                        dataStreamer.addData(i, stockOrder);
                    }
                    dataStreamer.flush();
                    System.out.println("default: " + (System.currentTimeMillis() - start));
                }
                cache.clear();

                try (IgniteDataStreamer<Long, StockOrder> dataStreamer = ignite.dataStreamer(cacheConfiguration.getName())) {
                    StockOrder stockOrder = new StockOrder();
                    long start = System.currentTimeMillis();
                    for (long i = 0; i < 5_000_000; i++) {
                        if (i % 100_000 == 0) {
                            dataStreamer.flush();
                        }
                        dataStreamer.addData(i, stockOrder);
                    }
                    dataStreamer.flush();
                    System.out.println("default: " + (System.currentTimeMillis() - start));
                }
                cache.clear();

                for (double k = 0.25d; k < 130; k = k * 2) {
                    try (IgniteDataStreamer<Long, StockOrder> dataStreamer = ignite.dataStreamer(cacheConfiguration.getName())) {
                        StockOrder stockOrder = new StockOrder();
                        int size = (int) (dataStreamer.perThreadBufferSize() * k);
                        dataStreamer.perThreadBufferSize(size);
                        dataStreamer.perNodeBufferSize((int) (dataStreamer.perNodeBufferSize() * k));
                        long start = System.currentTimeMillis();
                        for (long i = 0; i < 5_000_000; i++) {
                            if (i % 1000_000 == 0) {
                                dataStreamer.flush();
                            }
                            dataStreamer.addData(i, stockOrder);
                        }
                        dataStreamer.flush();
                        System.out.println(size + ": " + (System.currentTimeMillis() - start));
                    }
                    cache.clear();
                }

                for (double k = 0.25d; k < 130; k = k * 2) {
                    try (IgniteDataStreamer<Long, StockOrder> dataStreamer = ignite.dataStreamer(cacheConfiguration.getName())) {
                        StockOrder stockOrder = new StockOrder();
                        int size = (int) (dataStreamer.perNodeBufferSize() * k);
                        dataStreamer.perNodeBufferSize(size);
                        long start = System.currentTimeMillis();
                        for (long i = 0; i < 5_000_000; i++) {
                            if (i % 1000_000 == 0) {
                                dataStreamer.flush();
                            }
                            dataStreamer.addData(i, stockOrder);
                        }
                        dataStreamer.flush();
                        System.out.println(size + ": " + (System.currentTimeMillis() - start));
                    }
                    cache.clear();
                }
            } finally {
                cache.destroy();
            }
        }
    }
}
