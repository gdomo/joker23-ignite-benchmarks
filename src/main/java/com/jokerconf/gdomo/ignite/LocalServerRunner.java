package com.jokerconf.gdomo.ignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

public class LocalServerRunner {
    public static void main(String[] args) throws InterruptedException {
        try (Ignite ignite = Ignition.start(new IgniteConfiguration()
                .setClientMode(false)
                .setDataStorageConfiguration(new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                                new DataRegionConfiguration().setMaxSize(5 * 1024 * 1024 * 1024L)
                        )
                )
        )) {
            Thread.sleep(Long.MAX_VALUE);
        }
    }
}
