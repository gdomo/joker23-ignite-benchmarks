package com.jokerconf.gdomo.ignite;

import com.sun.management.GarbageCollectionNotificationInfo;
import com.sun.management.GcInfo;

import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.Map;

public class GcLogger {
    private static boolean started = false;
    public static synchronized void startLoggingGc() {
        if (started) {
            return;
        }
        // http://www.programcreek.com/java-api-examples/index.php?class=javax.management.MBeanServerConnection&method=addNotificationListener
        // https://docs.oracle.com/javase/8/docs/jre/api/management/extension/com/sun/management/GarbageCollectionNotificationInfo.html#GARBAGE_COLLECTION_NOTIFICATION
        for (GarbageCollectorMXBean gcMbean : ManagementFactory.getGarbageCollectorMXBeans()) {
            try {
                ManagementFactory.getPlatformMBeanServer().
                        addNotificationListener(gcMbean.getObjectName(), listener, null, null);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        started = true;
    }

    private static final NotificationListener listener = (notification, handback) -> {
        if (notification.getType().equals(GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION)) {
            // https://docs.oracle.com/javase/8/docs/jre/api/management/extension/com/sun/management/GarbageCollectionNotificationInfo.html
            CompositeData cd = (CompositeData) notification.getUserData();
            GarbageCollectionNotificationInfo gcNotificationInfo = GarbageCollectionNotificationInfo.from(cd);
            GcInfo gcInfo = gcNotificationInfo.getGcInfo();
            if (gcNotificationInfo.getGcAction().contains("major")) {
                System.out.println("Major GC " + gcInfo.getDuration() + " ms");
//                System.out.println("GarbageCollection: " +
//                        gcNotificationInfo.getGcAction() + " " +
//                        gcNotificationInfo.getGcName() +
//                        " duration: " + gcInfo.getDuration() + "ms" +
//                        " used: " + sumUsedMb(gcInfo.getMemoryUsageBeforeGc()) + "MB" +
//                        " -> " + sumUsedMb(gcInfo.getMemoryUsageAfterGc()) + "MB");
            }
        }
    };

    static private long sumUsedMb(Map<String, MemoryUsage> memUsages) {
        long sum = 0;
        for (MemoryUsage memoryUsage : memUsages.values()) {
            sum += memoryUsage.getUsed();
        }
        return sum / (1024 * 1024);
    }
}