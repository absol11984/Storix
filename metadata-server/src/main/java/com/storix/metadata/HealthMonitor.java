package com.storix.metadata;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Periodically checks node health and triggers repair when nodes become unhealthy.
 */
public class HealthMonitor {

    private final NodeRegistry nodeRegistry;
    private final RepairManager repairManager;
    private final long nodeTimeoutMillis;
    private final long checkIntervalMillis;
    private ScheduledExecutorService scheduler;

    public HealthMonitor(NodeRegistry nodeRegistry, RepairManager repairManager,
                         long nodeTimeoutMillis, long checkIntervalMillis) {
        this.nodeRegistry = nodeRegistry;
        this.repairManager = repairManager;
        this.nodeTimeoutMillis = nodeTimeoutMillis;
        this.checkIntervalMillis = checkIntervalMillis;
    }

    /**
     * Returns the configured health check interval in milliseconds.
     */
    public long getCheckIntervalMillis() {
        return checkIntervalMillis;
    }

    /**
     * Returns the configured node timeout in milliseconds.
     */
    public long getNodeTimeoutMillis() {
        return nodeTimeoutMillis;
    }

    /**
     * Starts the periodic health check.
     */
    public void start() {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "health-monitor");
            t.setDaemon(true);
            return t;
        });

        scheduler.scheduleAtFixedRate(this::checkAndRepair,
                checkIntervalMillis, checkIntervalMillis, TimeUnit.MILLISECONDS);

        System.out.println("[HEALTH] Monitor started (timeout=" + nodeTimeoutMillis + "ms, " +
                "interval=" + checkIntervalMillis + "ms)");
    }

    /**
     * Stops the health monitor.
     */
    public void stop() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    /**
     * Runs a single health check and triggers repair if nodes became unhealthy.
     */
    private void checkAndRepair() {
        try {
            List<String> newlyUnhealthy = nodeRegistry.checkHealth(nodeTimeoutMillis);
            if (!newlyUnhealthy.isEmpty()) {
                System.out.println("[HEALTH] Detected " + newlyUnhealthy.size() +
                        " newly unhealthy node(s): " + String.join(", ", newlyUnhealthy));
                System.out.println("[REPAIR] Triggering automatic repair...");
                RepairManager.RepairResult result = repairManager.repairAll();
                System.out.println("[REPAIR] Complete: scanned=" + result.chunksScanned() +
                        " repaired=" + result.chunksRepaired() +
                        " failed=" + result.chunksFailed() +
                        " healthy=" + result.chunksAlreadyHealthy());
            }
        } catch (Exception e) {
            System.err.println("[HEALTH] Error during health check: " + e.getMessage());
        }
    }
}
