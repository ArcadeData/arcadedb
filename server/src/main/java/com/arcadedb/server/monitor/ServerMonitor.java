/*
 * Copyright 2023 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.arcadedb.server.monitor;

import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.event.ServerEventLog;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Monitor ArcadeDB's server health.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ServerMonitor {
	private static final Logger LOGGER = Logger.getLogger(ServerMonitor.class.getName());
	private static final int INTERVAL_TIME = 10_000;
	private static final int MINS_30 = 30 * 60 * 1_000;
	private static final int HOURS_24 = 24 * 60 * 60 * 1_000;
	private final ArcadeDBServer server;
	private volatile Thread checker;
	private AtomicBoolean running = new AtomicBoolean(false);
	private long lastHotspotSafepointTime = 0L;
	private long lastHotspotSafepointCount = 0L;
	private long lastHeapWarningReported = 0L;
	private long lastDiskSpaceWarningReported = 0L;

	// JMX related fields
	private MBeanServer mBeanServer;
	private ObjectName hotspotRuntimeMBean;
	private boolean safepointMonitoringAvailable = false;
	private MemoryMXBean memoryMXBean;

	public ServerMonitor(final ArcadeDBServer server) {
		this.server = server;
		initializeJMXMonitoring();
	}

	private void initializeJMXMonitoring() {
		try {
			mBeanServer = ManagementFactory.getPlatformMBeanServer();
			memoryMXBean = ManagementFactory.getMemoryMXBean();

			// Try to access HotSpot runtime MBean through standard JMX
			try {
				hotspotRuntimeMBean = new ObjectName("sun.management:type=HotspotRuntime");
				if (mBeanServer.isRegistered(hotspotRuntimeMBean)) {
					safepointMonitoringAvailable = true;
					LOGGER.log(Level.FINE, "HotSpot safepoint monitoring is available");
				}
			}
			catch (Exception e) {
				LOGGER.log(Level.FINE, "HotSpot safepoint monitoring is not available: " + e.getMessage());
			}
		}
		catch (Exception e) {
			LOGGER.log(Level.WARNING, "Failed to initialize JMX monitoring: " + e.getMessage(), e);
		}
	}

	public void start() {
		running.set(true);
		checker = new Thread(() -> monitor(), "ArcadeDB-ServerMonitor");
		checker.setDaemon(true);
		checker.start();
	}

	private void monitor() {
		while (running.get()) {
			try {
				checkDiskSpace();
				checkHeapRAM();
				checkJVMHotSpot();

				Thread.sleep(INTERVAL_TIME);
			}
			catch (InterruptedException e) {
				// Expected when stopping the monitor
				Thread.currentThread().interrupt();
				break;
			}
			catch (Exception e) {
				// Log the error but continue monitoring
				LOGGER.log(Level.WARNING, "Error during server monitoring", e);
			}
		}
	}

	private void checkDiskSpace() {
		if (System.currentTimeMillis() - lastDiskSpaceWarningReported < HOURS_24) {
			// REPORT ONLY EVERY 24H FROM THE LAST WARNING
			return;
		}

		try {
			final File currentDir = new File(".");
			final long freeSpace = currentDir.getUsableSpace(); // Better than getFreeSpace()
			final long totalSpace = currentDir.getTotalSpace();

			if (totalSpace > 0) {
				final float freeSpacePerc = freeSpace * 100F / totalSpace;
				if (freeSpacePerc < 20) {
					// REPORT THE SPIKE
					server.getEventLog().reportEvent(ServerEventLog.EVENT_TYPE.WARNING, "JVM", null,
							String.format("Available space on disk is only %.1f%% (%.2f GB free of %.2f GB total)", freeSpacePerc,
									freeSpace / (1024.0 * 1024.0 * 1024.0), totalSpace / (1024.0 * 1024.0 * 1024.0)));
					lastDiskSpaceWarningReported = System.currentTimeMillis();
				}
			}
		}
		catch (SecurityException e) {
			LOGGER.log(Level.FINE, "Cannot check disk space due to security restrictions", e);
		}
	}

	private void checkHeapRAM() {
		if (System.currentTimeMillis() - lastHeapWarningReported < MINS_30) {
			// REPORT ONLY EVERY 30 MINS FROM THE LAST WARNING
			return;
		}

		try {
			// Use MemoryMXBean for more accurate memory information
			MemoryUsage heapUsage = memoryMXBean.getHeapMemoryUsage();

			final long heapUsed = heapUsage.getUsed();
			final long heapMax = heapUsage.getMax();

			if (heapMax > 0) {
				final float heapAvailablePerc = (heapMax - heapUsed) * 100F / heapMax;

				if (heapAvailablePerc < 20) {
					// REPORT THE SPIKE
					server.getEventLog().reportEvent(ServerEventLog.EVENT_TYPE.WARNING, "JVM", null,
							String.format("Server overloaded: available heap RAM is only %.1f%% (%.2f GB used of %.2f GB max)",
									heapAvailablePerc, heapUsed / (1024.0 * 1024.0 * 1024.0), heapMax / (1024.0 * 1024.0 * 1024.0)));
					lastHeapWarningReported = System.currentTimeMillis();

					// Optionally suggest GC if memory is critically low
					if (heapAvailablePerc < 10) {
						LOGGER.log(Level.INFO, "Suggesting garbage collection due to low memory");
						System.gc();
					}
				}
			}
		}
		catch (Exception e) {
			LOGGER.log(Level.FINE, "Error checking heap memory", e);
		}
	}

	private void checkJVMHotSpot() {
		if (!safepointMonitoringAvailable) {
			// Safepoint monitoring not available, skip this check
			return;
		}

		try {
			// Access HotSpot metrics through JMX MBean
			Long hotspotSafepointTime = (Long) mBeanServer.getAttribute(hotspotRuntimeMBean, "TotalSafepointTime");
			Long hotspotSafepointCount = (Long) mBeanServer.getAttribute(hotspotRuntimeMBean, "SafepointCount");

			if (hotspotSafepointTime != null && hotspotSafepointCount != null && hotspotSafepointCount > 0) {
				if (lastHotspotSafepointCount > 0) {
					final float lastAvgSafepointTime = lastHotspotSafepointTime / (float) lastHotspotSafepointCount;
					final float avgSafepointTime = hotspotSafepointTime / (float) hotspotSafepointCount;

					if (lastAvgSafepointTime > 0) {
						final float deltaPerc = (avgSafepointTime - lastAvgSafepointTime) * 100 / lastAvgSafepointTime;

						if (deltaPerc > 20) {
							// REPORT THE SPIKE
							server.getEventLog().reportEvent(ServerEventLog.EVENT_TYPE.WARNING, "JVM", null, String.format(
									"Server overloaded: JVM Safepoint spiked up %.1f%% from the last sampling (avg time: %.2fms -> %.2fms)",
									deltaPerc, lastAvgSafepointTime, avgSafepointTime));
						}
					}
				}

				lastHotspotSafepointTime = hotspotSafepointTime;
				lastHotspotSafepointCount = hotspotSafepointCount;
			}
		}
		catch (Exception e) {
			// If we can't access safepoint metrics, disable future attempts
			safepointMonitoringAvailable = false;
			LOGGER.log(Level.FINE, "Cannot access HotSpot safepoint metrics, disabling this monitoring", e);
		}
	}

	/**
	 * Alternative monitoring using GC metrics if safepoint monitoring is not
	 * available
	 */
	private void checkGCMetrics() {
		try {
			var gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
			long totalGCTime = 0;
			long totalGCCount = 0;

			for (var gcBean : gcBeans) {
				totalGCTime += gcBean.getCollectionTime();
				totalGCCount += gcBean.getCollectionCount();
			}

			// You can add logic here to track and report on GC spikes
			// similar to safepoint monitoring

		}
		catch (Exception e) {
			LOGGER.log(Level.FINE, "Error checking GC metrics", e);
		}
	}

	public void stop() {
		running.set(false);

		if (checker != null) {
			try {
				checker.interrupt();
				checker.join(INTERVAL_TIME + 100);
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				LOGGER.log(Level.FINE, "Interrupted while stopping monitor thread", e);
			}
		}
	}

	/**
	 * Get current monitoring status
	 */
	public MonitoringStatus getStatus() {
		return new MonitoringStatus(running.get(), safepointMonitoringAvailable, System.currentTimeMillis() - lastHeapWarningReported < MINS_30,
				System.currentTimeMillis() - lastDiskSpaceWarningReported < HOURS_24);
	}

	/**
	 * Status class for monitoring information
	 */
	public static class MonitoringStatus {
		public final boolean isRunning;
		public final boolean safepointMonitoringAvailable;
		public final boolean recentHeapWarning;
		public final boolean recentDiskWarning;

		public MonitoringStatus(boolean isRunning, boolean safepointMonitoringAvailable, boolean recentHeapWarning, boolean recentDiskWarning) {
			this.isRunning = isRunning;
			this.safepointMonitoringAvailable = safepointMonitoringAvailable;
			this.recentHeapWarning = recentHeapWarning;
			this.recentDiskWarning = recentDiskWarning;
		}
	}
}