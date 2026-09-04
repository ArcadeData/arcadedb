/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.monitor;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
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
	private static final float SAFEPOINT_SPIKE_THRESHOLD_PERC = 20F;
	private final ArcadeDBServer server;
	private volatile Thread checker;
	private AtomicBoolean running = new AtomicBoolean(false);
	// READ AND WRITTEN ONLY BY THE MONITOR THREAD.
	private final SafepointSpikeDetector safepointSpikeDetector = new SafepointSpikeDetector();
	// WRITTEN BY THE MONITOR THREAD, READ BY getStatus() FROM ANY THREAD: volatile GUARANTEES VISIBILITY.
	private volatile long lastHeapWarningReported = 0L;
	private volatile long lastDiskSpaceWarningReported = 0L;

	// JMX related fields
	private MBeanServer mBeanServer;
	private ObjectName hotspotRuntimeMBean;
	// WRITTEN BY THE MONITOR THREAD, READ BY getStatus() FROM ANY THREAD: volatile GUARANTEES VISIBILITY.
	private volatile boolean safepointMonitoringAvailable = false;
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
			final File monitoredDir = resolveDiskSpaceDirectory(server.getConfiguration());
			final long freeSpace = monitoredDir.getUsableSpace(); // Better than getFreeSpace()
			final long totalSpace = monitoredDir.getTotalSpace();

			if (totalSpace > 0) {
				final float freeSpacePerc = freeSpace * 100F / totalSpace;
				if (freeSpacePerc < 20) {
					// REPORT THE SPIKE
					server.getEventLog().reportEvent(ServerEventLog.EVENT_TYPE.WARNING, "JVM", null,
							String.format("Available space on disk is only %.1f%% (%.2f GB free of %.2f GB total) on '%s'", freeSpacePerc,
									freeSpace / (1024.0 * 1024.0 * 1024.0), totalSpace / (1024.0 * 1024.0 * 1024.0), monitoredDir.getPath()));
					lastDiskSpaceWarningReported = System.currentTimeMillis();
				}
			}
		}
		catch (SecurityException e) {
			LOGGER.log(Level.FINE, "Cannot check disk space due to security restrictions", e);
		}
	}

	/**
	 * Returns the directory whose filesystem the low-disk warning must measure.
	 * <p>
	 * Issue #7124: this used to be the JVM working directory. When the databases live on a mounted volume - the
	 * normal container and Kubernetes layout - that reports the container filesystem and the warning stays quiet
	 * while the data volume fills, which is precisely the condition it exists to precede.
	 * <p>
	 * The configured directory does not necessarily exist yet: a not-yet-created path reports 0 usable and 0 total
	 * bytes, which the {@code totalSpace > 0} guard reads as "cannot tell" and the check goes silent. Walking up to
	 * the closest existing ancestor measures the filesystem the databases are going to land on, which is the number
	 * the operator needs. The working directory remains the fallback so the check never has nothing to measure.
	 */
	static File resolveDiskSpaceDirectory(final ContextConfiguration configuration) {
		if (configuration != null) {
			try {
				final String configured = configuration.getValueAsString(GlobalConfiguration.SERVER_DATABASE_DIRECTORY);
				if (configured != null && !configured.isBlank()) {
					File dir = new File(configured.trim()).getAbsoluteFile();
					while (dir != null && !dir.exists())
						dir = dir.getParentFile();

					if (dir != null)
						return dir;
				}
			}
			catch (Exception e) {
				LOGGER.log(Level.FINE, "Cannot resolve the configured database directory, falling back to the working directory", e);
			}
		}

		return new File(".");
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

					// Do NOT force System.gc() here: a stop-the-world collection triggered precisely when the
					// server is already under memory pressure can cascade into HA election timeouts. Log the
					// critical condition and leave heap management to the JVM collector.
					if (heapAvailablePerc < 10)
						LOGGER.log(Level.WARNING, "Available heap RAM is critically low; relying on the JVM garbage collector");
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
				final SafepointSpike spike = safepointSpikeDetector.sample(hotspotSafepointTime, hotspotSafepointCount);
				if (spike != null)
					// REPORT THE SPIKE
					server.getEventLog().reportEvent(ServerEventLog.EVENT_TYPE.WARNING, "JVM", null, String.format(
							"Server overloaded: JVM Safepoint spiked up %.1f%% from the last sampling (avg time: %.2fms -> %.2fms)",
							spike.deltaPerc(), spike.previousIntervalAvgMs(), spike.currentIntervalAvgMs()));
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
	 * A safepoint-pause spike measured over one sampling interval: the average pause of the previous interval, the
	 * average pause of the interval just closed, and the increase between them as a percentage.
	 */
	record SafepointSpike(float previousIntervalAvgMs, float currentIntervalAvgMs, float deltaPerc) {
	}

	/**
	 * Detects a rise in the average JVM safepoint pause between consecutive sampling intervals.
	 * <p>
	 * Issue #7124: the check this replaces compared {@code TotalSafepointTime / SafepointCount} at one sample against
	 * the same ratio at the previous one. Both are LIFETIME cumulative averages, so within a few minutes of startup
	 * each is dominated by history and their difference tends to zero - the warning fired during startup and then
	 * never again, whatever the JVM went on to do. The message claiming the spike was "from the last sampling" was
	 * something those two numbers could not express.
	 * <p>
	 * Keeping the previous sample's RAW counters makes the interval measurable: {@code deltaTime / deltaCount} is the
	 * average pause of the interval just closed, and comparing consecutive interval averages is what "from the last
	 * sampling" means. Three samples are therefore needed before the first comparison: one to open the first
	 * interval, one to close it, and one to close the interval that gets compared against it.
	 * <p>
	 * Not thread-safe: a single {@link ServerMonitor} thread owns one instance.
	 */
	static final class SafepointSpikeDetector {
		private long  lastSafepointTime  = 0L;
		private long  lastSafepointCount = 0L;
		private float lastIntervalAvgMs  = -1F;

		/**
		 * Feeds the two monotonically increasing HotSpot counters of one sample.
		 *
		 * @return the spike, or {@code null} when there is nothing to report - no safepoint happened during the
		 * interval, no earlier interval exists to compare against, or the average pause did not rise past the
		 * threshold.
		 */
		SafepointSpike sample(final long totalSafepointTime, final long safepointCount) {
			SafepointSpike spike = null;

			// A QUIET INTERVAL LEAVES THE COUNTERS WHERE THEY WERE: THERE IS NO NEW AVERAGE, AND THE BASELINE MUST
			// STAY THE LAST INTERVAL THAT ACTUALLY HAD SAFEPOINTS RATHER THAN COLLAPSING TO ZERO.
			if (lastSafepointCount > 0 && safepointCount > lastSafepointCount) {
				final long deltaCount = safepointCount - lastSafepointCount;
				final long deltaTime = Math.max(0L, totalSafepointTime - lastSafepointTime);
				final float intervalAvgMs = deltaTime / (float) deltaCount;

				if (lastIntervalAvgMs > 0) {
					final float deltaPerc = (intervalAvgMs - lastIntervalAvgMs) * 100 / lastIntervalAvgMs;
					if (deltaPerc > SAFEPOINT_SPIKE_THRESHOLD_PERC)
						spike = new SafepointSpike(lastIntervalAvgMs, intervalAvgMs, deltaPerc);
				}

				lastIntervalAvgMs = intervalAvgMs;
			}

			lastSafepointTime = totalSafepointTime;
			lastSafepointCount = safepointCount;

			return spike;
		}
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
