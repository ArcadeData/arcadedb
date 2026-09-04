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
import com.arcadedb.server.monitor.ServerMonitor.SafepointSpike;
import com.arcadedb.server.monitor.ServerMonitor.SafepointSpikeDetector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for the first two findings of issue #7124.
 *
 * <ol>
 *   <li>the low-disk warning measured the JVM working directory instead of the configured database directory, so on
 *       the normal container layout (databases on a mounted volume) it reported the wrong filesystem and stayed
 *       quiet while the data volume filled;</li>
 *   <li>the safepoint "spike" check compared two LIFETIME cumulative averages, which after the first minutes are
 *       both dominated by history, so the warning fired at startup and then never again.</li>
 * </ol>
 */
class Issue7124ServerMonitorTest {

  @TempDir
  Path tempDir;

  @Test
  void diskSpaceIsMeasuredOnTheConfiguredDatabaseDirectory() throws Exception {
    final File databases = tempDir.resolve("mnt").resolve("databases").toFile();
    assertThat(databases.mkdirs()).isTrue();

    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, databases.getAbsolutePath());

    assertThat(ServerMonitor.resolveDiskSpaceDirectory(configuration).getCanonicalFile()).isEqualTo(
        databases.getCanonicalFile());
  }

  @Test
  void aNotYetCreatedDatabaseDirectoryResolvesToItsClosestExistingAncestor() throws Exception {
    // AT STARTUP THE DATABASE DIRECTORY DOES NOT EXIST YET: MEASURING A NON-EXISTENT FILE RETURNS 0/0 AND WOULD
    // SILENCE THE CHECK. THE CLOSEST EXISTING ANCESTOR SITS ON THE SAME FILESYSTEM THE DATABASES WILL LAND ON.
    final File volume = tempDir.resolve("volume").toFile();
    assertThat(volume.mkdirs()).isTrue();

    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY,
        volume.getAbsolutePath() + File.separator + "not-created-yet" + File.separator + "databases");

    final File resolved = ServerMonitor.resolveDiskSpaceDirectory(configuration);
    assertThat(resolved.getCanonicalFile()).isEqualTo(volume.getCanonicalFile());
    assertThat(resolved.getTotalSpace()).isGreaterThan(0L);
  }

  @Test
  void aBlankDatabaseDirectoryFallsBackToTheWorkingDirectory() throws Exception {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.SERVER_DATABASE_DIRECTORY, "   ");

    assertThat(ServerMonitor.resolveDiskSpaceDirectory(configuration).getCanonicalFile()).isEqualTo(
        new File(".").getCanonicalFile());
  }

  @Test
  void aNullConfigurationFallsBackToTheWorkingDirectory() throws Exception {
    assertThat(ServerMonitor.resolveDiskSpaceDirectory(null).getCanonicalFile()).isEqualTo(new File(".").getCanonicalFile());
  }

  @Test
  void aSpikeIsMeasuredOverTheIntervalNotOverTheLifetimeAverage() {
    final SafepointSpikeDetector detector = new SafepointSpikeDetector();

    // A LONG-RUNNING JVM: 100,000 SAFEPOINTS TOTALLING 100,000ms, SO THE LIFETIME AVERAGE IS 1.00ms.
    assertThat(detector.sample(100_000L, 100_000L)).isNull();
    // FIRST INTERVAL: 100 SAFEPOINTS IN 100ms, STILL 1.00ms EACH. NOTHING TO COMPARE AGAINST YET.
    assertThat(detector.sample(100_100L, 100_100L)).isNull();

    // SECOND INTERVAL: 100 SAFEPOINTS IN 300ms - THE AVERAGE PAUSE TRIPLED. THE LIFETIME AVERAGE ONLY MOVES FROM
    // 1.0000ms TO 1.0020ms (+0.2%), WELL UNDER THE 20% THRESHOLD, WHICH IS EXACTLY WHY THE OLD CHECK STAYED SILENT.
    final SafepointSpike spike = detector.sample(100_400L, 100_200L);
    assertThat(spike).isNotNull();
    assertThat(spike.previousIntervalAvgMs()).isEqualTo(1.0F);
    assertThat(spike.currentIntervalAvgMs()).isEqualTo(3.0F);
    assertThat(spike.deltaPerc()).isEqualTo(200.0F);
  }

  @Test
  void aSteadyIntervalAverageReportsNoSpike() {
    final SafepointSpikeDetector detector = new SafepointSpikeDetector();

    assertThat(detector.sample(1_000L, 1_000L)).isNull();
    assertThat(detector.sample(1_100L, 1_100L)).isNull();
    assertThat(detector.sample(1_200L, 1_200L)).isNull();
    assertThat(detector.sample(1_300L, 1_300L)).isNull();
  }

  @Test
  void aQuietIntervalWithNoNewSafepointsIsNotASpike() {
    final SafepointSpikeDetector detector = new SafepointSpikeDetector();

    assertThat(detector.sample(1_000L, 1_000L)).isNull();
    assertThat(detector.sample(1_100L, 1_100L)).isNull();
    // NO SAFEPOINT AT ALL DURING THE INTERVAL: THE COUNTERS DO NOT MOVE, SO THERE IS NO NEW AVERAGE TO COMPARE.
    assertThat(detector.sample(1_100L, 1_100L)).isNull();
    // AND THE BASELINE MUST STILL BE THE 1.00ms OF THE LAST INTERVAL THAT ACTUALLY HAD SAFEPOINTS.
    final SafepointSpike spike = detector.sample(1_400L, 1_200L);
    assertThat(spike).isNotNull();
    assertThat(spike.previousIntervalAvgMs()).isEqualTo(1.0F);
    assertThat(spike.currentIntervalAvgMs()).isEqualTo(3.0F);
  }

  @Test
  void aDropInTheAveragePauseIsNotReported() {
    final SafepointSpikeDetector detector = new SafepointSpikeDetector();

    assertThat(detector.sample(1_000L, 1_000L)).isNull();
    assertThat(detector.sample(1_300L, 1_100L)).isNull();  // 3.00ms average over the interval
    assertThat(detector.sample(1_400L, 1_200L)).isNull();  // back to 1.00ms: an improvement, not a spike
  }
}
