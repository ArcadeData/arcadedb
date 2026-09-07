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
package com.arcadedb.server.ha.raft;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7226: {@code arcadedb.ha.snapshotMaxEntrySize} is {@code SCOPE.SERVER}, but the reader added for #7121 read
 * it off the process-wide {@link GlobalConfiguration} enum. That enum is populated by {@code readConfiguration()}
 * alone - {@code System.getProperty} then {@code System.getenv} - while the server configuration file
 * ({@code ContextConfiguration.fromJSON}), {@code SET SERVER SETTING} and the MCP {@code set_server_setting} tool
 * all write into the server's {@link ContextConfiguration} overlay and never touch it. An operator tuning the
 * zip-bomb cap through any documented channel other than a raw {@code -D} therefore changed nothing, with no error
 * and no warning.
 * <p>
 * These tests drive the overlay directly, which is what all three of those channels ultimately write into, and pin
 * down that {@code -D} (the enum) still works as the fallback for a key nobody set.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Isolated
class Issue7226SnapshotMaxEntrySizeContextConfigurationTest {

  @AfterEach
  void reset() {
    GlobalConfiguration.HA_SNAPSHOT_MAX_ENTRY_SIZE.reset();
  }

  /** The server configuration file path: {@code fromJSON} loads the file into the overlay. */
  @Test
  void theServerConfigurationFileReachesTheLimit() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.fromJSON("{\"configuration\":{\"ha.snapshotMaxEntrySize\":8192}}");

    assertThat(SnapshotInstaller.maxZipEntryUncompressedBytes(configuration))
        .as("a limit set in the server configuration file must be the one enforced")
        .isEqualTo(8_192L);
  }

  /** The {@code SET SERVER SETTING} / MCP {@code set_server_setting} path: both go through {@code setValue}. */
  @Test
  void setServerSettingReachesTheLimit() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.HA_SNAPSHOT_MAX_ENTRY_SIZE, 4_096L);

    assertThat(SnapshotInstaller.maxZipEntryUncompressedBytes(configuration)).isEqualTo(4_096L);
  }

  /** The same, written by key as an admin command carries it: a string, not a typed long. */
  @Test
  void aSettingWrittenByKeyAsAStringReachesTheLimit() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.HA_SNAPSHOT_MAX_ENTRY_SIZE.getKey(), "16384");

    assertThat(SnapshotInstaller.maxZipEntryUncompressedBytes(configuration)).isEqualTo(16_384L);
  }

  /**
   * The overlay wins over the enum, so a server tuned through its own configuration is not overridden by whatever a
   * {@code -D} left on the process. Both are set to different values here, which is the only way to tell the two
   * readers apart.
   */
  @Test
  void theServerOverlayWinsOverTheProcessWideEnum() {
    GlobalConfiguration.HA_SNAPSHOT_MAX_ENTRY_SIZE.setValue(1_048_576L);

    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.HA_SNAPSHOT_MAX_ENTRY_SIZE, 2_048L);

    assertThat(SnapshotInstaller.maxZipEntryUncompressedBytes(configuration)).isEqualTo(2_048L);
  }

  /**
   * ...and for a key the overlay does not carry it falls through to the enum, so {@code -D} keeps working and the
   * fix is not a new way to ignore a channel.
   */
  @Test
  void anUnsetKeyStillFallsThroughToTheEnum() {
    GlobalConfiguration.HA_SNAPSHOT_MAX_ENTRY_SIZE.setValue(65_536L);

    assertThat(SnapshotInstaller.maxZipEntryUncompressedBytes(new ContextConfiguration())).isEqualTo(65_536L);
    assertThat(SnapshotInstaller.maxZipEntryUncompressedBytes(null)).isEqualTo(65_536L);
  }

  /** The non-positive fallback of #7121 has to survive the new channel too. */
  @Test
  void aNonPositiveLimitFromTheOverlayStillFallsBackToTheCompiledDefault() {
    final ContextConfiguration configuration = new ContextConfiguration();
    configuration.setValue(GlobalConfiguration.HA_SNAPSHOT_MAX_ENTRY_SIZE, 0L);

    assertThat(SnapshotInstaller.maxZipEntryUncompressedBytes(configuration))
        .as("a zip-bomb guard an operator can switch off by typing 0 is not a guard")
        .isEqualTo(SnapshotInstaller.MAX_ZIP_ENTRY_UNCOMPRESSED_BYTES);
  }
}
