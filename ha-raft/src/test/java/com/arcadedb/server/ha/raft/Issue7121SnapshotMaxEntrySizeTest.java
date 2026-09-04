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

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7121: {@code arcadedb.ha.snapshotMaxEntrySize} declared and documented the per-entry zip-bomb limit, but
 * the extraction path used a hardcoded constant with no reader for the setting, so the only way to change the limit
 * was to recompile {@link SnapshotInstaller}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Isolated
class Issue7121SnapshotMaxEntrySizeTest {

  @AfterEach
  void reset() {
    GlobalConfiguration.HA_SNAPSHOT_MAX_ENTRY_SIZE.reset();
  }

  @Test
  void theConfiguredLimitIsTheOneEnforced() {
    GlobalConfiguration.HA_SNAPSHOT_MAX_ENTRY_SIZE.setValue(4_096L);
    assertThat(SnapshotInstaller.maxZipEntryUncompressedBytes()).isEqualTo(4_096L);
  }

  @Test
  void aNonPositiveLimitFallsBackToTheCompiledDefaultInsteadOfDisablingTheGuard() {
    GlobalConfiguration.HA_SNAPSHOT_MAX_ENTRY_SIZE.setValue(0L);
    assertThat(SnapshotInstaller.maxZipEntryUncompressedBytes())
        .as("a zip-bomb guard an operator can switch off by typing 0 is not a guard")
        .isEqualTo(SnapshotInstaller.MAX_ZIP_ENTRY_UNCOMPRESSED_BYTES);

    GlobalConfiguration.HA_SNAPSHOT_MAX_ENTRY_SIZE.setValue(-1L);
    assertThat(SnapshotInstaller.maxZipEntryUncompressedBytes())
        .isEqualTo(SnapshotInstaller.MAX_ZIP_ENTRY_UNCOMPRESSED_BYTES);
  }

  @Test
  void theDefaultMatchesTheDeclaredSetting() {
    assertThat(GlobalConfiguration.HA_SNAPSHOT_MAX_ENTRY_SIZE.getValueAsLong())
        .as("the declared default and the compiled fallback must not drift apart")
        .isEqualTo(SnapshotInstaller.MAX_ZIP_ENTRY_UNCOMPRESSED_BYTES);
  }
}
