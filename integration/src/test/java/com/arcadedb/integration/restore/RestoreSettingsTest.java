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
package com.arcadedb.integration.restore;

import com.arcadedb.GlobalConfiguration;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Command-line and API surface of the restore-thread setting added by issue #6086. A bad value has to be rejected
 * where the user typed it, not several frames later inside a {@code ThreadPoolExecutor} constructor.
 */
class RestoreSettingsTest {
  @Test
  void parsesTheThreadParameter() {
    final RestoreSettings settings = new RestoreSettings();
    settings.parseParameter("restoreThreads", "4");

    assertThat(settings.restoreThreads).isEqualTo(4);
  }

  @Test
  void leavesItUnsetSoTheGlobalConfigurationApplies() {
    assertThat(new RestoreSettings().restoreThreads).isNull();
  }

  @Test
  void acceptsTheTwoSpecialThreadValues() {
    final RestoreSettings settings = new RestoreSettings();
    settings.parseParameter("restoreThreads", "0");
    assertThat(settings.restoreThreads).isZero();

    settings.parseParameter("restoreThreads", "-1");
    assertThat(settings.restoreThreads).isEqualTo(-1);
  }

  @Test
  void rejectsOutOfRangeAndNonNumericValues() {
    final RestoreSettings settings = new RestoreSettings();

    assertThatThrownBy(() -> settings.parseParameter("restoreThreads", "-2"))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("restoreThreads");
    assertThatThrownBy(() -> settings.parseParameter("restoreThreads", String.valueOf(RestoreSettings.MAX_RESTORE_THREADS + 1)))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("restoreThreads");
    assertThatThrownBy(() -> settings.parseParameter("restoreThreads", "all"))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("integer");
  }

  @Test
  void parsesTheWholeCommandLine() {
    final Restore restore = new Restore("-f target/x.zip -d target/db -o -restoreThreads 2".split(" "));

    assertThat(restore.settings.restoreThreads).isEqualTo(2);
  }

  @Test
  void theApiValidatesTheSameWayTheCommandLineDoes() {
    assertThatThrownBy(() -> new Restore("target/x.zip", "target/db").setRestoreThreads(-2))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("restoreThreads");

    assertThat(new Restore("target/x.zip", "target/db").setRestoreThreads(0).settings.restoreThreads).isZero();
  }

  /**
   * A value can also arrive straight from a system property, a configuration file or {@code setValue}, all of which
   * bypass the CLI and the API, so the bound has to live on the setting itself.
   */
  @Test
  void globalConfigurationRejectsOutOfRangeValues() {
    final int oldThreads = GlobalConfiguration.RESTORE_THREADS.getValueAsInteger();
    try {
      assertThatThrownBy(() -> GlobalConfiguration.RESTORE_THREADS.setValue(-2))
          .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("arcadedb.restore.threads");

      // A REJECTED VALUE MUST NOT BE LEFT BEHIND
      assertThat(GlobalConfiguration.RESTORE_THREADS.getValueAsInteger()).isEqualTo(oldThreads);

      GlobalConfiguration.RESTORE_THREADS.setValue(0);
      assertThat(GlobalConfiguration.RESTORE_THREADS.getValueAsInteger()).isZero();
    } finally {
      GlobalConfiguration.RESTORE_THREADS.setValue(oldThreads);
    }
  }

  /**
   * The thread bound exists twice - {@link RestoreSettings#MAX_RESTORE_THREADS} for the CLI and the API, and a
   * repeated literal in {@code GlobalConfiguration} because the engine module cannot depend on this one. A comment
   * asking the next editor to change both is not enforcement; this is. It fails the moment they drift.
   */
  @Test
  void restoreThreadBoundMatchesTheGlobalConfiguration() {
    final int oldThreads = GlobalConfiguration.RESTORE_THREADS.getValueAsInteger();
    try {
      GlobalConfiguration.RESTORE_THREADS.setValue(RestoreSettings.MAX_RESTORE_THREADS);
      assertThat(GlobalConfiguration.RESTORE_THREADS.getValueAsInteger()).isEqualTo(RestoreSettings.MAX_RESTORE_THREADS);

      assertThatThrownBy(() -> GlobalConfiguration.RESTORE_THREADS.setValue(RestoreSettings.MAX_RESTORE_THREADS + 1))
          .isInstanceOf(IllegalArgumentException.class);
    } finally {
      GlobalConfiguration.RESTORE_THREADS.setValue(oldThreads);
    }
  }
}
