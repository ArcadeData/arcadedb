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
package com.arcadedb.integration.backup;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Command-line surface of the compression settings added by issue #6072. A bad value has to be rejected where the user
 * typed it, not several frames later inside the JDK's {@code Deflater}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BackupSettingsTest {
  @Test
  void parsesTheCompressionParameters() {
    final BackupSettings settings = new BackupSettings();
    settings.parseParameter("compressionLevel", "9");
    settings.parseParameter("compressionThreads", "4");
    settings.parseParameter("maxMBPerSecond", "50");

    assertThat(settings.compressionLevel).isEqualTo(9);
    assertThat(settings.compressionThreads).isEqualTo(4);
    assertThat(settings.maxMBPerSecond).isEqualTo(50);
  }

  @Test
  void leavesThemUnsetSoTheGlobalConfigurationApplies() {
    final BackupSettings settings = new BackupSettings();

    assertThat(settings.compressionLevel).isNull();
    assertThat(settings.compressionThreads).isNull();
    assertThat(settings.maxMBPerSecond).isNull();
  }

  @Test
  void acceptsTheTwoSpecialThreadValues() {
    final BackupSettings settings = new BackupSettings();
    settings.parseParameter("compressionThreads", "0");
    assertThat(settings.compressionThreads).isZero();

    settings.parseParameter("compressionThreads", "-1");
    assertThat(settings.compressionThreads).isEqualTo(-1);
  }

  @Test
  void rejectsOutOfRangeAndNonNumericValues() {
    final BackupSettings settings = new BackupSettings();

    assertThatThrownBy(() -> settings.parseParameter("compressionLevel", "10"))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("compressionLevel");
    assertThatThrownBy(() -> settings.parseParameter("compressionLevel", "-1"))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("compressionLevel");
    assertThatThrownBy(() -> settings.parseParameter("compressionThreads", "-2"))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("compressionThreads");
    assertThatThrownBy(() -> settings.parseParameter("maxMBPerSecond", "-1"))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("maxMBPerSecond");
    assertThatThrownBy(() -> settings.parseParameter("compressionLevel", "fastest"))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("integer");
  }

  @Test
  void parsesTheWholeCommandLine() {
    final Backup backup = new Backup(
        "-f target/x.zip -d target/db -o -compressionLevel 6 -compressionThreads 2 -maxMBPerSecond 20".split(" "));

    assertThat(backup.settings.compressionLevel).isEqualTo(6);
    assertThat(backup.settings.compressionThreads).isEqualTo(2);
    assertThat(backup.settings.maxMBPerSecond).isEqualTo(20);
  }

  @Test
  void throttlerIsDisabledForNonPositiveRates() {
    assertThat(new IoThrottler(0).isEnabled()).isFalse();
    assertThat(new IoThrottler(-5).isEnabled()).isFalse();
    assertThat(new IoThrottler(1).isEnabled()).isTrue();
  }
}
