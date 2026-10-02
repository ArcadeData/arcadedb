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
package com.arcadedb.integration.importer;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8935: {@code -wal yes} used to be read as false and ran the import with the WAL off.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8935ImporterBooleanSettingsTest {

  @Test
  void aNonBooleanValueIsRefused() {
    final ImporterSettings settings = new ImporterSettings();
    for (final String bad : new String[] { "yes", "1", "on", "tru" })
      assertThatThrownBy(() -> settings.parseParameter("wal", bad))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining(bad);
  }

  @Test
  void everyBooleanSettingIsStrict() {
    final ImporterSettings settings = new ImporterSettings();
    for (final String name : new String[] { "forceDatabaseCreate", "typeIdUnique", "trimText", "probeOnly", "edgeBidirectional" }) {
      assertThatThrownBy(() -> settings.parseParameter(name, "1")).as(name).isInstanceOf(IllegalArgumentException.class);
      settings.parseParameter(name, "true");
    }
  }

  @Test
  void trueAndFalseAreAcceptedTrimmedAndCaseInsensitive() {
    final ImporterSettings settings = new ImporterSettings();
    settings.parseParameter("wal", " TRUE ");
    assertThat(settings.wal).isTrue();
    settings.parseParameter("wal", "False");
    assertThat(settings.wal).isFalse();
  }
}
