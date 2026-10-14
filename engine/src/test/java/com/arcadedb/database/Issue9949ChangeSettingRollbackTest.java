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
package com.arcadedb.database;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9949: {@link LocalDatabase#changeSetting(String, Runnable)} re-derives the per-database state from the value the
 * change stored. When that re-derivation fails, the previous value goes back - a setting that was not set is removed, not
 * stored as an explicit entry - so the configuration never names a value the database is not running with.
 */
class Issue9949ChangeSettingRollbackTest extends TestHelper {
  private static final String KEY = GlobalConfiguration.DATE_IMPLEMENTATION.getKey();

  @Test
  void aFailedReDerivationOfASettingThatWasNotSetLeavesItUnset() {
    final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
    assertThat(local.getConfiguration().hasValue(KEY)).isFalse();

    // stored raw, so only the re-derivation finds the class cannot be loaded
    assertThatThrownBy(() -> local.changeSetting(KEY, () -> local.getConfiguration().setValue(KEY, "com.example.NoSuchDate")))
        .isInstanceOf(RuntimeException.class);

    assertThat(local.getConfiguration().hasValue(KEY)).isFalse();
    assertThat(local.getSerializer().getDateImplementation()).isEqualTo(LocalDate.class);
  }

  @Test
  void aFailedReDerivationPutsBackTheValueSetBefore() {
    final LocalDatabase local = (LocalDatabase) ((DatabaseInternal) database).getEmbedded();
    local.changeSetting(KEY, () -> local.getConfiguration().setValue(KEY, Date.class.getName()));
    assertThat(local.getSerializer().getDateImplementation()).isEqualTo(Date.class);

    assertThatThrownBy(() -> local.changeSetting(KEY, () -> local.getConfiguration().setValue(KEY, "com.example.NoSuchDate")))
        .isInstanceOf(RuntimeException.class);

    assertThat(local.getConfiguration().getValue(KEY, null).toString()).isEqualTo(Date.class.getName());
    assertThat(local.getSerializer().getDateImplementation()).isEqualTo(Date.class);
  }
}
