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
package com.arcadedb.query.sql;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #7124: {@code ALTER DATABASE ... SETTING} is the fourth administrative writer into a
 * {@code ContextConfiguration}, and it converted through the permissive {@code GlobalConfiguration.coerce}, which
 * reads a boolean it cannot parse as {@code false}. So
 * {@code ALTER DATABASE `arcadedb.txWAL` 'ture'} answered with a result row saying {@code newValue: false} and
 * turned the write-ahead log OFF for the database - a durability hazard produced by a typo, reported as a success,
 * and saved to the database configuration.
 * <p>
 * {@code TX_WAL} is used here rather than an arbitrary boolean setting precisely because the consequence of getting
 * it wrong is the worst one in the set.
 */
class Issue7124AlterDatabaseBooleanSettingTest extends TestHelper {

  @Test
  void alterDatabaseRefusesABooleanTypoInsteadOfTurningTheSettingOff() {
    final boolean before = database.getConfiguration().getValueAsBoolean(GlobalConfiguration.TX_WAL);

    assertThatThrownBy(() -> database.command("sql", "alter database `arcadedb.txWAL` 'ture'")).hasMessageContaining(
        "arcadedb.txWAL");

    assertThat(database.getConfiguration().getValueAsBoolean(GlobalConfiguration.TX_WAL)).as(
        "a refused statement must leave the setting exactly as it was, not at the value the typo would have produced")
        .isEqualTo(before);
  }

  @Test
  void alterDatabaseStillStoresBothBooleanLiterals() {
    try (final ResultSet result = database.command("sql", "alter database `arcadedb.txWAL` 'false'")) {
      assertThat(result.next().<Object>getProperty("newValue")).isEqualTo(Boolean.FALSE);
    }
    assertThat(database.getConfiguration().getValueAsBoolean(GlobalConfiguration.TX_WAL)).isFalse();

    try (final ResultSet result = database.command("sql", "alter database `arcadedb.txWAL` 'TRUE'")) {
      assertThat(result.next().<Object>getProperty("newValue")).isEqualTo(Boolean.TRUE);
    }
    assertThat(database.getConfiguration().getValueAsBoolean(GlobalConfiguration.TX_WAL)).isTrue();
  }

  @Test
  void anUnquotedBooleanLiteralStillWorks() {
    // THE SQL EXPRESSION EVALUATES TO A Boolean RATHER THAN TEXT: THE STRICT PARSE MUST LET IT THROUGH UNTOUCHED.
    try (final ResultSet result = database.command("sql", "alter database `arcadedb.txWAL` false")) {
      assertThat(result.next().<Object>getProperty("newValue")).isEqualTo(Boolean.FALSE);
    }
    assertThat(database.getConfiguration().getValueAsBoolean(GlobalConfiguration.TX_WAL)).isFalse();

    try (final ResultSet result = database.command("sql", "alter database `arcadedb.txWAL` true")) {
      assertThat(result.next().<Object>getProperty("newValue")).isEqualTo(Boolean.TRUE);
    }
    assertThat(database.getConfiguration().getValueAsBoolean(GlobalConfiguration.TX_WAL)).isTrue();
  }
}
