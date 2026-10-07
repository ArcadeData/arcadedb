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
package com.arcadedb.postgres;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #9329 at the settings level: {@code search_path} is accepted and never stored, so {@code SHOW}
 * answers the schema the server resolves names in; {@code statement_timeout} is parsed as PostgreSQL parses it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9329SearchPathAndStatementTimeoutTest {

  @Test
  void searchPathAlwaysAnswersTheConnectedSchema() {
    final PostgresSessionSettings settings = new PostgresSessionSettings();
    settings.setCurrentSchema("pgtest");
    assertThat(settings.show("search_path")).isEqualTo("pgtest");

    settings.set("search_path", "other");
    assertThat(settings.show("search_path")).isEqualTo("pgtest");
    settings.set("search_path", "nosuchschema", true);
    assertThat(settings.show("SEARCH_PATH")).isEqualTo("pgtest");
    settings.set("search_path", null);
    assertThat(settings.show("search_path")).isEqualTo("pgtest");
    assertThat(settings.showAll()).filteredOn(row -> row[0].equals("search_path")).extracting(row -> row[1]).containsExactly("pgtest");
  }

  @ParameterizedTest
  @CsvSource({ "0,0", "5000,5000", "'  250 ',250", "30s,30000", "2min,120000", "1h,3600000", "1d,86400000", "1.5s,1500", "500us,1",
      "0s,0", "7MS,7" })
  void statementTimeoutIsReadInMillisecondsByDefault(final String value, final long expected) {
    final PostgresSessionSettings settings = new PostgresSessionSettings();
    settings.set("statement_timeout", value);
    assertThat(settings.statementTimeoutMillis()).isEqualTo(expected);
  }

  @Test
  void statementTimeoutDefaultsToNoneAndResets() {
    final PostgresSessionSettings settings = new PostgresSessionSettings();
    assertThat(settings.statementTimeoutMillis()).isZero();
    assertThat(settings.show("statement_timeout")).isEqualTo("0");

    settings.set("statement_timeout", "45s");
    assertThat(settings.show("statement_timeout")).isEqualTo("45s");
    assertThat(settings.statementTimeoutMillis()).isEqualTo(45_000L);

    settings.set("statement_timeout", null);
    assertThat(settings.statementTimeoutMillis()).isZero();
    assertThat(settings.show("statement_timeout")).isEqualTo("0");
  }

  @Test
  void localStatementTimeoutEndsWithTheTransaction() {
    final PostgresSessionSettings settings = new PostgresSessionSettings();
    settings.set("statement_timeout", "1s", true);
    assertThat(settings.statementTimeoutMillis()).isEqualTo(1000L);
    settings.commit();
    assertThat(settings.statementTimeoutMillis()).isZero();
  }

  @ParameterizedTest
  @CsvSource({ "forever", "-5", "10 parsecs", "''" })
  void invalidStatementTimeoutIsRefused(final String value) {
    final PostgresSessionSettings settings = new PostgresSessionSettings();
    assertThatThrownBy(() -> settings.set("statement_timeout", value)).isInstanceOfSatisfying(
        PostgresSessionSettings.SettingException.class, e -> assertThat(e.sqlState).isEqualTo("22023"));
    assertThat(settings.statementTimeoutMillis()).isZero();
  }
}
