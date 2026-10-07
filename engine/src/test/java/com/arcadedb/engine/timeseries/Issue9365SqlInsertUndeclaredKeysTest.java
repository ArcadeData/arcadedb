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
package com.arcadedb.engine.timeseries;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9365 (follow-up to #8646): {@code INSERT INTO <timeseries type> SET ...} built the row from the DECLARED columns
 * only, so a misspelled tag was stored as an empty tag, under another series than the one sent, and answered as a
 * success. It is now governed by {@link GlobalConfiguration#TIMESERIES_UNDECLARED_KEYS} like the line protocol and gRPC
 * writes: {@code reject} (default) fails the statement naming the key, {@code ignore} stores the row without it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9365SqlInsertUndeclaredKeysTest extends TestHelper {

  @BeforeEach
  void createType() {
    database.command("sql", "CREATE TIMESERIES TYPE weather TIMESTAMP ts TAGS (city STRING) FIELDS (temp DOUBLE)");
  }

  @Test
  void aMisspelledTagFailsTheStatementAndStoresNothing() {
    assertThatThrownBy(() -> database.transaction(() -> database.command("sql",
        "INSERT INTO weather SET ts = 1700000000000, citty = 'rome', temp = 21.5")))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("citty").hasMessageContaining("weather");
    assertThat(count()).isZero();
  }

  @Test
  void anUndeclaredFieldFailsToo() {
    assertThatThrownBy(() -> database.transaction(() -> database.command("sql",
        "INSERT INTO weather SET ts = 1700000000000, city = 'rome', temp = 21.5, humidity = 40")))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("humidity");
    assertThat(count()).isZero();
  }

  @Test
  void aDeclaredRowStillInserts() {
    database.transaction(() -> database.command("sql", "INSERT INTO weather SET ts = 1700000000000, city = 'rome', temp = 21.5"));
    assertThat(count()).isEqualTo(1);
  }

  @Test
  void insertFromSelectAndContentAreCheckedAndInternalKeysAreNotProperties() {
    // CONTENT builds the document from a map: no internal @rid / @type key reaches the check
    database.transaction(() -> database.command("sql",
        "INSERT INTO weather CONTENT {\"ts\": 1700000000001, \"city\": \"oslo\", \"temp\": 2.5}"));
    assertThat(count()).isEqualTo(1);

    assertThatThrownBy(() -> database.transaction(() -> database.command("sql",
        "INSERT INTO weather CONTENT {\"ts\": 1700000000002, \"citty\": \"oslo\", \"temp\": 2.5}")))
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("citty");
    assertThat(count()).isEqualTo(1);
  }

  @Test
  void ignoreStoresTheRowWithoutTheUndeclaredKey() {
    database.getConfiguration().setValue(GlobalConfiguration.TIMESERIES_UNDECLARED_KEYS, "ignore");
    database.transaction(() -> database.command("sql",
        "INSERT INTO weather SET ts = 1700000000000, city = 'rome', temp = 21.5, extra = 1"));
    assertThat(count()).isEqualTo(1);
  }

  private long count() {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS n FROM weather")) {
      return rs.next().<Long>getProperty("n");
    }
  }
}
