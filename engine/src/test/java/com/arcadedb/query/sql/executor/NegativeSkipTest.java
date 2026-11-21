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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A negative SKIP is refused, not read as "skip nothing". The planner already carried the message "Cannot execute a query with a
 * negative SKIP", but it threw it inside a try block that catches every exception (meant for a LIMIT or SKIP read from a
 * variable), so the statement ran as if there were no SKIP. openCypher refuses a negative SKIP too.
 */
class NegativeSkipTest extends TestHelper {

  @BeforeEach
  void createData() {
    database.getSchema().createDocumentType("NegSkip");
    database.transaction(() -> {
      for (int i = 1; i <= 3; i++)
        database.newDocument("NegSkip").set("id", i).save();
    });
  }

  private void drain(final String sql, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("sql", sql, params)) {
      while (rs.hasNext())
        rs.next();
    }
  }

  @Test
  void negativeSkipLiteralIsRefused() {
    assertThatThrownBy(() -> drain("SELECT id FROM NegSkip SKIP -1", Map.of())).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("negative SKIP");
  }

  @Test
  void negativeSkipWithOrderByIsRefused() {
    assertThatThrownBy(() -> drain("SELECT id FROM NegSkip ORDER BY id SKIP -1 LIMIT 2", Map.of())).isInstanceOf(
        CommandExecutionException.class).hasMessageContaining("negative SKIP");
  }

  @Test
  void negativeSkipParameterIsRefused() {
    assertThatThrownBy(() -> drain("SELECT id FROM NegSkip SKIP :n", Map.of("n", -5))).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("negative SKIP");
  }

  @Test
  void negativeSkipOnAnAggregateIsRefused() {
    assertThatThrownBy(() -> drain("SELECT count(*) FROM NegSkip SKIP -1", Map.of())).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("negative SKIP");
  }

  @Test
  void zeroAndPositiveSkipStillWork() {
    try (final ResultSet rs = database.query("sql", "SELECT id FROM NegSkip ORDER BY id SKIP 0")) {
      assertThat(rs.stream().count()).isEqualTo(3);
    }
    try (final ResultSet rs = database.query("sql", "SELECT id FROM NegSkip ORDER BY id SKIP 1")) {
      assertThat(rs.stream().map(r -> r.<Integer>getProperty("id")).toList()).containsExactly(2, 3);
    }
  }
}
