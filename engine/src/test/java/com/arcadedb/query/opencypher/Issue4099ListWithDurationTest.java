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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #4099.
 * <p>
 * A list literal that contains a {@code duration(...)} value must materialize
 * one row per outer row, not silently swallow them.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4099ListWithDurationTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/issue-4099-list-with-duration").create();
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  void listLiteralWithDurationProducesRow() {
    final ResultSet rs = database.query("opencypher", "RETURN [1, duration({months: 1000000})] AS x");
    assertThat(rs.hasNext()).isTrue();
    final List<Object> list = (List<Object>) rs.next().getProperty("x");
    assertThat(list).hasSize(2);
    assertThat(list.get(0)).isEqualTo(1L);
    assertThat(list.get(1)).isNotNull();
  }

  @Test
  void durationAloneProducesRow() {
    final ResultSet rs = database.query("opencypher", "RETURN duration({months: 1000000}) AS d");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Object>getProperty("d")).isNotNull();
  }

  @Test
  void unwindWithListLiteralContainingDurationProducesAllRows() {
    final ResultSet rs = database.query("opencypher",
        "UNWIND [1, 2, 3] AS num RETURN num, [1, duration({months: 1000000})] AS x");
    int count = 0;
    while (rs.hasNext()) {
      rs.next();
      count++;
    }
    assertThat(count).isEqualTo(3);
  }
}
