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
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for https://github.com/ArcadeData/arcadedb/issues/3479
 * MATCH (n:NOT_EXISTING_LABEL) RETURN COUNT(n) should return 0, not error.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherCountNonExistingLabelTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testCountNonExistingLabel").create();
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void countOnNonExistingLabelShouldReturnZero() {
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher",
          "MATCH (n:NOT_EXISTING_LABEL) RETURN COUNT(n)");
      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      assertThat(result.<Long>getProperty("COUNT(n)")).isEqualTo(0L);
      assertThat(rs.hasNext()).isFalse();
      rs.close();
    });
  }

  @Test
  void countWithAliasOnNonExistingLabelShouldReturnZero() {
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher",
          "MATCH (n:NOT_EXISTING_LABEL) RETURN COUNT(n) AS cnt");
      assertThat(rs.hasNext()).isTrue();
      final Result result = rs.next();
      assertThat(result.<Long>getProperty("cnt")).isEqualTo(0L);
      assertThat(rs.hasNext()).isFalse();
      rs.close();
    });
  }

  @Test
  void matchOnNonExistingLabelShouldReturnEmpty() {
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher",
          "MATCH (n:NOT_EXISTING_LABEL) RETURN n");
      assertThat(rs.hasNext()).isFalse();
      rs.close();
    });
  }
}
