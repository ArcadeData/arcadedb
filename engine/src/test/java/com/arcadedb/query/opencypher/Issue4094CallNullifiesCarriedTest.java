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
 * Regression test for GitHub issue #4094.
 * <p>
 * {@code CALL db.labels() YIELD label} must preserve previously bound
 * variables for every yielded row, matching Neo4j.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4094CallNullifiesCarriedTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/issue-4094-call-nullifies").create();
    database.transaction(() -> database.command("opencypher", "CREATE (:Foo), (:Bar), (:Baz)"));
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void callDbLabelsPreservesCarriedScalar() {
    final ResultSet rs = database.query("opencypher",
        "WITH 1 AS x CALL db.labels() YIELD label RETURN x, label ORDER BY label");
    int count = 0;
    while (rs.hasNext()) {
      final Result r = rs.next();
      assertThat(r.<Number>getProperty("x").longValue()).isEqualTo(1L);
      count++;
    }
    assertThat(count).isGreaterThan(0);
  }
}
