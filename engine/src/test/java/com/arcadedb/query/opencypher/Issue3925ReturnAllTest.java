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
 * Regression test for GitHub issue #3925 - {@code RETURN ALL} keyword.
 * <p>
 * The Cypher 25 grammar (shared with Neo4j) defines the return body as
 * {@code (DISTINCT | ALL)? returnItems ...}, so {@code ALL} is a valid
 * set-quantifier per GQL and the openCypher reference grammar. ArcadeDB
 * accepts it as a no-op (ALL is the default, opposite of DISTINCT). Neo4j's
 * runtime validator rejects it today but that is a stricter choice than the
 * published grammar requires.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue3925ReturnAllTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/testopencypher-return-all-3925").create();
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void returnAllKeyword() {
    // ArcadeDB accepts RETURN ALL (GQL compliant) and preserves the alias.
    final ResultSet rs = database.query("opencypher", "RETURN ALL 1 AS result");
    assertThat(rs.hasNext()).isTrue();
    final Result r = rs.next();
    assertThat(r.getPropertyNames()).containsExactly("result");
    assertThat(r.<Long>getProperty("result")).isEqualTo(1L);
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void returnAllMultipleRowsNoDedup() {
    // ALL is the opposite of DISTINCT: duplicates must be preserved.
    final ResultSet rs = database.query("opencypher",
        "UNWIND [1, 1, 2] AS x RETURN ALL x AS result");
    int count = 0;
    while (rs.hasNext()) {
      rs.next();
      count++;
    }
    assertThat(count).isEqualTo(3);
  }
}
