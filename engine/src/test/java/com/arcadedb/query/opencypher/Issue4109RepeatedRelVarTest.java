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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #4109.
 * <p>
 * A relationship variable repeated within a WHERE pattern requires both relationship
 * slots to bind to the same edge. The pattern {@code (p)-[r]->()<-[r]-()} cannot be
 * satisfied because that would require {@code r} to be both an outgoing and an
 * incoming edge from the same node simultaneously, which is impossible.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4109RepeatedRelVarTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/issue-4109-repeated-rel-var").create();
    database.transaction(() -> database.command("opencypher",
        "CREATE (:Person {name:'Alice'})-[:KNOWS]->(:Person {name:'Bob'})-[:KNOWS]->(:Person {name:'Charlie'})"));
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void repeatedRelVarInWhereIsUnsatisfiable() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (p:Person)-[r:KNOWS]-() WHERE (p)-[r:KNOWS]->()<-[r:KNOWS]-() RETURN count(*) AS cnt");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("cnt").longValue()).isEqualTo(0L);
  }

  @Test
  void repeatedRelVarUnderExistsAgrees() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (p:Person)-[r:KNOWS]-() WHERE EXISTS { MATCH (p)-[r:KNOWS]->()<-[r:KNOWS]-() } RETURN count(*) AS cnt");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("cnt").longValue()).isEqualTo(0L);
  }

  @Test
  void baselineMatchPersonRelMatchesFour() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (p:Person)-[r:KNOWS]-() RETURN count(*) AS cnt");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("cnt").longValue()).isEqualTo(4L);
  }
}
