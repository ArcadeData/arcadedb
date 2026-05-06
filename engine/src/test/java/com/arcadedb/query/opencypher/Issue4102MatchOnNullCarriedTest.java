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
 * Regression test for GitHub issue #4102.
 * <p>
 * When a variable is bound to {@code null} via {@code OPTIONAL MATCH} and
 * carried through {@code WITH}, a subsequent {@code MATCH} that references
 * that same variable must filter the row out (the row cannot satisfy a fresh
 * MATCH if the variable is already null), matching Neo4j.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4102MatchOnNullCarriedTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/issue-4102-match-null-carried").create();
    database.transaction(() -> database.command("opencypher",
        "CREATE (:Person {name:'Alice', city:'New York'}), (:City {name:'New York', population:8000000})"));
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void matchOnNullCarriedVariableEliminatesRow() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (p:Person {city:'New York'}) "
            + "OPTIONAL MATCH (p)-[:LIVES_IN]->(c:City) "
            + "WITH p, c "
            + "MATCH (c:City {population:8000000}) "
            + "RETURN p.name AS p, c.name AS c");
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void matchOnNullCarriedRelationshipEliminatesRow() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (p:Person {city:'New York'}) "
            + "OPTIONAL MATCH (p)-[r:LIVES_IN]->(c:City) "
            + "WITH p, c, r "
            + "MATCH (c:City {population:8000000}) "
            + "RETURN p.name AS p, c.name AS c");
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void optionalMatchAloneStillReturnsRowWithNull() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (p:Person {city:'New York'}) "
            + "OPTIONAL MATCH (p)-[:LIVES_IN]->(c:City) "
            + "WITH p, c "
            + "RETURN p.name AS p, c.name AS c");
    assertThat(rs.hasNext()).isTrue();
    final var r = rs.next();
    assertThat(r.<String>getProperty("p")).isEqualTo("Alice");
    assertThat(r.<String>getProperty("c")).isNull();
    assertThat(rs.hasNext()).isFalse();
  }
}
