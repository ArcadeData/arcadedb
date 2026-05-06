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
 * Regression test for GitHub issue #4100.
 * <p>
 * A list subscript whose index is an inline aggregate expression must be
 * evaluated using the aggregate's actual value, matching Neo4j.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4100AggSubscriptTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/issue-4100-agg-subscript").create();
    database.transaction(() -> database.command("opencypher",
        "CREATE (:Person {name:'Alice', scores:[85,92,78]})"));
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void aggregateAsListSubscriptReturnsValue() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (p:Person {name:'Alice'}) "
            + "OPTIONAL MATCH (p)-[:HAS_FRIEND]->(f:Person) "
            + "WITH p, collect(f) AS friends "
            + "RETURN p.scores[toInteger(avg(size(friends)))] AS selectedScore");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("selectedScore").longValue()).isEqualTo(85L);
  }

  @Test
  void nonAggregateSubscriptControl() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (p:Person {name:'Alice'}) "
            + "OPTIONAL MATCH (p)-[:HAS_FRIEND]->(f:Person) "
            + "WITH p, collect(f) AS friends "
            + "RETURN p.scores[size(friends)] AS selectedScore");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("selectedScore").longValue()).isEqualTo(85L);
  }
}
