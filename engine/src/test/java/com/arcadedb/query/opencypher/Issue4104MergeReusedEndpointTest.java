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

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #4104.
 * <p>
 * When {@code MERGE} is run across multiple input rows with an unbound endpoint
 * pattern that has identical inline keys for every row, each row must MATCH or
 * CREATE its own endpoint - not collapse onto a node created by an earlier row
 * in the same statement.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4104MergeReusedEndpointTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/issue-4104-merge-reused-endpoint").create();
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:MergeP {name:'Alice', score:10.0})");
      database.command("opencypher", "CREATE (:MergeP {name:'Bob', score:20.0})");
      database.command("opencypher", "CREATE (:MergeP {name:'Charlie', score:30.0})");
    });
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void mergeCreatesOneEndpointPerUnmatchedRow() {
    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher",
          "MATCH (p:MergeP) "
              + "WITH p, p.score / 2.0 AS halfScore "
              + "MERGE (p)-[:FRIEND]->(f:MergeF {name:''}) "
              + "ON CREATE SET f.score = halfScore + 5.0 "
              + "RETURN p.name AS person_name, f.score AS friend_score "
              + "ORDER BY person_name");
      final List<Double> scores = new ArrayList<>();
      while (rs.hasNext()) {
        final Result r = rs.next();
        scores.add(r.<Number>getProperty("friend_score").doubleValue());
      }
      assertThat(scores).containsExactly(10.0, 15.0, 20.0);
    });

    final ResultSet count = database.query("opencypher",
        "MATCH (f:MergeF) RETURN count(f) AS friend_nodes");
    assertThat(count.hasNext()).isTrue();
    assertThat(count.next().<Number>getProperty("friend_nodes").longValue()).isEqualTo(3L);
  }

  @Test
  void singleRowMergeCreatesEndpoint() {
    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher",
          "MATCH (p:MergeP {name:'Alice'}) "
              + "WITH p, p.score / 2.0 AS halfScore "
              + "MERGE (p)-[:FRIEND]->(f:MergeF {name:''}) "
              + "ON CREATE SET f.score = halfScore + 5.0 "
              + "RETURN f.score AS friend_score");
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<Number>getProperty("friend_score").doubleValue()).isEqualTo(10.0);
    });
  }
}
