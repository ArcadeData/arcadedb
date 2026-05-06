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
 * Regression test for GitHub issue #4103.
 * <p>
 * In a multi-row pipeline, {@code MERGE ... ON MATCH SET} must materialize the
 * updated property value in every returned row, including the first one,
 * matching Neo4j.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4103MergeOnMatchStaleTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/issue-4103-merge-on-match").create();
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Traveler {name:'Alice'})");
      database.command("opencypher", "CREATE (:Town {name:'London', population: 8900000})");
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
  void mergeOnMatchSetPersistsForAllRows() {
    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher",
          "MERGE (p:Traveler {name:'Alice'}) "
              + "WITH p "
              + "UNWIND [1, 2, 3] AS i "
              + "MERGE (p)-[:VISITS]->(c:Town {name:'London'}) "
              + "ON MATCH SET c.population = 9000000 "
              + "RETURN i, c.population AS population "
              + "ORDER BY i");
      final List<Long> populations = new ArrayList<>();
      while (rs.hasNext()) {
        final Result r = rs.next();
        populations.add(r.<Number>getProperty("population").longValue());
      }
      assertThat(populations).containsExactly(9000000L, 9000000L, 9000000L);
    });
  }

  @Test
  void persistedValueIsCorrect() {
    database.transaction(() -> database.command("opencypher",
        "MERGE (p:Traveler {name:'Alice'}) "
            + "WITH p "
            + "UNWIND [1, 2, 3] AS i "
            + "MERGE (p)-[:VISITS]->(c:Town {name:'London'}) "
            + "ON MATCH SET c.population = 9000000"));

    final ResultSet rs = database.query("opencypher",
        "MATCH (:Traveler {name:'Alice'})-[:VISITS]->(c:Town {name:'London'}) RETURN c.population AS population");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("population").longValue()).isEqualTo(9000000L);
  }
}
