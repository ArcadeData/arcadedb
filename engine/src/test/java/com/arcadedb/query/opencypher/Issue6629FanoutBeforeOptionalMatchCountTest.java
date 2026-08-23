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
 * Regression tests for issue #6629: the OPTIONAL MATCH + WITH count() fast-path optimization
 * (CountEdgesStep) assumes the bound vertex appears exactly once per input row. Any preceding
 * clause that fans a single input row out into several rows for the same bound vertex breaks
 * that assumption, since CountEdgesStep emits one output row per input row instead of collapsing
 * into the WITH aggregation's group.
 * <p>
 * PR #6630 guards against UNWIND / LOAD CSV preceding the OPTIONAL MATCH. This test also covers
 * other row-fanout sources (cartesian-product MATCH, variable-length paths) that are not excluded
 * by that guard, to check whether the same bug survives through them.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6629FanoutBeforeOptionalMatchCountTest {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testissue6629fanout").create();
    database.transaction(() -> {
      database.getSchema().createVertexType("X");
      database.getSchema().createEdgeType("L");
      database.command("sql", "CREATE VERTEX X SET _id = 'x1'");
      database.command("sql", "CREATE VERTEX X SET _id = 'x2'");
      database.command("sql",
          "CREATE EDGE L FROM (SELECT FROM X WHERE _id = 'x1') TO (SELECT FROM X WHERE _id = 'x2')");
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void unwindBeforeOptionalMatchCollapsesToOneGroup() {
    final ResultSet result = database.query("opencypher",
        "MATCH (a:X {_id:'x1'}) UNWIND [1, 2] AS u OPTIONAL MATCH (a)-[:L]->(m:X) WITH a, count(m) AS c RETURN c");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat(row.<Long>getProperty("c")).isEqualTo(2L);
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void cartesianProductBeforeOptionalMatchCollapsesToOneGroup() {
    // (b) is an unrelated pattern that cartesian-products with (a), fanning out the input
    // stream to 2 rows per (a) before the OPTIONAL MATCH runs - the same fan-out shape as
    // UNWIND, just produced by a MATCH clause instead.
    final ResultSet result = database.query("opencypher",
        "MATCH (a:X {_id:'x1'}), (b:X) OPTIONAL MATCH (a)-[:L]->(m:X) WITH a, count(m) AS c RETURN c");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat(row.<Long>getProperty("c")).isEqualTo(2L);
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void zeroMatchesStillCollapseToOneGroupWithUnwind() {
    final ResultSet result = database.query("opencypher",
        "MATCH (a:X {_id:'x2'}) UNWIND [1, 2] AS u OPTIONAL MATCH (a)-[:L]->(m:X) WITH a, count(m) AS c RETURN c");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat(row.<Long>getProperty("c")).isEqualTo(0L);
    assertThat(result.hasNext()).isFalse();
    result.close();
  }

  @Test
  void controlWithoutUnwindStillOptimized() {
    final ResultSet result = database.query("opencypher",
        "MATCH (a:X {_id:'x1'}) OPTIONAL MATCH (a)-[:L]->(m:X) WITH a, count(m) AS c RETURN c");

    assertThat(result.hasNext()).isTrue();
    final Result row = result.next();
    assertThat(row.<Long>getProperty("c")).isEqualTo(1L);
    assertThat(result.hasNext()).isFalse();
    result.close();
  }
}
