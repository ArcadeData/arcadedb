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
 * Regression test for GitHub issue #4107.
 * <p>
 * Inline aggregate expressions used as the list operand of {@code reduce(...)}
 * must be folded into the per-group aggregator stage, matching Neo4j.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4107ReduceOverAggregateTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/issue-4107-reduce-over-agg").create();
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Person {name:'Alice', age:30})");
      database.command("opencypher", "CREATE (:Person {name:'Bob', age:25})");
      database.command("opencypher", "CREATE (:Person {name:'Charlie', age:35})");
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
  void reduceOverInlineCollectPerGroup() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (p:Person) "
            + "RETURN p.name AS name, "
            + "       reduce(total = 0, n IN collect(p.age) | total + n) AS total_age_sum "
            + "ORDER BY name");
    final List<Long> values = new ArrayList<>();
    while (rs.hasNext()) {
      final Result r = rs.next();
      values.add(r.<Number>getProperty("total_age_sum").longValue());
    }
    assertThat(values).containsExactly(30L, 25L, 35L);
  }

  @Test
  void reduceOverInlineSumPerGroup() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (p:Person) "
            + "RETURN p.name AS name, "
            + "       reduce(total = 0, n IN [sum(p.age)] | total + n) AS s "
            + "ORDER BY name");
    final List<Long> values = new ArrayList<>();
    while (rs.hasNext()) {
      final Result r = rs.next();
      values.add(r.<Number>getProperty("s").longValue());
    }
    assertThat(values).containsExactly(30L, 25L, 35L);
  }

  @Test
  void reduceOverInlineCountStarPerGroup() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (p:Person) "
            + "RETURN p.name AS name, "
            + "       reduce(total = 0, n IN [count(*)] | total + n) AS s "
            + "ORDER BY name");
    final List<Long> values = new ArrayList<>();
    while (rs.hasNext()) {
      final Result r = rs.next();
      values.add(r.<Number>getProperty("s").longValue());
    }
    assertThat(values).containsExactly(1L, 1L, 1L);
  }

  @Test
  void reduceOverInlineCollectGlobal() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (p:Person) "
            + "RETURN reduce(total = 0, n IN collect(p.age) | total + n) AS total_age_sum");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("total_age_sum").longValue()).isEqualTo(90L);
  }
}
