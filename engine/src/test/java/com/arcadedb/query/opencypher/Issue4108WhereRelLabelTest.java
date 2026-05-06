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
 * Regression test for GitHub issue #4108.
 * <p>
 * In OpenCypher, the {@code variable:Label} predicate must work for relationship variables too.
 * For a bound relationship {@code r}, {@code WHERE r:KNOWS} is equivalent to
 * {@code WHERE type(r) = 'KNOWS'} (with multi-type OR support: {@code r:KNOWS|LIKES}).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4108WhereRelLabelTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/issue-4108-where-rel-label").create();
    database.transaction(() -> {
      database.command("opencypher", "CREATE (n1:Person {name:'Alice'}), (n2:Person {name:'Bob'}), (n3:Person {name:'Charlie'})");
      database.command("opencypher",
          "MATCH (n1:Person {name:'Alice'}), (n2:Person {name:'Bob'}) CREATE (n1)-[:KNOWS]->(n2)");
      database.command("opencypher",
          "MATCH (n2:Person {name:'Bob'}), (n3:Person {name:'Charlie'}) CREATE (n2)-[:KNOWS]->(n3)");
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
  void whereRelTypeOnBoundRelMatchesAll() {
    final ResultSet rs = database.query("opencypher", "MATCH (a)-[b]->(c) WHERE b:KNOWS RETURN count(b) AS cnt");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("cnt").longValue()).isEqualTo(2L);
  }

  @Test
  void whereRelTypeMismatchOnBoundRelReturnsZero() {
    final ResultSet rs = database.query("opencypher", "MATCH (a)-[b]->(c) WHERE b:NONEXISTENT RETURN count(b) AS cnt");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("cnt").longValue()).isEqualTo(0L);
  }

  @Test
  void whereRelTypeUnionOnBoundRelMatchesAny() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (a)-[b]->(c) WHERE b:KNOWS|LIKES RETURN count(b) AS cnt");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("cnt").longValue()).isEqualTo(2L);
  }

  @Test
  void whereNotRelTypeOnBoundRelMatchesNone() {
    final ResultSet rs = database.query("opencypher", "MATCH (a)-[b]->(c) WHERE NOT b:KNOWS RETURN count(b) AS cnt");
    assertThat(rs.hasNext()).isTrue();
    assertThat(rs.next().<Number>getProperty("cnt").longValue()).isEqualTo(0L);
  }
}
