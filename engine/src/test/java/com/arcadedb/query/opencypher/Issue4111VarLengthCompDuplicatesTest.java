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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #4111.
 * <p>
 * In a variable-length pattern comprehension where both endpoints are already
 * bound by an outer clause, the projection list must contain one element per
 * matching path that actually reaches the bound end vertex. Paths that pass
 * through the end vertex but continue further must not produce duplicates.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4111VarLengthCompDuplicatesTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/issue-4111-varlength-comp").create();
    database.transaction(() -> database.command("opencypher",
        "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), (c:Person {name:'Charlie'}), "
            + "(a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c)"));
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  void varLengthCompBoundEndpointsNoDuplicates() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (p1:Person {name:'Alice'}), (p2:Person {name:'Bob'}) "
            + "RETURN [(p1)-[:KNOWS*1..3]->(p2) | 'X'] AS vals");
    assertThat(rs.hasNext()).isTrue();
    final List<Object> list = (List<Object>) rs.next().getProperty("vals");
    assertThat(list).containsExactly("X");
  }

  @SuppressWarnings("unchecked")
  @Test
  void varLengthCompFixedLengthControl() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (p1:Person {name:'Alice'}), (p2:Person {name:'Bob'}) "
            + "RETURN [(p1)-[:KNOWS]->(p2) | 'X'] AS vals");
    assertThat(rs.hasNext()).isTrue();
    final List<Object> list = (List<Object>) rs.next().getProperty("vals");
    assertThat(list).containsExactly("X");
  }

  @SuppressWarnings("unchecked")
  @Test
  void varLengthCompNoMatchReturnsEmpty() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (p1:Person {name:'Charlie'}), (p2:Person {name:'Bob'}) "
            + "RETURN [(p1)-[:KNOWS*1..3]->(p2) | 'X'] AS vals");
    assertThat(rs.hasNext()).isTrue();
    final List<Object> list = (List<Object>) rs.next().getProperty("vals");
    assertThat(list).isEmpty();
  }

  @SuppressWarnings("unchecked")
  @Test
  void varLengthCompProjectsBoundProperty() {
    final ResultSet rs = database.query("opencypher",
        "MATCH (p1:Person {name:'Alice'}), (p2:Person {name:'Bob'}) "
            + "RETURN [(p1)-[:KNOWS*1..3]->(p2) | p1.name] AS vals");
    assertThat(rs.hasNext()).isTrue();
    final List<Object> list = (List<Object>) rs.next().getProperty("vals");
    assertThat(list).containsExactly("Alice");
  }
}
