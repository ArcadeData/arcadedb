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
 * Regression test for issue #6328: {@code REMOVE n.p} updated only the alias it was given, so a second alias of the
 * same node in the same row kept reporting the removed value - while {@code SET n.p = null}, the same write spelled
 * differently, answered null through both. Cypher has one binding per node per row, so both aliases must observe the
 * same state.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherRemovePropertyAliasIssue6328Test {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/cypher-remove-property-alias-6328");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.transaction(() -> database.command("opencypher", "CREATE (:P {name:'a', p:42})"));
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void removeIsObservedThroughEveryAliasOfTheSameNode() {
    final List<Result> rows = rows(
        "MATCH (n {name:'a'}) MATCH (m {name:'a'}) REMOVE n.p RETURN n.p AS throughN, m.p AS throughM");
    assertThat(rows).hasSize(1);
    assertThat((Object) rows.get(0).getProperty("throughN")).isNull();
    assertThat((Object) rows.get(0).getProperty("throughM")).isNull();
  }

  @Test
  void setToNullAnswersTheSameWay() {
    final List<Result> rows = rows(
        "MATCH (n {name:'a'}) MATCH (m {name:'a'}) SET n.p = null RETURN n.p AS throughN, m.p AS throughM");
    assertThat(rows).hasSize(1);
    assertThat((Object) rows.get(0).getProperty("throughN")).isNull();
    assertThat((Object) rows.get(0).getProperty("throughM")).isNull();
  }

  @Test
  void theSameNodeReachedAgainOnALaterRowObservesTheRemovalToo() {
    // UNWIND fans the same node out over two rows: the second one must not read the pre-removal snapshot.
    final List<Result> rows = rows(
        "MATCH (n {name:'a'}) UNWIND [1,2] AS i MATCH (m {name:'a'}) REMOVE n.p RETURN i, m.p AS throughM ORDER BY i");
    assertThat(rows).hasSize(2);
    for (final Result row : rows)
      assertThat((Object) row.getProperty("throughM")).isNull();
  }

  @Test
  void aRemovalIsStillPersisted() {
    rows("MATCH (n {name:'a'}) MATCH (m {name:'a'}) REMOVE n.p RETURN n.p");
    final List<Result> rows = rows("MATCH (n {name:'a'}) RETURN n.p AS p");
    assertThat(rows).hasSize(1);
    assertThat((Object) rows.get(0).getProperty("p")).isNull();
  }

  @Test
  void removingAPropertyThatIsNotThereLeavesTheOtherAliasesAlone() {
    final List<Result> rows = rows(
        "MATCH (n {name:'a'}) MATCH (m {name:'a'}) REMOVE n.missing RETURN m.p AS throughM");
    assertThat(rows).hasSize(1);
    assertThat(((Number) rows.get(0).getProperty("throughM")).intValue()).isEqualTo(42);
  }

  private List<Result> rows(final String query) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.command("opencypher", query)) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows;
  }
}
