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
package com.arcadedb.query;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.IndexKeyEquality;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8920: a stored -0.0 is equal to 0.0 (IEEE 754, openCypher), whichever path answers: SQL or Cypher, `=` or `IN`, an
 * index seek or a scan. The `IN` / `NOT IN` pair must partition the type.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8920SignedZeroTest extends TestHelper {

  @Override
  public void beginTest() {
    database.transaction(() -> {
      for (final String type : new String[] { "I", "N", "H" }) {
        database.command("sql", "CREATE VERTEX TYPE " + type);
        database.command("sql", "CREATE PROPERTY " + type + ".id INTEGER");
        database.command("sql", "CREATE PROPERTY " + type + ".v DOUBLE");
      }
      database.command("sql", "CREATE INDEX ON I (v) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON H (v) NOTUNIQUE_HASH");
      for (final String type : new String[] { "I", "N", "H" }) {
        database.command("sql", "INSERT INTO " + type + " SET id = 1, v = 0.0");
        database.command("sql", "INSERT INTO " + type + " SET id = 2, v = 1.5");
        database.command("sql", "INSERT INTO " + type + " SET id = 3, v = ?", -0.0d);
        database.command("sql", "INSERT INTO " + type + " SET id = 4, v = 0.1");
      }
    });
  }

  @Test
  void sqlEqualityAgreesBetweenIndexAndScan() {
    for (final String type : new String[] { "I", "N", "H" }) {
      assertThat(sql("SELECT id FROM " + type + " WHERE v = 0.0")).as(type + " v = 0.0").containsExactly(1, 3);
      assertThat(sql("SELECT id FROM " + type + " WHERE v = -0.0")).as(type + " v = -0.0").containsExactly(1, 3);
      assertThat(sql("SELECT id FROM " + type + " WHERE v <> 0.0")).as(type + " v <> 0.0").containsExactly(2, 4);
    }
  }

  @Test
  void sqlInAndNotInPartitionTheType() {
    for (final String type : new String[] { "I", "N", "H" }) {
      assertThat(sql("SELECT id FROM " + type + " WHERE v IN [0.0]")).as(type + " IN").containsExactly(1, 3);
      assertThat(sql("SELECT id FROM " + type + " WHERE v IN [-0.0]")).as(type + " IN -0.0").containsExactly(1, 3);
      assertThat(sql("SELECT id FROM " + type + " WHERE v NOT IN [0.0]")).as(type + " NOT IN").containsExactly(2, 4);
      assertThat(sql("SELECT id FROM " + type + " WHERE v IN [0.0, 'x']")).as(type + " IN mixed").containsExactly(1, 3);
      assertThat(sqlParam("SELECT id FROM " + type + " WHERE v IN ?", List.of(0.0d))).as(type + " IN ?").containsExactly(1, 3);
    }
  }

  @Test
  void sqlOrderingTreatsTheZerosAsOneValue() {
    for (final String type : new String[] { "I", "N" }) {
      assertThat(sql("SELECT id FROM " + type + " WHERE v < 0.0")).as(type + " v < 0.0").isEmpty();
      assertThat(sql("SELECT id FROM " + type + " WHERE v <= 0.0")).as(type + " v <= 0.0").containsExactly(1, 3);
      assertThat(sql("SELECT id FROM " + type + " WHERE v >= 0.0")).as(type + " v >= 0.0").containsExactly(1, 2, 3, 4);
      assertThat(sql("SELECT id FROM " + type + " WHERE v > -0.0")).as(type + " v > -0.0").containsExactly(2, 4);
    }
  }

  @Test
  void cypherEqualityAgreesBetweenIndexAndScan() {
    for (final String type : new String[] { "I", "N", "H" }) {
      assertThat(cypher("MATCH (n:" + type + ") WHERE n.v = 0.0 RETURN n.id AS id")).as(type + " = 0.0").containsExactly(1, 3);
      assertThat(cypher("MATCH (n:" + type + ") WHERE n.v = -0.0 RETURN n.id AS id")).as(type + " = -0.0").containsExactly(1, 3);
      assertThat(cypher("MATCH (n:" + type + ") WHERE n.v IN [0.0] RETURN n.id AS id")).as(type + " IN").containsExactly(1, 3);
      assertThat(cypher("MATCH (n:" + type + ") WHERE NOT n.v IN [0.0] RETURN n.id AS id")).as(type + " NOT IN").containsExactly(2, 4);
      assertThat(cypher("MATCH (n:" + type + " {v: 0.0}) RETURN n.id AS id")).as(type + " {v: 0.0}").containsExactly(1, 3);
    }
  }

  @Test
  void uniqueIndexSeesTheCollision() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE U");
      database.command("sql", "CREATE PROPERTY U.v DOUBLE");
      database.command("sql", "CREATE INDEX ON U (v) UNIQUE");
      database.command("sql", "INSERT INTO U SET v = 0.0");
    });
    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO U SET v = ?", -0.0d)))
        .isInstanceOf(DuplicatedKeyException.class);
  }

  @Test
  void indexKeyEqualityTreatsTheZerosAsOneKey() {
    assertThat(IndexKeyEquality.sameTuple(new Object[] { 0.0d, "a" }, new Object[] { -0.0d, "a" })).isTrue();
    assertThat(IndexKeyEquality.sameTuple(new Object[] { 0.0f }, new Object[] { -0.0f })).isTrue();
    assertThat(IndexKeyEquality.sameTuple(new Object[] { 0.0d }, new Object[] { 1.0d })).isFalse();
    assertThat(IndexKeyEquality.hashTuple(new Object[] { 0.0d, "a" })).isEqualTo(IndexKeyEquality.hashTuple(new Object[] { -0.0d, "a" }));
    assertThat(IndexKeyEquality.hashTuple(new Object[] { 0.0f })).isEqualTo(IndexKeyEquality.hashTuple(new Object[] { -0.0f }));
    assertThat(IndexKeyEquality.hashTuple(new Object[] { new byte[] { 1, 2 } }))
        .isEqualTo(IndexKeyEquality.hashTuple(new Object[] { new byte[] { 1, 2 } }));
    assertThat(IndexKeyEquality.hashTuple(new Object[] { new byte[] { 1, 2 } })).isEqualTo(java.util.Arrays.deepHashCode(new Object[] { new byte[] { 1, 2 } }));
  }

  @Test
  void zerosStayEqualAfterTheIndexIsCompacted() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE C");
      database.command("sql", "CREATE PROPERTY C.id INTEGER");
      database.command("sql", "CREATE PROPERTY C.v DOUBLE");
      database.command("sql", "CREATE INDEX ON C (v) NOTUNIQUE");
      database.command("sql", "INSERT INTO C SET id = 1, v = 0.0");
      database.command("sql", "INSERT INTO C SET id = 2, v = ?", -0.0d);
    });
    for (final var index : database.getSchema().getType("C").getAllIndexes(false))
      try {
        ((com.arcadedb.index.IndexInternal) index).compact();
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    assertThat(sql("SELECT id FROM C WHERE v = 0.0")).containsExactly(1, 2);
    assertThat(sql("SELECT id FROM C WHERE v = -0.0")).containsExactly(1, 2);
  }

  private List<Integer> sql(final String statement) {
    return ids(database.query("sql", statement + " ORDER BY id"));
  }

  private List<Integer> sqlParam(final String statement, final Object param) {
    return ids(database.query("sql", statement + " ORDER BY id", param));
  }

  private List<Integer> cypher(final String statement) {
    return ids(database.query("opencypher", statement + " ORDER BY id", Map.of()));
  }

  private static List<Integer> ids(final ResultSet rs) {
    final List<Integer> ids = new ArrayList<>();
    try (rs) {
      while (rs.hasNext())
        ids.add(rs.next().<Number>getProperty("id").intValue());
    }
    return ids;
  }
}
