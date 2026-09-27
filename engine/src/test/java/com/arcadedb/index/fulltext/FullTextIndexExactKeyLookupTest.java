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
package com.arcadedb.index.fulltext;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #8439: every exact lookup by property value must not answer from a FULL_TEXT index, which matches by analyzer
 * token ({@code 'a'} finds {@code 'a b'}) and finds nothing for a value without tokens ({@code '--'}), so filtering its
 * answer afterwards cannot bring the missing record back.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class FullTextIndexExactKeyLookupTest extends TestHelper {
  private static final String[] VALUES = { "a b", "a", "-a", "x:a", "--" };

  @BeforeEach
  void createSchema() {
    database.command("sql", "CREATE VERTEX TYPE Node");
    database.command("sql", "CREATE PROPERTY Node.k STRING");
    database.command("sql", "CREATE INDEX ON Node (k) FULL_TEXT");
    database.command("sql", "CREATE EDGE TYPE Link");
    database.transaction(() -> {
      for (final String v : VALUES)
        database.command("sql", "CREATE VERTEX Node SET k = ?", v);
    });
  }

  @Test
  void lookupByKeyRefusesAFullTextIndex() {
    assertThatThrownBy(() -> database.lookupByKey("Node", new String[] { "k" }, new Object[] { "a" }))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("FULL_TEXT");
    assertThatThrownBy(() -> database.lookupByKey("Node", "k", "a")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void newEdgeByKeysRefusesAFullTextIndexInsteadOfPickingTheWrongVertex() {
    database.transaction(() -> assertThatThrownBy(
        () -> database.newEdgeByKeys("Node", new String[] { "k" }, new Object[] { "a" }, "Node", new String[] { "k" },
            new Object[] { "a b" }, false, "Link", false)).isInstanceOf(IllegalArgumentException.class));
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Link")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isZero();
    }
  }

  @Test
  void cypherNodePropertyMapIsExact() {
    for (final String v : VALUES) {
      assertThat(count("MATCH (n:Node {k: $v}) RETURN n", v)).as("MATCH {k: '%s'}", v).isEqualTo(1);
      assertThat(count("MATCH (n:Node) WHERE n.k = $v RETURN n", v)).as("WHERE n.k = '%s'", v).isEqualTo(1);
    }
  }

  @Test
  void cypherMergeDoesNotDuplicateAValueWithoutTokens() {
    database.transaction(() -> {
      database.command("opencypher", "MERGE (n:Node {k: '--'})");
      database.command("opencypher", "MERGE (n:Node {k: 'a'})");
    });
    assertThat(count("MATCH (n:Node) RETURN n", null)).isEqualTo(VALUES.length);
  }

  @Test
  void javaSelectApiIsExact() {
    for (final String v : VALUES) {
      final List<String> found = new ArrayList<>();
      final Iterator<? extends Document> it = database.select().fromType("Node").where().property("k").eq().value(v).documents();
      while (it.hasNext())
        found.add(it.next().getString("k"));
      assertThat(found).as("select().eq('%s')", v).containsExactly(v);
    }
  }

  @Test
  void keyIndexStillAnswersLookupByKey() {
    database.command("sql", "CREATE PROPERTY Node.code STRING");
    database.command("sql", "CREATE INDEX ON Node (code) UNIQUE");
    database.transaction(() -> database.command("sql", "UPDATE Node SET code = k"));
    final Iterator<Identifiable> it = database.lookupByKey("Node", "code", "--");
    assertThat(it.hasNext()).isTrue();
    assertThat(it.next().asDocument().getString("k")).isEqualTo("--");
    assertThat(it.hasNext()).isFalse();
  }

  private long count(final String cypher, final String v) {
    try (final ResultSet rs = v == null ? database.query("opencypher", cypher) : database.query("opencypher", cypher, "v", v)) {
      return rs.stream().count();
    }
  }
}
