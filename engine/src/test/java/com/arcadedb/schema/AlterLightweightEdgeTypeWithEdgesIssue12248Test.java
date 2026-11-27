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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #12248: {@code ALTER TYPE L WITH lightweight = false} was accepted on a LIGHTWEIGHT edge type
 * that held edges, after which {@code SELECT count(*) FROM L}, {@code DELETE FROM L} and {@code TRUNCATE TYPE L} missed them
 * while traversals still returned them; and {@code ALTER TYPE L WITH unique = true} was accepted while a pair of vertices
 * held two of its edges, declaring a constraint the data violated. Both are refused now, as {@code lightweight = true} on a
 * type with edge records and {@code unique = true} on a record-backed type with a duplicate are.
 */
class AlterLightweightEdgeTypeWithEdgesIssue12248Test extends TestHelper {
  private MutableVertex a, b, c;

  private void createGraph(final boolean duplicate) {
    database.getSchema().createVertexType("V");
    database.getSchema().buildEdgeType().withName("L").withLightweight(true).create();
    database.transaction(() -> {
      a = database.newVertex("V").save();
      b = database.newVertex("V").save();
      c = database.newVertex("V").save();
      a.newEdge("L", b);
      a.newEdge("L", c);
      if (duplicate)
        a.newEdge("L", b);
    });
  }

  private long countL() {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS n FROM L")) {
      return rs.next().<Long>getProperty("n");
    }
  }

  private EdgeType typeL() {
    return database.getSchema().getType("L") instanceof EdgeType edgeType ? edgeType : null;
  }

  @Test
  void lightweightFalseIsRefusedWhileTheTypeHoldsEdges() {
    createGraph(false);
    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE L WITH lightweight = false"))
        .isInstanceOf(SchemaException.class).hasMessageContaining("lightweight edge");
    assertThat(typeL().isLightweight()).isTrue();
    assertThat(countL()).isEqualTo(2L);

    assertThatThrownBy(() -> ((LocalEdgeType) typeL()).setLightweight(false)).isInstanceOf(SchemaException.class);
    assertThat(typeL().isLightweight()).isTrue();
  }

  @Test
  void lightweightFalseIsAcceptedOnceTheEdgesAreDeleted() {
    createGraph(false);
    database.transaction(() -> database.command("sql", "DELETE FROM L"));
    assertThat(countL()).isZero();
    database.command("sql", "ALTER TYPE L WITH lightweight = false");
    assertThat(typeL().isLightweight()).isFalse();
  }

  @Test
  void lightweightFalseIsAcceptedOnAnEmptyType() {
    database.getSchema().buildEdgeType().withName("L").withLightweight(true).create();
    database.command("sql", "ALTER TYPE L WITH lightweight = false");
    assertThat(typeL().isLightweight()).isFalse();
  }

  @Test
  void uniqueIsRefusedWhileTwoEdgesJoinTheSamePair() {
    createGraph(true);
    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE L WITH unique = true"))
        .isInstanceOf(SchemaException.class).hasMessageContaining(a.getIdentity().toString());
    assertThat(typeL().isUnique()).isFalse();
    // still not enforced: the refused ALTER left the type as it was
    database.transaction(() -> a.newEdge("L", c));
  }

  @Test
  void combinedAlterIsRefusedAndLeavesBothFlagsAsTheyWere() {
    createGraph(false);
    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE L WITH lightweight = false, unique = true"))
        .isInstanceOf(SchemaException.class).hasMessageContaining("lightweight edge");
    assertThat(typeL().isLightweight()).isTrue();
    assertThat(typeL().isUnique()).isFalse();
    assertThat(database.getSchema().existsIndex(LocalEdgeType.uniqueIndexName("L"))).isFalse();
    assertThat(countL()).isEqualTo(2L);
    // still not enforced: the refused ALTER left the type as it was
    database.transaction(() -> a.newEdge("L", b));
  }

  @Test
  void uniqueIsRefusedThroughTheJavaApiToo() {
    createGraph(true);
    assertThatThrownBy(() -> ((LocalEdgeType) typeL()).setUnique(true)).isInstanceOf(SchemaException.class)
        .hasMessageContaining("UNIQUE");
    assertThat(typeL().isUnique()).isFalse();
  }

  @Test
  void edgesOfAnotherLightweightTypeDoNotBlockTheAlter() {
    createGraph(true);
    database.getSchema().buildEdgeType().withName("Other").withLightweight(true).create();
    database.transaction(() -> database.command("sql", "DELETE FROM L"));
    database.transaction(() -> {
      a.newEdge("Other", b);
      a.newEdge("Other", b);
    });
    database.command("sql", "ALTER TYPE L WITH unique = true");
    assertThat(typeL().isUnique()).isTrue();
    database.command("sql", "ALTER TYPE L WITH unique = false");
    database.command("sql", "ALTER TYPE L WITH lightweight = false");
    assertThat(typeL().isLightweight()).isFalse();
  }

  @Test
  void uniqueIsAcceptedWhenEveryPairIsDistinctAndThenEnforced() {
    createGraph(false);
    database.command("sql", "ALTER TYPE L WITH unique = true");
    assertThat(typeL().isUnique()).isTrue();
    assertThatThrownBy(() -> database.transaction(() -> a.newEdge("L", b))).isInstanceOf(DuplicatedKeyException.class);
  }
}
