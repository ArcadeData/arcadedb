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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * https://github.com/ArcadeData/arcadedb/issues/10923
 * <p>
 * {@code DROP TYPE} asked whether an edge type held any RECORD. A lightweight edge holds none, so the drop was accepted
 * while every edge was live in the vertices' adjacency lists, leaving entries no surface could read or delete.
 */
class Issue10923DropTypeLightweightEdgesTest extends TestHelper {

  private MutableVertex[] createVerticesAndSchema(final String edgeDdl) {
    database.command("sql", "CREATE VERTEX TYPE V");
    database.command("sql", edgeDdl);
    final MutableVertex[] vertices = new MutableVertex[2];
    database.transaction(() -> {
      vertices[0] = database.newVertex("V").save();
      vertices[1] = database.newVertex("V").save();
    });
    return vertices;
  }

  @Test
  void dropOfLightweightEdgeTypeWithEdgesIsRefused() {
    final MutableVertex[] v = createVerticesAndSchema("CREATE EDGE TYPE EL LIGHTWEIGHT");
    database.transaction(() -> v[0].newLightEdge("EL", v[1]));

    assertThatThrownBy(() -> database.command("sql", "DROP TYPE EL")).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("contains Edges");

    assertThat(database.getSchema().existsType("EL")).isTrue();
    assertThat(v[0].countEdges(Vertex.DIRECTION.OUT, "EL")).isEqualTo(1);
  }

  @Test
  void dropOfUnidirectionalLightweightEdgeTypeWithEdgesIsRefused() {
    final MutableVertex[] v = createVerticesAndSchema("CREATE EDGE TYPE EUL UNIDIRECTIONAL LIGHTWEIGHT");
    database.transaction(() -> v[0].newLightEdge("EUL", v[1]));

    assertThatThrownBy(() -> database.command("sql", "DROP TYPE EUL")).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("contains Edges");
    assertThat(database.getSchema().existsType("EUL")).isTrue();
  }

  @Test
  void dropOfPlainEdgeTypeHoldingLightEdgesIsRefused() {
    final MutableVertex[] v = createVerticesAndSchema("CREATE EDGE TYPE E");
    database.transaction(() -> v[0].newLightEdge("E", v[1]));

    assertThatThrownBy(() -> database.command("sql", "DROP TYPE E")).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("contains Edges");
    assertThat(database.getSchema().existsType("E")).isTrue();
  }

  @Test
  void unsafeStillDropsAnEdgeTypeWithLightEdges() {
    final MutableVertex[] v = createVerticesAndSchema("CREATE EDGE TYPE EL LIGHTWEIGHT");
    database.transaction(() -> v[0].newLightEdge("EL", v[1]));

    database.command("sql", "DROP TYPE EL UNSAFE");
    assertThat(database.getSchema().existsType("EL")).isFalse();
  }

  @Test
  void emptyLightweightEdgeTypeCanBeDropped() {
    createVerticesAndSchema("CREATE EDGE TYPE EL LIGHTWEIGHT");
    database.command("sql", "DROP TYPE EL");
    assertThat(database.getSchema().existsType("EL")).isFalse();
  }

  @Test
  void dropSucceedsOnceTheLightEdgesAreDeleted() {
    final MutableVertex[] v = createVerticesAndSchema("CREATE EDGE TYPE EL LIGHTWEIGHT");
    database.transaction(() -> v[0].newLightEdge("EL", v[1]));

    database.transaction(() -> database.command("sql", "DELETE FROM EL"));
    database.command("sql", "DROP TYPE EL");
    assertThat(database.getSchema().existsType("EL")).isFalse();
  }

  @Test
  void edgeTypeOnlyBlockedByItsOwnEdges() {
    final MutableVertex[] v = createVerticesAndSchema("CREATE EDGE TYPE EL LIGHTWEIGHT");
    database.command("sql", "CREATE EDGE TYPE EOTHER LIGHTWEIGHT");
    database.transaction(() -> v[0].newLightEdge("EL", v[1]));

    database.command("sql", "DROP TYPE EOTHER");
    assertThat(database.getSchema().existsType("EOTHER")).isFalse();
  }

  @Test
  void lightEdgeOnAVertexOfASubtypeIsFound() {
    database.command("sql", "CREATE VERTEX TYPE V");
    database.command("sql", "CREATE VERTEX TYPE SubV EXTENDS V");
    database.command("sql", "CREATE EDGE TYPE EL LIGHTWEIGHT");
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("SubV").save();
      final MutableVertex b = database.newVertex("SubV").save();
      a.newLightEdge("EL", b);
    });

    assertThatThrownBy(() -> database.command("sql", "DROP TYPE EL")).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("contains Edges");
  }

  @Test
  void dropOfEdgeTypeWithRecordEdgesIsRefused() {
    final MutableVertex[] v = createVerticesAndSchema("CREATE EDGE TYPE ER");
    database.transaction(() -> v[0].newEdge("ER", v[1]));

    assertThatThrownBy(() -> database.command("sql", "DROP TYPE ER")).isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("contains Edges");
  }
}
