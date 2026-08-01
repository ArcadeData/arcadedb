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
package com.arcadedb.integration.exporter;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.LightEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A lightweight edge has no record, so the record-based export loop cannot see it and every such edge used to be
 * dropped silently by an export/import cycle. Pins the full round trip: the storage declaration, the constraint, and
 * the edges themselves.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LightweightEdgeRoundTripIT {
  private static final String SOURCE_PATH = "target/databases/lightweight-roundtrip-source";
  private static final String TARGET_PATH = "target/databases/lightweight-roundtrip-target";
  private static final String FILE        = "target/lightweight-roundtrip.jsonl.tgz";

  @BeforeEach
  @AfterEach
  void clean() {
    TestHelper.checkActiveDatabases();
    FileUtils.deleteRecursively(new File(SOURCE_PATH));
    FileUtils.deleteRecursively(new File(TARGET_PATH));
    new File(FILE).delete();
  }

  @Test
  void lightweightEdgesSurviveAnExportImportCycle() throws Exception {
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.transaction(() -> {
        source.getSchema().buildVertexType().withName("Person").create();
        source.getSchema().buildEdgeType().withName("Follows").withLightweight(true).withUnique(true).create();
        source.getSchema().buildEdgeType().withName("Rated").create();

        final MutableVertex a = source.newVertex("Person").set("id", 0).save();
        final MutableVertex b = source.newVertex("Person").set("id", 1).save();
        final MutableVertex c = source.newVertex("Person").set("id", 2).save();

        a.newEdge("Follows", b);
        b.newEdge("Follows", c);
        // a regular edge alongside, to prove the two paths do not interfere
        a.newEdge("Rated", c, "stars", 5);
      });

      assertThat(edgesOf(source, 0)).containsExactlyInAnyOrder("Follows/light->1", "Rated/regular->2");
    }

    new Exporter(("-f " + FILE + " -d " + SOURCE_PATH + " -o -format jsonl").split(" ")).exportDatabase();
    assertThat(new File(FILE).exists()).isTrue();

    try (final Database target = new DatabaseFactory(TARGET_PATH).create()) {
      target.command("sql", "IMPORT DATABASE file://" + new File(FILE).getAbsolutePath());
    }

    try (final Database target = new DatabaseFactory(TARGET_PATH).open()) {
      final EdgeType follows = (EdgeType) target.getSchema().getType("Follows");
      assertThat(follows.isLightweight()).as("the storage declaration must survive").isTrue();
      assertThat(follows.isUnique()).as("the constraint must survive").isTrue();

      assertThat(edgesOf(target, 0)).containsExactlyInAnyOrder("Follows/light->1", "Rated/regular->2");
      assertThat(edgesOf(target, 1)).containsExactly("Follows/light->2");

      // still lightweight on the other side: no record was materialised for them
      assertThat(target.countType("Follows", false)).isZero();
      assertThat(target.countType("Rated", false)).isEqualTo(1);
    }
  }

  /** Outgoing edges of the Person with the given id, rendered as {@code type/shape->targetId}. */
  private List<String> edgesOf(final Database database, final int id) {
    final List<String> found = new ArrayList<>();
    database.transaction(() -> {
      final Vertex vertex = database.query("sql", "select from Person where id = ?", id).next().getVertex().get();
      for (final Edge edge : vertex.getEdges(Vertex.DIRECTION.OUT))
        found.add(edge.getTypeName() + (edge instanceof LightEdge ? "/light" : "/regular") + "->"
            + edge.getInVertex().get("id"));
    });
    return found;
  }
}
