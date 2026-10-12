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

import com.arcadedb.TestHelper;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9806: on a database written before the native temporal types, a property READ restores a stored ISO text to its
 * temporal, but the comparison of an inline pattern property did not: a relationship pattern {@code -[r:R {d: duration(...)}]->}
 * matched nothing while {@code WHERE r.d = duration(...)} found the row. MERGE had the restore as a local copy, so a node
 * pattern and MERGE were the two shapes that passed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9806LegacyTemporalPatternTest extends TestHelper {

  @Test
  void everyPatternShapeFindsALegacyTemporalTextByItsTemporal() throws IOException {
    database.getSchema().createVertexType("T");
    database.getSchema().createEdgeType("R");
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("T").set("name", "a").save();
      final MutableVertex b = database.newVertex("T").set("name", "b").save();
      // what a release before the native temporal types stored for duration({days: 1})
      a.newEdge("R", b).set("d", "P1D").save();
    });
    makeLegacy();
    assertThat(database.getSchema().getEmbedded().isLegacyTemporalText()).isTrue();

    // the control: a WHERE reads the property, which restores it
    assertThat(names("MATCH (a:T)-[r:R]->(b:T) WHERE r.d = duration({days: 1}) RETURN b.name AS name")).containsExactly("b");

    assertThat(names("MATCH (a:T)-[r:R {d: duration({days: 1})}]->(b:T) RETURN b.name AS name")).containsExactly("b");
    assertThat(names("MATCH (a:T)-[:R*1..2 {d: duration({days: 1})}]->(b:T) RETURN b.name AS name")).containsExactly("b");
    assertThat(names("MATCH (a:T {name: 'a'}) WHERE (a)-[:R {d: duration({days: 1})}]->() RETURN a.name AS name")).containsExactly(
        "a");
    assertThat(names("MATCH (a:T {name: 'a'}) RETURN [(a)-[:R {d: duration({days: 1})}]->(b) | b.name][0] AS name"))
        .containsExactly("b");

    // a temporal among the identifying properties of merge.relationship finds the existing edge instead of adding one beside it
    database.transaction(() -> database.command("opencypher",
        "MATCH (a:T {name: 'a'}), (b:T {name: 'b'}) CALL merge.relationship(a, 'R', {d: duration({days: 1})}, {}, b) YIELD rel RETURN rel")
        .close());
    database.transaction(() -> database.command("opencypher",
        "MATCH (a:T {name: 'a'}), (b:T {name: 'b'}) MERGE (a)-[:R {d: duration({days: 1})}]->(b)").close());
    assertThat(names("MATCH ()-[r:R]->() RETURN r.d AS name")).hasSize(1);

    // the restore never applies to text the pattern does not compare against a temporal
    assertThat(names("MATCH (a:T)-[r:R {d: 'P1D'}]->(b:T) RETURN b.name AS name")).containsExactly("b");
  }

  @Test
  void aNativeDatabaseKeepsTheTextAsText() {
    database.getSchema().createVertexType("T");
    database.getSchema().createEdgeType("R");
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("T").set("name", "a").save();
      final MutableVertex b = database.newVertex("T").set("name", "b").save();
      a.newEdge("R", b).set("d", "P1D").save();
      a.newEdge("R", b).set("d", new CypherDuration(0, 1, 0, 0)).save();
    });

    // only the native duration is the duration: on a database created with the native types a text that looks like one is text
    assertThat(names("MATCH (a:T)-[r:R {d: duration({days: 1})}]->(b:T) RETURN b.name AS name")).hasSize(1);
  }

  private List<Object> names(final String query) {
    final List<Object> names = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query)) {
      while (rs.hasNext())
        names.add(rs.next().getProperty("name"));
    }
    return names;
  }

  /**
   * Closes the database and removes the temporal storage setting from its schema file, which is what a schema written by a
   * release before the native temporal types looks like, then opens it again.
   */
  private void makeLegacy() throws IOException {
    database.close();
    final File schemaFile = new File(getDatabasePath(), "schema.json");
    final JSONObject root = new JSONObject(Files.readString(schemaFile.toPath(), StandardCharsets.UTF_8));
    root.getJSONObject("settings").remove("temporalStorage");
    Files.writeString(schemaFile.toPath(), root.toString(), StandardCharsets.UTF_8);
    database = factory.open();
  }
}
