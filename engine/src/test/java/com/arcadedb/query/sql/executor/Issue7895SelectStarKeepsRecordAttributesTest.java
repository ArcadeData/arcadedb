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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7895. The #7773 fix skipped {@code Result.toJSON()}'s element short-circuit whenever
 * {@code hasProjectedProperties()} answered true - but that predicate is {@code content != null && !content
 * .isEmpty()}, and a PLAIN {@code SELECT *} populates {@code content} as well. So a statement that reshapes
 * nothing took the new path too, and the property loop emitted only what {@code getPropertyNames()} lists:
 * {@code @cat} was lost on every row (the only thing in the JSON that says whether the row is a document, a
 * vertex or an edge), and an EDGE row additionally lost {@code @in} and {@code @out}, so the serialized edge no
 * longer said which vertices it connects and could not be reconstructed from its own JSON at all.
 * <p>
 * {@code SELECT FROM E1} still took the short-circuit and answered the full form, so two statements returning the
 * same rows serialized them differently.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7895SelectStarKeepsRecordAttributesTest {

  @Test
  void aPlainSelectStarSerializesExactlyAsTheRecordDoes() throws Exception {
    TestHelper.executeInNewDatabase("issue7895PlainStar", (db) -> {
      db.command("sql", "CREATE VERTEX TYPE V1");
      db.command("sql", "CREATE EDGE TYPE E1");
      db.command("sql", "CREATE DOCUMENT TYPE T");
      db.command("sql", "INSERT INTO T SET name = 'n1', n = 1");
      db.command("sql", "CREATE VERTEX V1 SET name = 'a'");
      db.command("sql", "CREATE VERTEX V1 SET name = 'b'");
      db.command("sql", "CREATE EDGE E1 FROM (SELECT FROM V1 WHERE name = 'a') TO (SELECT FROM V1 WHERE name = 'b') SET w = 5");

      for (final String type : new String[] { "T", "V1", "E1" }) {
        try (final ResultSet rs = db.query("sql", "SELECT * FROM " + type)) {
          final Result row = rs.next();
          final Document record = row.getElement().orElseThrow();

          // Compared as objects rather than as text: JSON key order is not part of what either side promises,
          // and the seed writes the structural attributes first where the record appends them last.
          assertThat(row.toJSON().toMap())
              .as("a row that reshapes nothing must serialize as the record it wraps (%s)", type)
              .containsExactlyInAnyOrderEntriesOf(record.toJSON().toMap());
        }
      }
    });
  }

  @Test
  void anEdgeRowKeepsTheVerticesItConnects() throws Exception {
    TestHelper.executeInNewDatabase("issue7895Edge", (db) -> {
      db.command("sql", "CREATE VERTEX TYPE V1");
      db.command("sql", "CREATE EDGE TYPE E1");
      db.command("sql", "CREATE VERTEX V1 SET name = 'a'");
      db.command("sql", "CREATE VERTEX V1 SET name = 'b'");
      db.command("sql", "CREATE EDGE E1 FROM (SELECT FROM V1 WHERE name = 'a') TO (SELECT FROM V1 WHERE name = 'b') SET w = 5");

      // The reshaped form too: #7773 needs the computed alias, #7895 needs the endpoints, and the endpoints are
      // not reachable as ordinary row properties - getPropertyNames() on such a row is [w, @rid, @type].
      try (final ResultSet rs = db.query("sql", "SELECT *, w+1 AS z FROM E1")) {
        final JSONObject json = rs.next().toJSON();
        assertThat(json.getInt("z")).as("the computed alias #7773 exists for").isEqualTo(6);
        assertThat(json.getString("@cat")).isEqualTo("e");
        assertThat(json.has("@in")).as("a serialized edge must say which vertices it connects").isTrue();
        assertThat(json.has("@out")).isTrue();
      }

      // And the plain form, through the asJSON() SQL method - the same path the #7773 fix's own last test uses.
      try (final ResultSet rs = db.query("sql", "SELECT $a.asJSON() AS j FROM V1 LET $a = (SELECT * FROM E1) LIMIT 1")) {
        assertThat(rs.next().getProperty("j").toString()).contains("\"@cat\":\"e\"", "@in", "@out");
      }
    });
  }

  @Test
  void aVertexRowKeepsItsCategory() throws Exception {
    TestHelper.executeInNewDatabase("issue7895Vertex", (db) -> {
      db.command("sql", "CREATE VERTEX TYPE V1");
      db.command("sql", "CREATE VERTEX V1 SET name = 'a'");

      try (final ResultSet rs = db.query("sql", "SELECT * FROM V1")) {
        final JSONObject json = rs.next().toJSON();
        assertThat(json.getString("@cat")).as("the only thing in the JSON that says what kind of row this is")
            .isEqualTo("v");
        assertThat(json.getString("name")).isEqualTo("a");
      }
    });
  }

  /**
   * Found in review: {@code DetachedDocument.toJSON(true)} / {@code toMap(true)} hardcoded {@code @cat = "d"},
   * so a DETACHED vertex or edge said it was a document - while {@code JsonSerializer.serializeResult} read its
   * schema type and said "v"/"e" for the same object, and so does the new seed. Two paths serializing the same
   * row differently is the whole of #7895, so the three are aligned rather than left to disagree.
   */
  @Test
  void aDetachedVertexOrEdgeKeepsItsOwnCategory() throws Exception {
    TestHelper.executeInNewDatabase("issue7895Detached", (db) -> {
      db.command("sql", "CREATE VERTEX TYPE V1");
      db.command("sql", "CREATE EDGE TYPE E1");
      db.command("sql", "CREATE DOCUMENT TYPE T");
      db.command("sql", "INSERT INTO T SET name = 'n1'");
      db.command("sql", "CREATE VERTEX V1 SET name = 'a'");
      db.command("sql", "CREATE VERTEX V1 SET name = 'b'");
      db.command("sql", "CREATE EDGE E1 FROM (SELECT FROM V1 WHERE name = 'a') TO (SELECT FROM V1 WHERE name = 'b') SET w = 5");

      for (final String[] step : new String[][] { { "V1", "v" }, { "E1", "e" }, { "T", "d" } }) {
        try (final ResultSet rs = db.query("sql", "SELECT FROM " + step[0])) {
          final Document detached = rs.next().getElement().orElseThrow().detach();
          assertThat(detached.toJSON(true).getString("@cat"))
              .as("a detached %s must not claim to be a document", step[0]).isEqualTo(step[1]);
          assertThat(detached.toMap(true).get("@cat")).isEqualTo(step[1]);
        }
      }
    });
  }

  /**
   * The seed is structural attributes only, never the record's properties: what #7773 established must not be
   * undone from the other side.
   */
  @Test
  void theSeedDoesNotResurrectAnExcludedColumnOrOverrideAComputedAlias() throws Exception {
    TestHelper.executeInNewDatabase("issue7895Exclusions", (db) -> {
      db.command("sql", "CREATE VERTEX TYPE V1");
      db.command("sql", "CREATE VERTEX V1 SET name = 'a', secret = 'hunter2', n = 1");

      try (final ResultSet rs = db.query("sql", "SELECT *, !secret FROM V1")) {
        final JSONObject json = rs.next().toJSON();
        assertThat(json.has("secret")).as("an excluded column must not leak back in through the seed").isFalse();
        assertThat(json.getString("@cat")).isEqualTo("v");
      }

      try (final ResultSet rs = db.query("sql", "SELECT *, n+1 AS n FROM V1")) {
        final JSONObject json = rs.next().toJSON();
        assertThat(json.getInt("n")).as("the computed alias still wins over the stored column").isEqualTo(2);
      }
    });
  }

  /**
   * Found in review: {@code SELECT *, !@rid} excludes a RECORD ATTRIBUTE, and the projection implements that by
   * never writing the value - not by {@code removeProperty()}, so no tombstone is recorded. The seed, which only
   * consulted tombstones, put {@code @rid} straight back, contradicting the statement and the seed's own javadoc.
   * The exclusion now travels with the row.
   */
  @Test
  void anExcludedRecordAttributeIsNotSeededBackIn() throws Exception {
    TestHelper.executeInNewDatabase("issue7895ExcludedAttributes", (db) -> {
      db.command("sql", "CREATE VERTEX TYPE V1");
      db.command("sql", "CREATE EDGE TYPE E1");
      db.command("sql", "CREATE VERTEX V1 SET name = 'a'");
      db.command("sql", "CREATE VERTEX V1 SET name = 'b'");
      db.command("sql", "CREATE EDGE E1 FROM (SELECT FROM V1 WHERE name = 'a') TO (SELECT FROM V1 WHERE name = 'b') SET w = 5");

      try (final ResultSet rs = db.query("sql", "SELECT *, !@rid FROM V1 LIMIT 1")) {
        final JSONObject json = rs.next().toJSON();
        assertThat(json.has("@rid")).as("the statement excluded @rid").isFalse();
        assertThat(json.getString("@type")).as("and only @rid").isEqualTo("V1");
        assertThat(json.getString("@cat")).isEqualTo("v");
      }

      try (final ResultSet rs = db.query("sql", "SELECT *, !@type FROM V1 LIMIT 1")) {
        final JSONObject json = rs.next().toJSON();
        assertThat(json.has("@type")).isFalse();
        assertThat(json.has("@rid")).isTrue();
      }

      // The shape DistinctExecutionStepTest uses: both attributes gone, the row's own columns kept.
      try (final ResultSet rs = db.query("sql", "SELECT *, !@rid, !@type FROM V1 LIMIT 1")) {
        final JSONObject json = rs.next().toJSON();
        assertThat(json.has("@rid")).isFalse();
        assertThat(json.has("@type")).isFalse();
        assertThat(json.getString("name")).isEqualTo("a");
      }

      // On an edge, where the seed also has @cat/@in/@out to write: only @rid goes, the rest stay. (@cat/@in/@out
      // are not excludable - the grammar's exclusion accepts @rid and @type and nothing else - so the seed is the
      // only thing that can write them, and the guard must not overreach and drop them too.)
      try (final ResultSet rs = db.query("sql", "SELECT *, !@rid FROM E1")) {
        final JSONObject json = rs.next().toJSON();
        assertThat(json.has("@rid")).isFalse();
        assertThat(json.getString("@cat")).isEqualTo("e");
        assertThat(json.has("@in")).isTrue();
        assertThat(json.has("@out")).isTrue();
        assertThat(json.getInt("w")).isEqualTo(5);
      }
    });
  }
}
