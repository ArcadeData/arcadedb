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
import com.arcadedb.database.CountingImmutableDocument;
import com.arcadedb.database.Document;
import com.arcadedb.database.ImmutableDocument;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.query.sql.parser.SuffixIdentifier;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * SQL projections: which columns a projected row carries, how a nested projection selects them, how a row reads and
 * serializes the record it wraps, and how a projected value is stored.
 */
class ProjectionExecutionTest extends TestHelper {

  // ---- #5756: no column push-down on a filtered scan ----
  /*
   * Issue #5756 - the column projection push-down that {@code ScanWithFilterStep} used to carry was unreachable by
   * construction: the planner computed the projected column set only for a statement with no WHERE clause, while the
   * filtered scan exists only because the statement has one. It has been removed.
   * <p>
   * These tests pin the contract a future column push-down would have to honour, because the removed one did not:
   * it emitted a row carrying the projected columns as content <em>and</em> the record as element, and
   * {@link ResultInternal} reads those two stores with different precedences. A column left out of the projected set
   * therefore answered {@code null} to {@code getProperty()} while {@code hasProperty()} still reported {@code true} -
   * so every downstream reader of a column the SELECT does not name (a per-record LET, an ORDER BY alias, a subquery)
   * would have silently read {@code null} instead of the stored value.
   */

  private static final int RECORD_COUNT = 40;

  private void createWideDocuments() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Wide");
      for (int i = 0; i < RECORD_COUNT; i++) {
        final MutableDocument doc = database.newDocument("Wide");
        doc.set("id", i);
        doc.set("name", "n" + i);
        doc.set("status", i % 2 == 0 ? "active" : "closed");
        doc.set("amount", i * 1.5);
        doc.set("note", "note_" + i);
        doc.save();
      }
    });
  }

  /**
   * Regression test for issue #5756.
   * <p>
   * The filtered scan is chosen exactly when there is a WHERE clause, so no plan can ever advertise a column
   * push-down on it. Before the fix the marker was emitted by dead code that no query shape could reach; after it,
   * the concept is gone. Either way the plan must never claim to project columns at the scan.
   */
  @Test
  void noFilteredScanPlanAdvertisesAColumnPushdown() {
    createWideDocuments();
    final List<String> queries = List.of(
        "SELECT name, amount FROM Wide WHERE status = 'closed'",
        "SELECT name FROM Wide WHERE id > 30",
        "SELECT name FROM Wide WHERE id > 30 ORDER BY amount DESC",
        "SELECT name, amount FROM Wide WHERE name IS NOT NULL AND id > 30",
        "SELECT status FROM Wide WHERE id > 2 GROUP BY status",
        "SELECT name, $a AS a FROM Wide LET $a = amount WHERE id > 30");

    for (final String query : queries) {
      final String plan = explain(query);
      assertThat(plan).as("filtered scan expected for: %s", query).contains("SCAN WITH FILTER");
      assertThat(plan).as("no column push-down may be advertised for: %s", query).doesNotContain("[projected:");
    }
  }

  /**
   * Regression test for issue #5756.
   * <p>
   * A narrow SELECT over a filtered scan returns exactly the requested columns, and the column the filter ran on is
   * not smuggled into the row through any accessor - not {@code getPropertyNames()}, not {@code hasProperty()}, and
   * not the defaulting {@code getProperty(name, default)} overload that reads the backing element.
   */
  @Test
  void narrowProjectionOverAFilteredScanReturnsExactlyTheSelectedColumns() {
    createWideDocuments();
    final ResultSet rs = database.query("SQL", "SELECT name, amount FROM Wide WHERE status = 'closed'");

    int count = 0;
    while (rs.hasNext()) {
      final Result row = rs.next();
      assertThat(row.getPropertyNames()).containsExactlyInAnyOrder("name", "amount");
      assertThat(row.<String>getProperty("name")).startsWith("n");
      assertThat(row.<Double>getProperty("amount")).isNotNull();

      assertThat(row.hasProperty("status")).isFalse();
      assertThat(row.<Object>getProperty("status")).isNull();
      assertThat(row.<Object>getProperty("status", "RESURRECTED")).isEqualTo("RESURRECTED");
      assertThat(row.toMap()).doesNotContainKeys("status", "note");
      count++;
    }
    assertThat(count).isEqualTo(RECORD_COUNT / 2);
  }

  /**
   * Regression test for issue #5756.
   * <p>
   * The decisive guard. {@code amount} is read by a per-record LET, downstream of the filtered scan and outside the
   * SELECT list. Under the removed push-down the scan would have emitted a row whose content held only
   * {@code name}, and {@code $a} would have evaluated to null on every row while the query still looked healthy.
   */
  @Test
  void aColumnOutsideTheSelectListIsStillReadableDownstreamOfAFilteredScan() {
    createWideDocuments();
    final ResultSet rs = database.query("SQL", "SELECT name, $a AS a FROM Wide LET $a = amount WHERE id > 30");

    final List<Double> amounts = new ArrayList<>();
    while (rs.hasNext()) {
      final Result row = rs.next();
      final Double a = row.getProperty("a");
      assertThat(a).as("LET over a filtered scan must read the stored value, not null").isNotNull();
      amounts.add(a);
    }
    assertThat(amounts).hasSize(RECORD_COUNT - 31);
    assertThat(amounts).containsExactly(31 * 1.5, 32 * 1.5, 33 * 1.5, 34 * 1.5, 35 * 1.5, 36 * 1.5, 37 * 1.5, 38 * 1.5,
        39 * 1.5);
  }

  /**
   * Regression test for issue #5756.
   * <p>
   * Same guard on the sort path: {@code note} is neither selected nor filtered on, and the ORDER BY has to see its
   * real values on rows coming out of the filtered scan.
   */
  @Test
  void orderByAColumnOutsideTheSelectListOverAFilteredScan() {
    createWideDocuments();
    final ResultSet rs = database.query("SQL", "SELECT name FROM Wide WHERE status = 'active' ORDER BY amount DESC LIMIT 3");

    final List<String> names = new ArrayList<>();
    while (rs.hasNext())
      names.add(rs.next().getProperty("name"));

    assertThat(names).containsExactly("n38", "n36", "n34");
  }

  /**
   * Regression test for issue #5756.
   * <p>
   * A record missing one of the projected columns still survives the filter and reports the column as absent rather
   * than as a value borrowed from another record.
   */
  @Test
  void aRecordMissingAProjectedColumnIsReportedAsAbsent() {
    createWideDocuments();
    database.transaction(() -> {
      final MutableDocument sparse = database.newDocument("Wide");
      sparse.set("id", 100);
      sparse.set("status", "closed");
      sparse.save();
    });

    final ResultSet rs = database.query("SQL", "SELECT name, amount FROM Wide WHERE id = 100");
    assertThat(rs.hasNext()).isTrue();
    final Result row = rs.next();
    assertThat(row.<Object>getProperty("name")).isNull();
    assertThat(row.<Object>getProperty("amount")).isNull();
    assertThat(rs.hasNext()).isFalse();
  }

  private String explain(final String query) {
    final ResultSet rs = database.query("SQL", "EXPLAIN " + query);
    assertThat(rs.hasNext()).isTrue();
    return rs.next().getProperty("executionPlanAsString");
  }

  // ---- #7895: SELECT * keeps the record attributes in toJSON() ----
  /*
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
   */

  /** Regression test for issue #7895. */
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

  /** Regression test for issue #7895. */
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

  /** Regression test for issue #7895. */
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
   * Regression test for issue #7895.
   * <p>
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
   * Regression test for issue #7895.
   * <p>
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
   * Regression test for issue #7895.
   * <p>
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

  // ---- #8266: single-pass property read ----
  /*
   * Issue #8266: {@code SuffixIdentifier.execute()} resolved every property through {@code hasProperty()} and then
   * {@code getProperty()}, two passes over the record for every property an expression read. It now asks once, through
   * {@link Result#getPropertyIfPresent(String, Object)}, which must answer exactly what the pair answered: the value when
   * the property is present (null included), the caller's absent marker when it is not.
   */

  private static final Object ABSENT = new Object();

  private void createSinglePassReadTypes() {
    database.getSchema().createDocumentType("Doc8266");
    database.getSchema().createVertexType("V8266");
    database.getSchema().createEdgeType("E8266");
  }

  /** Regression test for issue #8266. */
  @Test
  void suffixIdentifierReadsThePropertyInOnePass() {
    createSinglePassReadTypes();
    final CountingResult row = new CountingResult(new ResultInternal(Map.of("name", "Jay")));

    assertThat(new SuffixIdentifier(new Identifier("name")).execute(row, null)).isEqualTo("Jay");
    assertThat(row.hasPropertyCalls).isZero();
    assertThat(row.getPropertyCalls).isZero();
    assertThat(row.ifPresentCalls).isEqualTo(1);
  }

  /** Regression test for issue #8266. */
  @Test
  void resultWithoutItsOwnImplementationKeepsTheHasThenGetAnswer() {
    createSinglePassReadTypes();
    // EmptyResult implements Result directly, as an embedder's own Result would, so it gets the has()-then-get() default
    final Result row = new EmptyResult();
    assertThat(row.getPropertyIfPresent("missing", ABSENT)).isSameAs(ABSENT);
  }

  /** Regression test for issue #8266. */
  @Test
  void immutableDocumentTellsAbsentFromNull() {
    createSinglePassReadTypes();
    final RID[] ridHolder = new RID[1];
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Doc8266");
      doc.set("name", "Jay");
      doc.set("nothing", null);
      doc.newEmbeddedDocument("Doc8266", "embedded").set("inner", 1);
      ridHolder[0] = doc.save().getIdentity();
    });
    final RID rid = ridHolder[0];

    // FRESH LAZY SHELL, SO THE SINGLE PASS ALSO HAS TO LOAD THE CONTENT
    final Document doc = (Document) database.lookupByRID(rid, false);
    assertThat(doc).isInstanceOf(ImmutableDocument.class);
    assertPresence(doc);
    assertThat(((Document) doc.getIfPresent("embedded", ABSENT)).getIfPresent("inner", ABSENT)).isEqualTo(1);
    assertThat(((Document) doc.getIfPresent("embedded", ABSENT)).getIfPresent("outer", ABSENT)).isSameAs(ABSENT);
    // A NAME THE DICTIONARY HAS NEVER SEEN CANNOT BE STORED IN ANY RECORD
    assertThat(doc.getIfPresent("neverSeenAnywhere8266", ABSENT)).isSameAs(ABSENT);
    assertThat(doc.getIfPresent(null, ABSENT)).isSameAs(ABSENT);

    database.transaction(() -> assertPresence(doc.modify()));
  }

  /** Regression test for issue #8266. */
  @Test
  void graphRecordsTellAbsentFromNull() {
    createSinglePassReadTypes();
    final RID[] rids = new RID[2];
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("V8266").set("name", "Jay").set("nothing", null).save();
      final MutableVertex b = database.newVertex("V8266").set("name", "Kay").save();
      final Edge e = a.newEdge("E8266", b, "name", "Jay", "nothing", null);
      rids[0] = a.getIdentity();
      rids[1] = e.getIdentity();
    });

    final Vertex v = (Vertex) database.lookupByRID(rids[0], false);
    assertPresence(v);
    final Edge e = (Edge) database.lookupByRID(rids[1], true);
    assertPresence(e);
    // @out/@in ARE ANSWERED BY get() BUT NOT REPORTED BY has(): THE PAIR SAID "ABSENT", AND SO MUST THE SINGLE PASS
    assertThat(e.getIfPresent("@out", ABSENT)).isEqualTo(e.has("@out") ? e.get("@out") : ABSENT);
    database.transaction(() -> {
      assertPresence(v.modify());
      assertPresence(e.modify());
    });
  }

  /** Regression test for issue #8266. */
  @Test
  void sqlAnswersMatchTheHasThenGetPair() {
    createSinglePassReadTypes();
    database.transaction(() -> {
      database.newDocument("Doc8266").set("name", "Jay").set("nothing", null).set("n", 1).save();
      database.newDocument("Doc8266").set("name", "Kay").set("n", 2).save();
    });

    try (final ResultSet rs = database.query("sql",
        "select name, nothing, missing, n from Doc8266 order by n")) {
      Result r = rs.next();
      assertThat(r.<String>getProperty("name")).isEqualTo("Jay");
      assertThat(r.<Object>getProperty("nothing")).isNull();
      assertThat(r.<Object>getProperty("missing")).isNull();
      r = rs.next();
      assertThat(r.<String>getProperty("name")).isEqualTo("Kay");
    }

    // `SELECT *, !name` LEAVES THE ELEMENT ON THE ROW BUT DOES NOT PROJECT name: AN OUTER READ MUST STILL SEE null, NOT
    // THE ELEMENT'S VALUE THROUGH A FALL-BACK
    try (final ResultSet rs = database.query("sql", "select name from (select *, !name from Doc8266 where n = 1)")) {
      assertThat(rs.next().<Object>getProperty("name")).isNull();
    }

    // A WHERE ON A PRESENT-BUT-NULL AND ON AN ABSENT PROPERTY
    try (final ResultSet rs = database.query("sql", "select count(*) as c from Doc8266 where nothing is null")) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(2L);
    }
    try (final ResultSet rs = database.query("sql", "select count(*) as c from Doc8266 where nothing is not defined")) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(1L);
    }
  }

  /** Regression test for issue #8266. */
  @Test
  void resultInternalKeepsItsPrecedence() {
    createSinglePassReadTypes();
    final RID[] ridHolder = new RID[1];
    database.transaction(
        () -> ridHolder[0] = database.newDocument("Doc8266").set("name", "Jay").set("other", "x").save().getIdentity());
    final RID rid = ridHolder[0];
    final Document doc = rid.asDocument();

    // ELEMENT ONLY
    final ResultInternal elementRow = new ResultInternal(doc);
    assertThat(elementRow.getPropertyIfPresent("name", ABSENT)).isEqualTo("Jay");
    assertThat(elementRow.getPropertyIfPresent("missing", ABSENT)).isSameAs(ABSENT);

    // ELEMENT + NON-EMPTY CONTENT: CONTENT ANSWERS, AND A NAME ONLY THE ELEMENT HAS READS null (PRESENT), AS BEFORE
    final ResultInternal mixed = new ResultInternal(database);
    mixed.setElement(doc);
    mixed.setProperty("other", "projected");
    assertThat(mixed.getPropertyIfPresent("other", ABSENT)).isEqualTo("projected");
    assertThat(mixed.getPropertyIfPresent("name", ABSENT)).isEqualTo(mixed.hasProperty("name") ? mixed.getProperty("name") : ABSENT);
    assertThat(mixed.getPropertyIfPresent("missing", ABSENT)).isSameAs(ABSENT);

    // TOMBSTONE
    mixed.removeProperty("other");
    assertThat(mixed.getPropertyIfPresent("other", ABSENT)).isSameAs(ABSENT);

    // A LINK IS ANSWERED AS ITS RID, LIKE getProperty() DOES
    final ResultInternal linkRow = new ResultInternal(database);
    linkRow.setProperty("link", doc);
    assertThat(linkRow.getPropertyIfPresent("link", ABSENT)).isEqualTo(linkRow.getProperty("link"));

    // TRAVERSE $depth IS ANSWERED BY getProperty() BUT NOT REPORTED BY hasProperty()
    final TraverseResult traverse = new TraverseResult(doc);
    traverse.setProperty("$depth", 3);
    assertThat(traverse.getPropertyIfPresent("$depth", ABSENT)).isSameAs(ABSENT);
    assertThat(traverse.getPropertyIfPresent("name", ABSENT)).isEqualTo("Jay");

    database.transaction(() -> {
      final UpdatableResult updatable = new UpdatableResult(doc.modify());
      assertThat(updatable.getPropertyIfPresent("name", ABSENT)).isEqualTo("Jay");
      assertThat(updatable.getPropertyIfPresent("missing", ABSENT)).isSameAs(ABSENT);
    });
  }

  private static void assertPresence(final Document doc) {
    assertThat(doc.getIfPresent("name", ABSENT)).isEqualTo(doc.get("name"));
    assertThat(doc.has("nothing")).isTrue();
    assertThat(doc.getIfPresent("nothing", ABSENT)).isNull();
    assertThat(doc.has("missing")).isFalse();
    assertThat(doc.getIfPresent("missing", ABSENT)).isSameAs(ABSENT);
  }

  private static final class CountingResult extends ResultInternal {
    private final ResultInternal delegate;
    int hasPropertyCalls;
    int getPropertyCalls;
    int ifPresentCalls;

    CountingResult(final ResultInternal delegate) {
      super((Object) null);
      this.delegate = delegate;
    }

    @Override
    public boolean hasProperty(final String name) {
      hasPropertyCalls++;
      return delegate.hasProperty(name);
    }

    @Override
    public <T> T getProperty(final String name) {
      getPropertyCalls++;
      return delegate.getProperty(name);
    }

    @Override
    public Object getPropertyIfPresent(final String name, final Object absentValue) {
      ifPresentCalls++;
      return delegate.getPropertyIfPresent(name, absentValue);
    }
  }

  // ---- #9841: prefix wildcard in a nested projection ----
  /*
   * Issue #9841: a prefix wildcard on the include side of a nested projection ({@code @this:{s*}}) was parsed, stored and
   * printed back, but the include loops evaluated it as the literal property {@code s}, answering a null column instead of
   * the properties the wildcard names. The exclude side ({@code !s*}) already honored it.
   */

  private void createNestedProjectionDocument() {
    database.command("sql", "create document type D1").close();
    database.transaction(() -> database.command("sql", "insert into D1 content {n:1, m:2, secret:'s', nested:{a:1, ab:2, b:3}}").close());
  }

  private Result project(final String projection) {
    try (final ResultSet rs = database.query("sql", "select " + projection + " as p from D1")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(rs.hasNext()).isFalse();
      return row.getProperty("p");
    }
  }

  /** Regression test for issue #9841. */
  @Test
  void prefixWildcardOnDocumentSelectsMatchingProperties() {
    createNestedProjectionDocument();
    final Result p = project("@this:{s*}");
    assertThat(p.getPropertyNames()).containsExactly("secret");
    assertThat(p.<String>getProperty("secret")).isEqualTo("s");
  }

  /** Regression test for issue #9841. */
  @Test
  void prefixWildcardThatIsAlsoALiteralPropertyKeepsEveryMatch() {
    createNestedProjectionDocument();
    final Result p = project("@this:{n*, m}");
    assertThat(p.getPropertyNames()).containsExactlyInAnyOrder("n", "nested", "m");
    assertThat(p.<Integer>getProperty("n")).isEqualTo(1);
    assertThat(p.<Integer>getProperty("m")).isEqualTo(2);
    assertThat(p.<Object>getProperty("nested")).isNotNull();
  }

  /** Regression test for issue #9841. */
  @Test
  void prefixWildcardMatchingNothingProducesNoColumn() {
    createNestedProjectionDocument();
    final Result p = project("@this:{zz*, m}");
    assertThat(p.getPropertyNames()).containsExactly("m");
  }

  /** Regression test for issue #9841. */
  @Test
  void prefixWildcardHonorsExcludeItemsOfTheSameBraces() {
    createNestedProjectionDocument();
    final Result p = project("@this:{n*, !nested}");
    assertThat(p.getPropertyNames()).containsExactly("n");
  }

  /** Regression test for issue #9841. */
  @Test
  void prefixWildcardOnMapValue() {
    createNestedProjectionDocument();
    final Result p = project("nested:{a*}");
    assertThat(p.getPropertyNames()).containsExactlyInAnyOrder("a", "ab");
    assertThat(p.<Integer>getProperty("a")).isEqualTo(1);
    assertThat(p.<Integer>getProperty("ab")).isEqualTo(2);
  }

  /** Regression test for issue #9841. */
  @Test
  void prefixWildcardOnNonElementResult() {
    createNestedProjectionDocument();
    // A LET subquery yields projected rows: Result objects that are not records, so the nested projection takes its Result path
    try (final ResultSet rs = database.query("sql", "select $r:{s*} as p let $r = (select n, secret, m from D1)")) {
      final List<Result> rows = rs.next().getProperty("p");
      assertThat(rows).hasSize(1);
      final Result p = rows.getFirst();
      assertThat(p.isElement()).isFalse();
      assertThat(p.getPropertyNames()).containsExactly("secret");
      assertThat(p.<String>getProperty("secret")).isEqualTo("s");
    }
  }

  /** Regression test for issue #9841. */
  @Test
  void prefixWildcardAppliesNestedExpansionToEveryMatch() {
    createNestedProjectionDocument();
    final Result p = project("@this:{ne*:{a}}");
    assertThat(p.getPropertyNames()).containsExactly("nested");
    final Result nested = p.getProperty("nested");
    assertThat(nested.getPropertyNames()).containsExactly("a");
    assertThat(nested.<Integer>getProperty("a")).isEqualTo(1);
  }

  /** Regression test for issue #9841. */
  @Test
  void prefixWildcardOnMapValueHonorsExcludeItems() {
    createNestedProjectionDocument();
    final Result p = project("nested:{a*, !ab}");
    assertThat(p.getPropertyNames()).containsExactly("a");
  }

  /** Regression test for issue #9841. */
  @Test
  void overlappingItemsLetTheLastOneWin() {
    createNestedProjectionDocument();
    // n* emits nested as stored, then ne*:{a} names it again with an expansion: the later item's value is the one kept
    final Result p = project("@this:{n*, ne*:{a}}");
    assertThat(p.getPropertyNames()).containsExactlyInAnyOrder("n", "nested");
    final Result nested = p.getProperty("nested");
    assertThat(nested.getPropertyNames()).containsExactly("a");
  }

  /** Regression test for issue #9841. */
  @Test
  void nonWildcardIncludeItemsKeepTheirBehaviour() {
    createNestedProjectionDocument();
    final Result p = project("@this:{n, nested.a as na}");
    assertThat(p.getPropertyNames()).containsExactlyInAnyOrder("n", "na");
    assertThat(p.<Integer>getProperty("na")).isEqualTo(1);
  }

  /** Regression test for issue #9841. */
  @Test
  void aliasOnAPrefixWildcardIsRefused() {
    createNestedProjectionDocument();
    assertThatThrownBy(() -> project("@this:{se* as k}")).isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("se*");
  }

  /** Regression test for issue #9841. */
  @Test
  void wildcardOnAnythingButAPlainPropertyNameIsRefused() {
    createNestedProjectionDocument();
    assertThatThrownBy(() -> project("@this:{nested.a*}")).isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("nested.a*");
    assertThatThrownBy(() -> project("@this:{!nested.a*}")).isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("nested.a*");
  }

  /** Regression test for issue #9841. */
  @Test
  void backtickQuotedPrefixIsAccepted() {
    createNestedProjectionDocument();
    final Result p = project("@this:{`se`*}");
    assertThat(p.getPropertyNames()).containsExactly("secret");
  }

  /** Regression test for issue #9841. */
  @Test
  void aliasOnAnExcludeWildcardIsIgnoredLikeOnAnyExcludeItem() {
    createNestedProjectionDocument();
    // An exclude item produces no column, so its alias has nothing to name: it is accepted and ignored, as on `!secret as k`
    final Result p = project("@this:{!se* as k}");
    assertThat(p.getPropertyNames()).doesNotContain("secret", "k").contains("n", "m", "nested");
  }

  /** Regression test for issue #9841. */
  @Test
  void prefixWildcardNextToTheBareStarAddsNothingTheStarDidNotSelect() {
    createNestedProjectionDocument();
    final Result p = project("@this:{*, s*}");
    assertThat(p.getPropertyNames()).contains("n", "m", "nested", "secret");
    assertThat(p.<String>getProperty("secret")).isEqualTo("s");
  }

  /** Regression test for issue #9841. */
  @Test
  void excludeItemRemovesAPropertyBothTheStarAndTheWildcardSelect() {
    createNestedProjectionDocument();
    final Result p = project("@this:{*, s*, !secret}");
    assertThat(p.getPropertyNames()).doesNotContain("secret").contains("n", "m", "nested");
  }

  /** Regression test for issue #9841. */
  @Test
  void prefixWildcardKeepsANullValuedProperty() {
    createNestedProjectionDocument();
    database.command("sql", "create document type D2").close();
    database.transaction(() -> database.command("sql", "insert into D2 content {sa:null, sb:1}").close());
    try (final ResultSet rs = database.query("sql", "select @this:{s*} as p from D2")) {
      final Result p = rs.next().getProperty("p");
      assertThat(p.getPropertyNames()).containsExactlyInAnyOrder("sa", "sb");
      assertThat(p.<Object>getProperty("sa")).isNull();
    }
  }

  /** Regression test for issue #9841. */
  @Test
  void excludeWildcardStillFilters() {
    createNestedProjectionDocument();
    final Result p = project("@this:{!s*}");
    assertThat(p.getPropertyNames()).doesNotContain("secret").contains("n", "m", "nested");
  }

  // ---- #10098: toPropertyMap() in one pass ----
  /*
   * Issue #10098: {@link Result#toPropertyMap(boolean)} of a row that carries a record and no projection content read the record one
   * property at a time, and {@code ImmutableDocument.get(name)} walks the record header from its first byte until it meets the name, so
   * N properties cost N+1 passes where the {@code toMap(false)} it replaced cost one. The ratio grew with the property count (1.7x at 10
   * properties, 3.6x at 160). The guarantee is the number of passes, not a faster line: reading the whole row costs one pass over the
   * record, whatever its shape, whatever the row drops.
   */

  private static final int PROPERTIES = 40;

  private void createPropertyMapTypes() {
    database.getSchema().createDocumentType("Doc10098");
    database.getSchema().createVertexType("Vtx10098");
  }

  /** Regression test for issue #10098. */
  @Test
  void readingTheWholeRowIsOnePassOverTheRecordWhateverItsPropertyCount() {
    createPropertyMapTypes();
    final RID rid = createDocument(PROPERTIES);
    final CountingImmutableDocument document = new CountingImmutableDocument(database, database.getSchema().getType("Doc10098"), rid);

    final Map<String, Object> map = new ResultInternal(document).toPropertyMap(false);

    assertThat(map).hasSize(PROPERTIES + 3);
    assertThat(document.propertyReads()).as("reads by name, each a walk over the record header").isZero();
    assertThat(document.headerWalks()).as("passes over the record header").isEqualTo(1);
  }

  /** Regression test for issue #10098. */
  @Test
  void theMetadataSeedDoesNotCostAPass() {
    createPropertyMapTypes();
    final RID rid = createDocument(PROPERTIES);
    final CountingImmutableDocument document = new CountingImmutableDocument(database, database.getSchema().getType("Doc10098"), rid);

    final Map<String, Object> map = new ResultInternal(document).toPropertyMap(true);

    assertThat(map).containsEntry("@type", "Doc10098").containsEntry("@cat", "d").containsEntry("@rid", rid.toString());
    assertThat(document.headerWalks()).isEqualTo(1);
  }

  /** Regression test for issue #10098. */
  @Test
  void aPropertyTheRowDroppedIsLeftOutAndStillCostsOnePass() {
    createPropertyMapTypes();
    final RID rid = createDocument(PROPERTIES);
    final CountingImmutableDocument document = new CountingImmutableDocument(database, database.getSchema().getType("Doc10098"), rid);
    final ResultInternal row = new ResultInternal(document);
    row.removeProperty("p3");

    final Map<String, Object> map = row.toPropertyMap(false);

    assertThat(map).doesNotContainKey("p3").containsKey("p4").hasSize(PROPERTIES + 2);
    assertThat(document.headerWalks()).isEqualTo(1);
  }

  /**
   * Regression test for issue #10098.
   * <p>
   * The single pass answers exactly what the record's getters do, shape by shape.
   */
  @Test
  void theMapHoldsWhatTheRowGettersAnswer() {
    createPropertyMapTypes();
    final RID[] link = new RID[1];
    final RID[] rid = new RID[1];
    database.transaction(() -> {
      link[0] = database.newVertex("Vtx10098").set("name", "target").save().getIdentity();
      final MutableDocument doc = database.newDocument("Doc10098");
      doc.set("text", "hello").set("number", 42).set("flag", true).set("nothing", null);
      doc.set("list", List.of(1, 2, 3)).set("map", Map.of("k", "v"));
      doc.set("link", link[0]);
      rid[0] = doc.save().getIdentity();
    });

    final CountingImmutableDocument document = new CountingImmutableDocument(database, database.getSchema().getType("Doc10098"), rid[0]);
    final ResultInternal row = new ResultInternal(document);
    final Map<String, Object> map = row.toPropertyMap(false);

    assertThat(map.keySet()).containsExactlyInAnyOrderElementsOf(row.getPropertyNames());
    for (final String name : row.getPropertyNames())
      assertThat(map.get(name)).as(name).isEqualTo(row.getProperty(name));
    assertThat(map.get("link")).isEqualTo(link[0]);
    assertThat(map).containsKey("nothing");
  }

  /**
   * Regression test for issue #10098.
   * <p>
   * The single pass decodes the same entries and value types as a read by name, on a schema with typed properties, a default value, an
   * embedded document, temporal and decimal types, a list and a map.
   */
  @Test
  void theMapHoldsTheSameEntriesAndTypesAsAReadByNameOnATypedSchema() {
    createPropertyMapTypes();
    database.getSchema().createDocumentType("Address10098");
    final DocumentType mixed = database.getSchema().createDocumentType("Mixed10098");
    mixed.createProperty("when", Type.DATETIME);
    mixed.createProperty("day", Type.DATE);
    mixed.createProperty("price", Type.DECIMAL);
    mixed.createProperty("tags", Type.LIST);
    mixed.createProperty("attributes", Type.MAP);
    mixed.createProperty("home", Type.EMBEDDED).setOfType("Address10098");
    mixed.createProperty("status", Type.STRING).setDefaultValue("'new'");

    final RID[] rid = new RID[1];
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Mixed10098");
      doc.set("when", LocalDateTime.of(2026, 10, 15, 12, 30, 45)).set("day", LocalDate.of(2026, 10, 15))
          .set("price", new BigDecimal("12.34")).set("tags", List.of("a", "b")).set("attributes", Map.of("k", 1));
      doc.newEmbeddedDocument("Address10098", "home").set("city", "Rome").set("zip", 118);
      rid[0] = doc.save().getIdentity();
    });

    final CountingImmutableDocument document = new CountingImmutableDocument(database, database.getSchema().getType("Mixed10098"), rid[0]);
    final Map<String, Object> map = new ResultInternal(document).toPropertyMap(false);
    assertThat(document.headerWalks()).isEqualTo(1);

    final Document byName = database.lookupByRID(rid[0], true).asDocument();
    assertThat(map.keySet()).containsExactlyInAnyOrderElementsOf(byName.getPropertyNames());
    for (final String name : byName.getPropertyNames()) {
      final Object expected = byName.get(name);
      if (expected instanceof Document embedded)
        assertThat(((Document) map.get(name)).toMap(false)).as(name).isEqualTo(embedded.toMap(false));
      else
        assertThat(map.get(name)).as(name).isEqualTo(expected);
      assertThat(map.get(name) == null ? null : map.get(name).getClass()).as("type of " + name)
          .isEqualTo(expected == null ? null : expected.getClass());
    }
    assertThat(map.get("status")).isEqualTo("new");
  }

  /**
   * Regression test for issue #10098.
   * <p>
   * Every shape of record a row can carry answers, by name, exactly what its property map holds: mutable, vertex, edge, detached.
   */
  @Test
  void everyRecordShapeAgreesWithItsGettersByName() {
    createPropertyMapTypes();
    database.getSchema().createEdgeType("Edge10098");
    final RID[] rids = new RID[3];
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("Vtx10098").set("name", "a").set("n", 1).set("nothing", null).save();
      final MutableVertex b = database.newVertex("Vtx10098").set("name", "b").save();
      rids[0] = a.getIdentity();
      rids[1] = a.newEdge("Edge10098", b, "weight", 2.5, "label", "x").getIdentity();
      rids[2] = createDocument(3);
    });

    final List<Document> shapes = List.of(database.lookupByRID(rids[0], true).asDocument(),
        database.lookupByRID(rids[1], true).asDocument(), database.lookupByRID(rids[2], true).asDocument(),
        database.lookupByRID(rids[2], true).asDocument().modify(), database.lookupByRID(rids[2], true).asDocument().detach());

    for (final Document shape : shapes) {
      final Map<String, Object> map = new ResultInternal(shape).toPropertyMap(false);
      assertThat(map.keySet()).as(shape.getClass().getSimpleName()).containsExactlyInAnyOrderElementsOf(shape.getPropertyNames());
      for (final String name : shape.getPropertyNames())
        assertThat(map.get(name)).as(shape.getClass().getSimpleName() + "." + name).isEqualTo(shape.get(name));
    }
  }

  /**
   * Regression test for issue #10098.
   * <p>
   * {@code ImmutableDocument.toMap()} adds its metadata to the map the serializer answers, without a copy: that holds for a record
   * with no properties too, whose map is the serializer's early-return one.
   */
  @Test
  void aRecordWithoutPropertiesAnswersAMapTheMetadataCanBeAddedTo() {
    createPropertyMapTypes();
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("Doc10098").save().getIdentity());

    final Document empty = database.lookupByRID(rid[0], true).asDocument();
    assertThat(empty.toMap(false)).isEmpty();
    assertThat(empty.toMap(true)).containsEntry("@type", "Doc10098").containsEntry("@cat", "d").containsEntry("@rid", rid[0].toString());
    assertThat(new ResultInternal(empty).toPropertyMap(false)).isEmpty();
    assertThat(new ResultInternal(empty).toPropertyMap(true)).containsEntry("@rid", rid[0].toString());
  }

  /** Regression test for issue #10098. */
  @Test
  void aVertexRowIsOnePassToo() {
    createPropertyMapTypes();
    final RID[] rid = new RID[1];
    database.transaction(() -> {
      final MutableVertex vertex = database.newVertex("Vtx10098");
      for (int i = 0; i < PROPERTIES; i++)
        vertex.set("p" + i, "value-" + i);
      rid[0] = vertex.save().getIdentity();
    });

    final Document loaded = database.lookupByRID(rid[0], true).asDocument();
    assertThat(new ResultInternal(loaded).toPropertyMap(false)).hasSize(PROPERTIES);
  }

  /**
   * Regression test for issue #10098.
   * <p>
   * A row off a real scan, the case of INSERT ... FROM SELECT, answers the same map as the record's own.
   */
  @Test
  void aRowOffAScanAnswersTheRecordsOwnMap() {
    createPropertyMapTypes();
    createDocument(5);

    try (final ResultSet rs = database.query("sql", "SELECT FROM Doc10098")) {
      final Result row = rs.next();
      assertThat(row.toPropertyMap(false)).isEqualTo(row.toElement().toMap(false));
    }
  }

  // --- helpers ---

  private RID createDocument(final int properties) {
    final RID[] rid = new RID[1];
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Doc10098");
      for (int i = 0; i < properties; i++)
        doc.set("p" + i, "value-" + i);
      doc.set("n", 7).set("flag", true).set("nothing", null);
      rid[0] = doc.save().getIdentity();
    });
    return rid[0];
  }

  // ---- #10264: storing a projected subquery ----
  /*
   * #10264: storing the result of a projection that holds a SELECT subquery failed with a {@code ClassCastException}
   * ({@code ResultInternal} to {@code Map}) in the record serializer, for {@code CREATE MATERIALIZED VIEW} and for
   * {@code INSERT ... FROM}, while the query itself answered fine. The subquery's value is a list of non-element results, which
   * the serializer types as maps but then cast without converting.
   */

  private static final String QUERY = "SELECT id, (SELECT count(*) FROM Owner) AS oc FROM Base WHERE n < 3";

  private void createProjectedSubqueryData() {
    database.command("sql", "CREATE DOCUMENT TYPE Base").close();
    database.command("sql", "CREATE PROPERTY Base.id INTEGER").close();
    database.command("sql", "CREATE PROPERTY Base.n INTEGER").close();
    database.command("sql", "CREATE DOCUMENT TYPE Owner").close();
    database.transaction(() -> {
      for (int i = 0; i < 5; i++)
        database.newDocument("Base").set("id", i).set("n", i).save();
      for (int i = 0; i < 2; i++)
        database.newDocument("Owner").set("k", i).save();
    });
  }

  @SuppressWarnings("unchecked")
  private void assertCountOfOwners(final String source) {
    int rows = 0;
    try (final ResultSet rs = database.query("sql", "SELECT id, oc FROM " + source + " ORDER BY id")) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat(row.<Number>getProperty("id").intValue()).isEqualTo(rows++);
        final List<Object> oc = row.getProperty("oc");
        assertThat(oc).hasSize(1);
        assertThat(((Map<String, Object>) oc.getFirst()).get("count(*)")).isEqualTo(2L);
      }
    }
    assertThat(rows).isEqualTo(3);
  }

  /** Regression test for issue #10264. */
  @Test
  void insertFromSelectStoresTheSubqueryAsAListOfMaps() {
    createProjectedSubqueryData();
    database.command("sql", "CREATE DOCUMENT TYPE T").close();
    database.transaction(() -> database.command("sql", "INSERT INTO T FROM " + QUERY).close());

    assertCountOfOwners("T");
  }

  /** Regression test for issue #10264. */
  @Test
  void materializedViewWithDefaultRefreshStoresTheSubquery() {
    createProjectedSubqueryData();
    database.command("sql", "CREATE MATERIALIZED VIEW vm AS " + QUERY).close();

    assertCountOfOwners("vm");
  }

  /** Regression test for issue #10264. */
  @Test
  void incrementalMaterializedViewStoresTheSubquery() {
    createProjectedSubqueryData();
    database.command("sql", "CREATE MATERIALIZED VIEW vi AS " + QUERY + " REFRESH INCREMENTAL").close();

    assertCountOfOwners("vi");
  }

  /** Regression test for issue #10264. */
  @Test
  void aMapHoldingAResultIsStoredToo() {
    createProjectedSubqueryData();
    // the same cast sits behind a Result nested in a map value, so it is pinned here and not only through the list path
    database.command("sql", "CREATE DOCUMENT TYPE M").close();
    database.transaction(() -> database.command("sql",
        "INSERT INTO M FROM SELECT id, { 'owners': (SELECT count(*) FROM Owner) } AS wrapped FROM Base WHERE n < 1").close());

    try (final ResultSet rs = database.query("sql", "SELECT wrapped FROM M")) {
      assertThat(rs.hasNext()).isTrue();
      final Map<String, Object> wrapped = rs.next().getProperty("wrapped");
      assertThat(wrapped.get("owners")).isEqualTo(List.of(Map.of("count(*)", 2L)));
    }
  }

  /** Regression test for issue #10264. */
  @Test
  void theControlWithoutASubqueryKeepsWorking() {
    createProjectedSubqueryData();
    database.command("sql", "CREATE MATERIALIZED VIEW vc AS SELECT id, n FROM Base WHERE n < 3 REFRESH INCREMENTAL").close();

    try (final ResultSet rs = database.query("sql", "SELECT count(@rid) AS c FROM vc")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isEqualTo(3L);
    }
  }
}
