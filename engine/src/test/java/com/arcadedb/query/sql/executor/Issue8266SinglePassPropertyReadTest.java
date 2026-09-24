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
import com.arcadedb.database.ImmutableDocument;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.query.sql.parser.SuffixIdentifier;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8266: {@code SuffixIdentifier.execute()} resolved every property through {@code hasProperty()} and then
 * {@code getProperty()}, two passes over the record for every property an expression read. It now asks once, through
 * {@link Result#getPropertyIfPresent(String, Object)}, which must answer exactly what the pair answered: the value when
 * the property is present (null included), the caller's absent marker when it is not.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8266SinglePassPropertyReadTest extends TestHelper {
  private static final Object ABSENT = new Object();

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("Doc8266");
    database.getSchema().createVertexType("V8266");
    database.getSchema().createEdgeType("E8266");
  }

  @Test
  void suffixIdentifierReadsThePropertyInOnePass() {
    final CountingResult row = new CountingResult(new ResultInternal(Map.of("name", "Jay")));

    assertThat(new SuffixIdentifier(new Identifier("name")).execute(row, null)).isEqualTo("Jay");
    assertThat(row.hasPropertyCalls).isZero();
    assertThat(row.getPropertyCalls).isZero();
    assertThat(row.ifPresentCalls).isEqualTo(1);
  }

  @Test
  void resultWithoutItsOwnImplementationKeepsTheHasThenGetAnswer() {
    // EmptyResult implements Result directly, as an embedder's own Result would, so it gets the has()-then-get() default
    final Result row = new EmptyResult();
    assertThat(row.getPropertyIfPresent("missing", ABSENT)).isSameAs(ABSENT);
  }

  @Test
  void immutableDocumentTellsAbsentFromNull() {
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

  @Test
  void graphRecordsTellAbsentFromNull() {
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

  @Test
  void sqlAnswersMatchTheHasThenGetPair() {
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

  @Test
  void resultInternalKeepsItsPrecedence() {
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
}
