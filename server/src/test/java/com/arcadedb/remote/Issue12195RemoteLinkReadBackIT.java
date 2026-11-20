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
package com.arcadedb.remote;

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12195: a LINK stored in a property the schema does not declare was read back by the HTTP client as its "#12:0" text,
 * and a list or map of links as strings, so {@code (Identifiable) doc.get(name)} failed.
 * <p>
 * The hint that fixes it must not reach what is no link: an embedded document or a vertex goes out as a JSON object, and the
 * client rebuilds a hinted item as a RID, which used to empty it.
 */
class Issue12195RemoteLinkReadBackIT extends BaseGraphServerTest {
  private RemoteDatabase db;

  @Override
  protected String getDatabaseName() {
    return "issue12195";
  }

  @BeforeEach
  void connect() {
    db = new RemoteDatabase("127.0.0.1", getServerHttpPort(), getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);
    db.command("sql", "CREATE VERTEX TYPE P12195");
    db.command("sql", "CREATE DOCUMENT TYPE Addr12195");
    db.command("sql", "CREATE DOCUMENT TYPE Person12195");
    db.command("sql", "CREATE PROPERTY Person12195.addresses LIST OF Addr12195");
  }

  @AfterEach
  void disconnect() {
    if (db != null)
      db.close();
  }

  @Test
  void linksOfAnUndeclaredPropertyAreReadBackAsRids() {
    final RID other = db.command("sql", "INSERT INTO P12195 SET name = 'other'").next().getIdentity().get();
    final RID me = db.command("sql",
        "INSERT INTO P12195 SET name = 'me', friend = " + other + ", friends = [" + other + "], byName = {'x': " + other + "}").next()
        .getIdentity().get();

    assertLinks(db.lookupByRID(me, true).asDocument(), other);

    // the same record read through a query and through an iteration
    try (final var rs = db.query("sql", "SELECT FROM P12195 WHERE name = 'me'")) {
      assertLinks(rs.next().getElement().get(), other);
    }
    for (final Iterator<Record> it = db.iterateType("P12195", true); it.hasNext(); ) {
      final Document doc = it.next().asDocument();
      if ("me".equals(doc.getString("name")))
        assertLinks(doc, other);
    }
  }

  @Test
  void aProjectedListOfLinksIsAListOfRids() {
    final RID other = db.command("sql", "INSERT INTO P12195 SET name = 'other'").next().getIdentity().get();
    final RID me = db.command("sql", "INSERT INTO P12195 SET name = 'me', friend = " + other + ", friends = [" + other + "]").next()
        .getIdentity().get();

    final Result row = db.query("sql", "SELECT friend, friends FROM " + me).next();
    assertThat(row.<Object>getProperty("friend")).isEqualTo(other);
    assertThat(row.<List<Object>>getProperty("friends")).containsExactly(other);
  }

  @Test
  void aStringThatLooksLikeARidStaysAString() {
    final RID me = db.command("sql", "INSERT INTO P12195 SET name = 'me', code = '#1:0', codes = ['#1:0']").next().getIdentity().get();
    final Document doc = db.lookupByRID(me, true).asDocument();
    assertThat(doc.get("code")).isEqualTo("#1:0");
    assertThat(doc.<Object>getList("codes")).containsExactly("#1:0");
  }

  /** A list of strings that merely look like rids is no list of links, nor is one mixing links and text. */
  @Test
  void aListMixingLinksAndStringsKeepsItsText() {
    final RID other = db.command("sql", "INSERT INTO P12195 SET name = 'other'").next().getIdentity().get();
    final RID me = db.command("sql", "INSERT INTO P12195 SET name = 'me', mixed = ['text', '#1:0', " + other + "]").next()
        .getIdentity().get();
    final List<Object> mixed = db.lookupByRID(me, true).asDocument().getList("mixed");
    assertThat(mixed).hasSize(3);
    assertThat(mixed.get(0)).isEqualTo("text");
    assertThat(mixed.get(1)).isEqualTo("#1:0");
  }

  /** A property the schema DECLARES with its element type reads back through the schema, as it did before the hint. */
  @Test
  void declaredLinkPropertiesReadBackAsRids() {
    db.command("sql", "CREATE PROPERTY P12195.dlink LINK");
    db.command("sql", "CREATE PROPERTY P12195.dlinks LIST OF LINK");
    db.command("sql", "CREATE PROPERTY P12195.dmapl MAP OF LINK");
    final RID other = db.command("sql", "INSERT INTO P12195 SET name = 'other'").next().getIdentity().get();
    final RID me = db.command("sql",
        "INSERT INTO P12195 SET name = 'me', dlink = " + other + ", dlinks = [" + other + "], dmapl = {'x': " + other + "}").next()
        .getIdentity().get();

    final Document doc = db.lookupByRID(me, true).asDocument();
    assertThat(doc.get("dlink")).isEqualTo(other);
    assertThat(doc.<Object>getList("dlinks")).containsExactly(other);
    assertThat(doc.<Map<String, Object>>getMap("dmapl")).containsEntry("x", other);
  }

  /** The content of an embedded document in an undeclared property: a single one, a list of them and a map of them. */
  @Test
  void undeclaredEmbeddedDocumentsKeepTheirContent() {
    final RID rid = createVertexWithEmbeddedDocuments();

    assertEmbeddedContent(db.lookupByRID(rid, true).asDocument());
    try (final ResultSet rs = db.query("sql", "SELECT FROM P12195 WHERE name = 'emb'")) {
      assertEmbeddedContent(rs.next().getElement().get());
    }
  }

  /** SELECT addresses FROM Person: a declared LIST OF EMBEDDED projected as a column, which always carries a LIST hint. */
  @Test
  void aProjectedListOfEmbeddedDocumentsKeepsItsContent() {
    final Database server = getServerDatabase(0, getDatabaseName());
    server.transaction(() -> {
      final MutableDocument person = server.newDocument("Person12195");
      person.set("name", "ann");
      person.newEmbeddedDocument("Addr12195", "addresses").set("city", "Milan");
      person.newEmbeddedDocument("Addr12195", "addresses").set("city", "Turin");
      person.save();
    });

    try (final ResultSet rs = db.query("sql", "SELECT addresses FROM Person12195")) {
      final List<Object> addresses = rs.next().getProperty("addresses");
      assertThat(addresses).hasSize(2);
      assertThat(cityOf(addresses.get(0))).isEqualTo("Milan");
      assertThat(cityOf(addresses.get(1))).isEqualTo("Turin");
    }
  }

  /** RETURN collect(n): a list of vertices is a LIST column too, and each vertex goes out as a JSON object. */
  @Test
  void aCypherCollectOfVerticesKeepsItsContent() {
    db.command("sql", "INSERT INTO P12195 SET name = 'a', age = 1");
    db.command("sql", "INSERT INTO P12195 SET name = 'b', age = 2");

    try (final ResultSet rs = db.query("opencypher", "MATCH (n:P12195) RETURN collect(n) AS nodes")) {
      final List<Object> nodes = rs.next().getProperty("nodes");
      assertThat(nodes).hasSize(2);
      final List<String> names = new ArrayList<>();
      for (final Object node : nodes)
        names.add(nameOf(node));
      assertThat(names).containsExactlyInAnyOrder("a", "b");
    }
  }

  private RID createVertexWithEmbeddedDocuments() {
    final Database server = getServerDatabase(0, getDatabaseName());
    final RID[] rid = new RID[1];
    server.transaction(() -> {
      final MutableVertex vertex = server.newVertex("P12195").set("name", "emb");
      vertex.set("list", new ArrayList<>()).set("map", new HashMap<>());
      vertex.newEmbeddedDocument("Addr12195", "one").set("city", "Rome");
      vertex.newEmbeddedDocument("Addr12195", "list").set("city", "Milan");
      vertex.newEmbeddedDocument("Addr12195", "map", "home").set("city", "Turin");
      vertex.save();
      rid[0] = vertex.getIdentity();
    });
    return rid[0];
  }

  private static void assertEmbeddedContent(final Document doc) {
    assertThat(cityOf(doc.get("one"))).isEqualTo("Rome");

    final List<Object> list = doc.getList("list");
    assertThat(list).hasSize(1);
    assertThat(cityOf(list.get(0))).isEqualTo("Milan");

    final Map<String, Object> map = doc.getMap("map");
    assertThat(map).containsOnlyKeys("home");
    assertThat(cityOf(map.get("home"))).isEqualTo("Turin");
  }

  private static String cityOf(final Object embedded) {
    return propertyOf(embedded, "city");
  }

  private static String nameOf(final Object node) {
    return propertyOf(node, "name");
  }

  /** An embedded document or a vertex reaches the client as a Map or as a Document, depending on the shape of the row. */
  private static String propertyOf(final Object value, final String name) {
    assertThat(value).as("the content of the nested document, not a rid or an empty value").isNotNull();
    if (value instanceof Map<?, ?> map)
      return (String) map.get(name);
    if (value instanceof Result result)
      return result.getProperty(name);
    if (value instanceof Document document)
      return document.getString(name);
    throw new AssertionError("Unexpected " + value.getClass().getName() + ": " + value);
  }

  private static void assertLinks(final Document doc, final RID other) {
    assertThat(doc.get("friend")).isEqualTo(other);
    assertThat(((Identifiable) doc.get("friend")).getIdentity()).isEqualTo(other);
    assertThat(doc.<Object>getList("friends")).containsExactly(other);
    assertThat(doc.<Map<String, Object>>getMap("byName")).containsEntry("x", other);
  }
}
