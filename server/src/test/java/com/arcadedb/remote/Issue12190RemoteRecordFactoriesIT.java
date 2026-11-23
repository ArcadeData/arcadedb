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

import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.MutableEmbeddedDocument;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12190: {@code newEmbeddedDocument()} on a remote record threw a NullPointerException, {@code newDocument()} on a vertex
 * type saved a vertex, and {@code newVertex()} on a document type threw a ClassCastException.
 */
class Issue12190RemoteRecordFactoriesIT extends BaseGraphServerTest {
  private RemoteDatabase database;

  @Override
  protected String getDatabaseName() {
    return "issue12190";
  }

  @BeforeEach
  void openAndPopulate() {
    database = new RemoteDatabase("127.0.0.1", getServerHttpPort(), getDatabaseName(), "root",
        BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    database.command("sql", "create document type Doc12190");
    database.command("sql", "create document type Address12190");
    database.command("sql", "create vertex type Person12190");
    database.command("sql", "create document type Note12190");
    database.command("sql", "create edge type Knows12190");
  }

  @AfterEach
  void closeDatabase() {
    if (database != null)
      database.close();
  }

  @Test
  void newEmbeddedDocumentIsSavedWithItsOwner() {
    final MutableDocument doc = database.newDocument("Doc12190");
    doc.set("name", "owner");
    final MutableEmbeddedDocument address = doc.newEmbeddedDocument("Address12190", "address");
    address.set("city", "Rome");
    final MutableEmbeddedDocument nested = address.newEmbeddedDocument("Address12190", "previous");
    nested.set("city", "Milan");
    doc.save();

    try (final ResultSet rs = database.query("sql",
        "SELECT address.city AS city, address.@type AS type, address.previous.city AS previousCity FROM " + doc.getIdentity())) {
      final Result row = rs.next();
      assertThat(row.<String>getProperty("city")).isEqualTo("Rome");
      assertThat(row.<String>getProperty("type")).isEqualTo("Address12190");
      assertThat(row.<String>getProperty("previousCity")).isEqualTo("Milan");
    }
  }

  @Test
  void newEmbeddedDocumentInListAndMap() {
    final MutableDocument doc = database.newDocument("Doc12190");
    doc.set("addresses", new ArrayList<>());
    doc.newEmbeddedDocument("Address12190", "addresses").set("city", "Rome");
    doc.newEmbeddedDocument("Address12190", "addresses").set("city", "Milan");
    doc.newEmbeddedDocument("Address12190", "byName", "home").set("city", "Turin");
    doc.save();

    try (final ResultSet rs = database.query("sql",
        "SELECT addresses.city AS cities, byName.home.city AS home FROM " + doc.getIdentity())) {
      final Result row = rs.next();
      assertThat(row.<List<String>>getProperty("cities")).containsExactly("Rome", "Milan");
      assertThat(row.<String>getProperty("home")).isEqualTo("Turin");
    }
  }

  @Test
  void newEmbeddedDocumentOnAVertex() {
    final MutableVertex vertex = database.newVertex("Person12190");
    vertex.newEmbeddedDocument("Address12190", "address").set("city", "Rome");
    vertex.save();

    try (final ResultSet rs = database.query("sql", "SELECT address.city AS city FROM " + vertex.getIdentity())) {
      assertThat(rs.next().<String>getProperty("city")).isEqualTo("Rome");
    }
  }

  @Test
  void newEmbeddedDocumentOfAVertexTypeIsRefused() {
    final MutableDocument doc = database.newDocument("Doc12190");
    assertThatThrownBy(() -> doc.newEmbeddedDocument("Person12190", "address"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Person12190");
  }

  @Test
  void newDocumentOnAVertexTypeIsRefused() {
    assertThatThrownBy(() -> database.newDocument("Person12190"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("is not a document type");
    assertThat(database.countType("Person12190", false)).isZero();
  }

  @Test
  void newVertexOnADocumentTypeIsRefused() {
    assertThatThrownBy(() -> database.newVertex("Note12190"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("is not a vertex type");
  }

  @Test
  void newDocumentAndNewVertexOnTheirOwnKindsStillWork() {
    final MutableDocument doc = database.newDocument("Note12190");
    doc.set("text", "hello");
    doc.save();
    final RID docRid = doc.getIdentity();
    assertThat(database.lookupByRID(docRid, true).asDocument().getString("text")).isEqualTo("hello");

    final MutableVertex vertex = database.newVertex("Person12190");
    vertex.set("name", "alice");
    vertex.save();
    assertThat(database.countType("Person12190", false)).isEqualTo(1);
  }

  @Test
  void newEmbeddedDocumentOnAnEdge() {
    final MutableVertex from = database.newVertex("Person12190").save();
    final MutableVertex to = database.newVertex("Person12190").save();
    final MutableEdge edge = from.newEdge("Knows12190", to);
    edge.newEmbeddedDocument("Address12190", "address").set("city", "Rome");
    edge.save();

    try (final ResultSet rs = database.query("sql", "SELECT address.city AS city FROM " + edge.getIdentity())) {
      assertThat(rs.next().<String>getProperty("city")).isEqualTo("Rome");
    }
  }

  @Test
  void newDocumentAndNewVertexOnAnEdgeTypeAreRefused() {
    assertThatThrownBy(() -> database.newDocument("Knows12190"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("is not a document type");
    assertThatThrownBy(() -> database.newVertex("Knows12190"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("is not a vertex type");
  }

  @Test
  void newDocumentOnATimeSeriesTypeIsNotRefused() {
    // as the embedded API, which accepts a time-series type next to a plain document type
    database.command("sql", "create timeseries type Metric12190 timestamp ts fields (value double)");
    assertThat(database.newDocument("Metric12190")).isNotNull();
  }

  @Test
  void typeNewRecordBuildsTheRecordOfItsKind() {
    final MutableDocument doc = database.getSchema().getType("Note12190").newRecord();
    doc.set("text", "hello");
    doc.save();
    assertThat(database.lookupByRID(doc.getIdentity(), true).asDocument().getString("text")).isEqualTo("hello");

    // a vertex type builds a vertex, as the embedded API: it used to build a document that saved a vertex
    final MutableDocument vertex = database.getSchema().getType("Person12190").newRecord();
    assertThat(vertex).isInstanceOf(MutableVertex.class);
    vertex.set("name", "alice");
    vertex.save();
    assertThat(database.lookupByRID(vertex.getIdentity(), true).asVertex().getString("name")).isEqualTo("alice");
    assertThat(database.countType("Person12190", false)).isEqualTo(1);
  }

  @Test
  void edgeTypeNewRecordIsRefused() {
    // as the embedded API: an edge needs its two vertices
    assertThatThrownBy(() -> database.getSchema().getType("Knows12190").newRecord())
        .isInstanceOf(UnsupportedOperationException.class);
  }
}
