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

import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.VertexType;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11053: {@code getType()} threw {@code UnsupportedOperationException} on every remote mutable record (document, vertex
 * and edge), although the record holds its type and the immutable record it was copied from answers it, so generic code
 * written against {@link Document} broke the moment a record became mutable. The mutable and immutable records are a pair and
 * are checked as one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11053RemoteMutableRecordTypeIT extends BaseGraphServerTest {
  private static final String DATABASE_NAME = "remote-database-11053";

  private RemoteDatabase remote;

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @Test
  void aNewRecordAnswersItsType() {
    final MutableDocument document = remote.newDocument("Doc");
    assertThat(document.getType()).isInstanceOf(DocumentType.class);
    assertThat(document.getType().getName()).isEqualTo("Doc");

    final MutableVertex vertex = remote.newVertex("Person");
    assertThat(vertex.getType()).isInstanceOf(VertexType.class);
    assertThat(vertex.getType().getName()).isEqualTo("Person");
  }

  @Test
  void aModifiedRecordAnswersWhatTheRecordItWasCopiedFromAnswers() {
    remote.transaction(() -> {
      remote.newDocument("Doc").set("name", "doc", "n", 1).save();
      final MutableVertex a = remote.newVertex("Person").set("name", "a").save();
      final MutableVertex b = remote.newVertex("Person").set("name", "b").save();
      final MutableEdge edge = a.newEdge("Knows", b, "since", 2020);
      assertThat(edge.getType()).isInstanceOf(EdgeType.class);
      assertThat(edge.getType().getName()).isEqualTo("Knows");
    });

    assertPair(readOne("SELECT FROM Doc"), DocumentType.class);
    assertPair(readOne("SELECT FROM Person WHERE name = 'a'"), VertexType.class);
    assertPair(readOne("SELECT FROM Knows"), EdgeType.class);
  }

  /** The type a modified record answers is the one its save writes to. */
  @Test
  void aModifiedRecordSavesThroughItsType() {
    remote.transaction(() -> remote.newVertex("Person").set("name", "a").save());
    remote.transaction(() -> {
      final MutableVertex modified = ((Vertex) readOne("SELECT FROM Person")).modify();
      assertThat(modified.getType().getName()).isEqualTo("Person");
      modified.set("name", "renamed").save();
    });
    assertThat(readOne("SELECT FROM Person").getString("name")).isEqualTo("renamed");
  }

  private Document readOne(final String query) {
    try (final ResultSet rs = remote.query("sql", query)) {
      return rs.next().toElement();
    }
  }

  private static void assertPair(final Document read, final Class<? extends DocumentType> kind) {
    final MutableDocument modified = read.modify();

    assertThat(read.getType()).isInstanceOf(kind);
    assertThat(modified.getType()).isSameAs(read.getType());
    assertThat(modified.getTypeName()).isEqualTo(read.getTypeName());
    assertThat(modified.getIdentity()).isEqualTo(read.getIdentity());
    assertThat(modified.getPropertyNames()).isEqualTo(read.getPropertyNames());
    for (final String property : read.getPropertyNames()) {
      assertThat(modified.has(property)).as(property).isEqualTo(read.has(property));
      assertThat(modified.get(property)).as(property).isEqualTo(read.get(property));
    }
    assertThat(modified.toMap()).isEqualTo(read.toMap());
    assertThat(modified.toJSON().toMap()).isEqualTo(read.toJSON().toMap());
  }

  @BeforeEach
  public void beginTest() {
    super.beginTest();
    final RemoteServer server = new RemoteServer("127.0.0.1", getServerHttpPort(), "root", DEFAULT_PASSWORD_FOR_TESTS);
    if (!server.exists(DATABASE_NAME))
      server.create(DATABASE_NAME);
    remote = new RemoteDatabase("127.0.0.1", getServerHttpPort(), DATABASE_NAME, "root", DEFAULT_PASSWORD_FOR_TESTS);
    remote.command("sqlscript", """
        CREATE DOCUMENT TYPE Doc;
        CREATE VERTEX TYPE Person;
        CREATE EDGE TYPE Knows;
        """);
  }

  @AfterEach
  public void endTest() {
    if (remote != null)
      remote.close();
    final RemoteServer server = new RemoteServer("127.0.0.1", getServerHttpPort(), "root", DEFAULT_PASSWORD_FOR_TESTS);
    if (server.exists(DATABASE_NAME))
      server.drop(DATABASE_NAME);
    super.endTest();
  }
}
