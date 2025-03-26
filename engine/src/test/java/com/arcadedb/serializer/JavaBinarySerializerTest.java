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
package com.arcadedb.serializer;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;

import java.io.*;

import static org.assertj.core.api.Assertions.assertThat;

public class JavaBinarySerializerTest extends TestHelper {

  @Test
  public void testDocumentTransient() throws IOException, ClassNotFoundException {
    final DocumentType type = database.getSchema().createDocumentType("Doc");
    type.createProperty("id", Type.LONG);

    final MutableDocument doc1 = database.newDocument("Doc").set("id", 100L, "name", "John");
    try (final ByteArrayOutputStream arrayOut = new ByteArrayOutputStream();
        final ObjectOutput buffer = new ObjectOutputStream(arrayOut)) {
      doc1.writeExternal(buffer);
      buffer.flush();

      assertThat(arrayOut.size() > 0).isTrue();

      final MutableDocument doc2 = database.newDocument("Doc");

      try (final ByteArrayInputStream arrayIn = new ByteArrayInputStream(arrayOut.toByteArray());
          final ObjectInput in = new ObjectInputStream(arrayIn)) {
        doc2.readExternal(in);
        assertThat(doc2).isEqualTo(doc1);
        assertThat(doc2.toMap()).isEqualTo(doc1.toMap());
      }
    }
  }

  @Test
  public void testDocumentPersistent() throws IOException, ClassNotFoundException {
    final DocumentType type = database.getSchema().createDocumentType("Doc");
    type.createProperty("id", Type.LONG);

    database.setAutoTransaction(true);
    final MutableDocument doc1 = database.newDocument("Doc").set("id", 100L, "name", "John");
    doc1.save();

    try (final ByteArrayOutputStream arrayOut = new ByteArrayOutputStream();
        final ObjectOutput buffer = new ObjectOutputStream(arrayOut)) {
      doc1.writeExternal(buffer);
      buffer.flush();

      assertThat(arrayOut.size() > 0).isTrue();

      final MutableDocument doc2 = database.newDocument("Doc");

      try (final ByteArrayInputStream arrayIn = new ByteArrayInputStream(arrayOut.toByteArray());
          final ObjectInput in = new ObjectInputStream(arrayIn)) {
        doc2.readExternal(in);
        assertThat(doc2).isEqualTo(doc1);
        assertThat(doc2.toMap()).isEqualTo(doc1.toMap());
      }
    }
  }

  @Test
  public void testVertexTransient() throws IOException, ClassNotFoundException {
    final VertexType type = database.getSchema().createVertexType("Doc");
    type.createProperty("id", Type.LONG);

    final MutableVertex doc1 = database.newVertex("Doc").set("id", 100L, "name", "John");
    try (final ByteArrayOutputStream arrayOut = new ByteArrayOutputStream();
        final ObjectOutput buffer = new ObjectOutputStream(arrayOut)) {
      doc1.writeExternal(buffer);
      buffer.flush();

      assertThat(arrayOut.size() > 0).isTrue();

      final MutableVertex docTest = database.newVertex("Doc");

      try (final ByteArrayInputStream arrayIn = new ByteArrayInputStream(arrayOut.toByteArray());
          final ObjectInput in = new ObjectInputStream(arrayIn)) {
        docTest.readExternal(in);
        assertThat(docTest).isEqualTo(doc1);
        assertThat(docTest.toMap()).isEqualTo(doc1.toMap());
      }
    }
  }

  @Test
  public void testVertexPersistent() throws IOException, ClassNotFoundException {
    final VertexType type = database.getSchema().createVertexType("Doc");
    database.getSchema().createEdgeType("Edge");
    type.createProperty("id", Type.LONG);

    database.setAutoTransaction(true);
    final MutableVertex v1 = database.newVertex("Doc").set("id", 100L, "name", "John");
    v1.save();
    final MutableVertex v2 = database.newVertex("Doc").set("id", 101L, "name", "Jay");
    v2.save();
    v1.newEdge("Edge", v2);

    try (final ByteArrayOutputStream arrayOut = new ByteArrayOutputStream();
        final ObjectOutput buffer = new ObjectOutputStream(arrayOut)) {
      v1.writeExternal(buffer);
      buffer.flush();

      assertThat(arrayOut.size() > 0).isTrue();

      final MutableVertex vTest = database.newVertex("Doc");

      try (final ByteArrayInputStream arrayIn = new ByteArrayInputStream(arrayOut.toByteArray());
          final ObjectInput in = new ObjectInputStream(arrayIn)) {
        vTest.readExternal(in);
        assertThat(vTest).isEqualTo(v1);
        assertThat(vTest.toMap()).isEqualTo(v1.toMap());
        assertThat(vTest.getOutEdgesHeadChunk()).isEqualTo(v1.getOutEdgesHeadChunk());
        assertThat(vTest.getInEdgesHeadChunk()).isEqualTo(v1.getInEdgesHeadChunk());
      }
    }
  }

  @Test
  public void testEdgePersistent() throws IOException, ClassNotFoundException {
    database.getSchema().createVertexType("Doc");
    final EdgeType type = database.getSchema().createEdgeType("Edge");

    database.setAutoTransaction(true);
    final MutableVertex v1 = database.newVertex("Doc").set("id", 100L, "name", "John");
    v1.save();
    final MutableVertex v2 = database.newVertex("Doc").set("id", 101L, "name", "Jay");
    v2.save();
    final MutableEdge edge1 = v1.newEdge("Edge", v2);

    try (final ByteArrayOutputStream arrayOut = new ByteArrayOutputStream();
        final ObjectOutput buffer = new ObjectOutputStream(arrayOut)) {
      edge1.writeExternal(buffer);
      buffer.flush();

      assertThat(arrayOut.size() > 0).isTrue();

      final MutableEdge edgeTest = new MutableEdge(database, type, null);

      try (final ByteArrayInputStream arrayIn = new ByteArrayInputStream(arrayOut.toByteArray());
          final ObjectInput in = new ObjectInputStream(arrayIn)) {
        edgeTest.readExternal(in);
        assertThat(edgeTest).isEqualTo(edge1);
        assertThat(edgeTest.toMap()).isEqualTo(edge1.toMap());
        assertThat(edgeTest.getOut()).isEqualTo(edge1.getOut());
        assertThat(edgeTest.getIn()).isEqualTo(edge1.getIn());
      }
    }
  }
}
