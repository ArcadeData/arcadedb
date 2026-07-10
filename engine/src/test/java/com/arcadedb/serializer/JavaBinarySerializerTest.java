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
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class JavaBinarySerializerTest extends TestHelper {

  /**
   * readExternal must correctly populate every property even when the ObjectInput.read(byte[]) delivers
   * only one byte at a time (short-read scenario valid on compressed / networked streams).
   * Using readFully() instead of read() is the contract-safe way to fill the buffer.
   */
  @Test
  void readExternalSurvivesShortReadInput() throws Exception {
    final DocumentType type = database.getSchema().createDocumentType("Doc");
    type.createProperty("id", Type.LONG);

    final MutableDocument doc1 = database.newDocument("Doc").set("id", 100L, "name", "Alice", "score", 3.14);
    try (final ByteArrayOutputStream arrayOut = new ByteArrayOutputStream();
        final ObjectOutput buffer = new ObjectOutputStream(arrayOut)) {
      doc1.writeExternal(buffer);
      buffer.flush();

      final MutableDocument doc2 = database.newDocument("Doc");
      try (final ByteArrayInputStream arrayIn = new ByteArrayInputStream(arrayOut.toByteArray());
          final ObjectInputStream rawIn = new ObjectInputStream(arrayIn)) {
        doc2.readExternal(new ThrottledObjectInput(rawIn));
        assertThat(doc2.toMap()).isEqualTo(doc1.toMap());
      }
    }
  }

  /**
   * ObjectInput wrapper that returns at most one byte per read(byte[],int,int) call,
   * simulating the short-read behaviour permitted on compressed or network-backed streams.
   * readFully delegates through this same throttled path and therefore must loop to completion.
   */
  private static final class ThrottledObjectInput implements ObjectInput {
    private final ObjectInput delegate;

    ThrottledObjectInput(final ObjectInput delegate) {
      this.delegate = delegate;
    }

    @Override
    public int read() throws IOException {
      return delegate.read();
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
      if (off < 0 || len < 0 || len > b.length - off)
        throw new IndexOutOfBoundsException();
      if (len == 0)
        return 0;
      return delegate.read(b, off, 1);
    }

    @Override
    public int read(final byte[] b) throws IOException {
      return read(b, 0, b.length);
    }

    @Override
    public void readFully(final byte[] b) throws IOException {
      readFully(b, 0, b.length);
    }

    @Override
    public void readFully(final byte[] b, final int off, final int len) throws IOException {
      if (off < 0 || len < 0 || len > b.length - off)
        throw new IndexOutOfBoundsException();
      int total = 0;
      while (total < len) {
        final int n = read(b, off + total, len - total);
        if (n < 0)
          throw new EOFException();
        total += n;
      }
    }

    @Override
    public int skipBytes(final int n) throws IOException { return delegate.skipBytes(n); }

    @Override
    public boolean readBoolean() throws IOException { return delegate.readBoolean(); }

    @Override
    public byte readByte() throws IOException { return delegate.readByte(); }

    @Override
    public int readUnsignedByte() throws IOException { return delegate.readUnsignedByte(); }

    @Override
    public short readShort() throws IOException { return delegate.readShort(); }

    @Override
    public int readUnsignedShort() throws IOException { return delegate.readUnsignedShort(); }

    @Override
    public char readChar() throws IOException { return delegate.readChar(); }

    @Override
    public int readInt() throws IOException { return delegate.readInt(); }

    @Override
    public long readLong() throws IOException { return delegate.readLong(); }

    @Override
    public float readFloat() throws IOException { return delegate.readFloat(); }

    @Override
    public double readDouble() throws IOException { return delegate.readDouble(); }

    @Override
    public String readLine() throws IOException { return delegate.readLine(); }

    @Override
    public String readUTF() throws IOException { return delegate.readUTF(); }

    @Override
    public Object readObject() throws ClassNotFoundException, IOException { return delegate.readObject(); }

    @Override
    public long skip(final long n) throws IOException { return delegate.skip(n); }

    @Override
    public int available() throws IOException { return delegate.available(); }

    @Override
    public void close() throws IOException { delegate.close(); }
  }

  @Test
  void documentTransient() throws Exception {
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
  void documentPersistent() throws Exception {
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
  void vertexTransient() throws Exception {
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
  void vertexPersistent() throws Exception {
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
  void documentWithNullPropertyDoesNotDesync() throws Exception {
    database.getSchema().createDocumentType("Doc");

    // Null-valued property must not inflate the written count - otherwise readExternal desyncs
    final MutableDocument doc1 = database.newDocument("Doc").set("id", 100L).set("nullProp", (Object) null).set("name", "John");
    try (final ByteArrayOutputStream arrayOut = new ByteArrayOutputStream();
        final ObjectOutput out = new ObjectOutputStream(arrayOut)) {
      doc1.writeExternal(out);
      out.flush();

      final MutableDocument doc2 = database.newDocument("Doc");
      try (final ByteArrayInputStream arrayIn = new ByteArrayInputStream(arrayOut.toByteArray());
          final ObjectInput in = new ObjectInputStream(arrayIn)) {
        doc2.readExternal(in);
        // Null properties are not serialized; the non-null ones must come back intact
        final Map<String, Object> result = doc2.toMap();
        assertThat(result).containsEntry("id", 100L).containsEntry("name", "John").doesNotContainKey("nullProp");
      }
    }
  }

  @Test
  void documentAllNullPropertiesDeserializesEmpty() throws Exception {
    database.getSchema().createDocumentType("Doc");

    final MutableDocument doc1 = database.newDocument("Doc").set("a", (Object) null).set("b", (Object) null);
    try (final ByteArrayOutputStream arrayOut = new ByteArrayOutputStream();
        final ObjectOutput out = new ObjectOutputStream(arrayOut)) {
      doc1.writeExternal(out);
      out.flush();

      final MutableDocument doc2 = database.newDocument("Doc");
      try (final ByteArrayInputStream arrayIn = new ByteArrayInputStream(arrayOut.toByteArray());
          final ObjectInput in = new ObjectInputStream(arrayIn)) {
        doc2.readExternal(in);
        // toMap(false) excludes @cat/@type metadata - only user properties should be empty
        assertThat(doc2.toMap(false)).isEmpty();
      }
    }
  }

  @Test
  void edgePersistent() throws Exception {
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
