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
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.InvalidClassException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9842: {@code JavaBinarySerializer.writeExternal} took its property list from {@code Document.toMap()}, which is
 * {@code toMap(true)} and so INCLUDES the metadata ({@code @cat}, {@code @type}, {@code @rid} and, for an edge, {@code @in} and
 * {@code @out}). They were written into the stream as properties, on top of the identity the method already writes in its header
 * and the endpoints it writes again in the trailing block of a vertex or an edge: a one-property edge was streamed as six
 * properties, and {@code MutableDocument.fromMap} decodes every one of them on the way back only to discard each {@code @}-prefixed
 * entry.
 * <p>
 * The other half of the report - that a record declares {@code Externalizable} but {@code ObjectInputStream.readObject} can never
 * build one, because the JDK needs a public no-argument constructor and a record needs a database - is the contract issue #186
 * settled on: the caller creates a database-bound record of the right type and asks IT to read the stream. The tests below pin both
 * sides of that, so the limit is stated by the build and not discovered in another process.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9842ExternalizedRecordStreamTest extends TestHelper {

  /** What {@code writeExternal} put in the stream, decoded the way {@code readExternal} reads it. */
  private record Stream(RID identity, List<String> propertyNames, RID firstPointer, RID secondPointer) {
  }

  private static byte[] externalize(final Document record) throws IOException {
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (final ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      ((Externalizable) record).writeExternal(out);
    }
    return bytes.toByteArray();
  }

  private Stream decode(final byte[] bytes, final boolean hasTrailingPointers) throws IOException {
    try (final ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
      final RID identity = database.newRID(in.readInt(), in.readLong());
      final int count = in.readInt();
      final List<String> names = new ArrayList<>(count);
      for (int i = 0; i < count; i++) {
        names.add(in.readUTF());
        in.readFully(new byte[in.readInt()]);
      }
      if (!hasTrailingPointers)
        return new Stream(identity, names, null, null);
      return new Stream(identity, names, database.newRID(in.readInt(), in.readLong()), database.newRID(in.readInt(), in.readLong()));
    }
  }

  @Test
  void aDocumentStreamCarriesItsPropertiesAndNoMetadata() throws Exception {
    database.getSchema().createDocumentType("Doc").createProperty("n", Type.INTEGER);
    database.setAutoTransaction(true);
    final MutableDocument doc = database.newDocument("Doc").set("n", 1).set("s", "x");
    doc.save();

    final Stream stream = decode(externalize(doc), false);

    assertThat(stream.identity()).isEqualTo(doc.getIdentity());
    assertThat(stream.propertyNames()).as("only the properties of the record, not @cat/@type/@rid").containsExactlyInAnyOrder("n", "s");
  }

  @Test
  void aVertexStreamCarriesItsPropertiesAndNoMetadata() throws Exception {
    database.getSchema().createVertexType("V").createProperty("n", Type.INTEGER);
    database.setAutoTransaction(true);
    final MutableVertex vertex = database.newVertex("V").set("n", 1);
    vertex.save();

    final Stream stream = decode(externalize(vertex), true);

    assertThat(stream.propertyNames()).containsExactly("n");
  }

  /** The endpoints are written once, in the trailing block, and not a second time as {@code @in}/{@code @out} properties. */
  @Test
  void anEdgeStreamCarriesItsEndpointsOnceAndNoMetadata() throws Exception {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("E").createProperty("w", Type.INTEGER);
    database.setAutoTransaction(true);
    final MutableVertex from = database.newVertex("V");
    from.save();
    final MutableVertex to = database.newVertex("V");
    to.save();
    final Edge edge = from.newEdge("E", to, "w", 7);

    final Stream stream = decode(externalize(edge), true);

    assertThat(stream.propertyNames()).as("a one-property edge is one property in the stream").containsExactly("w");
    assertThat(stream.firstPointer()).isEqualTo(from.getIdentity());
    assertThat(stream.secondPointer()).isEqualTo(to.getIdentity());
  }

  /** What the supported path preserves: a record created for the type, bound to the database, and asked to read the stream. */
  @Test
  void aRecordReadsItsStreamBackIntoABoundRecordOfTheSameType() throws Exception {
    database.getSchema().createDocumentType("Doc").createProperty("n", Type.INTEGER);
    database.setAutoTransaction(true);
    final MutableDocument doc = database.newDocument("Doc").set("n", 1).set("s", "x");
    doc.save();

    final MutableDocument target = database.newDocument("Doc");
    try (final ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(externalize(doc)))) {
      target.readExternal(in);
    }

    assertThat(target.getIdentity()).isEqualTo(doc.getIdentity());
    assertThat(target.toMap(false)).isEqualTo(doc.toMap(false));
    assertThat(target.getString("s")).isEqualTo("x");
  }

  /**
   * A stream written before the fix carries {@code @cat}, {@code @type} and {@code @rid} among its properties. It must still read:
   * {@code fromMap} discards every {@code @}-prefixed entry, so the metadata never reaches the record and the identity is the
   * one of the header. The stream is built by hand here, in the old layout, because the writer no longer produces it.
   */
  @Test
  void aStreamWrittenWithMetadataPropertiesStillReads() throws Exception {
    database.getSchema().createDocumentType("Doc").createProperty("n", Type.INTEGER);
    final DatabaseInternal db = (DatabaseInternal) database;
    final RID identity = db.newRID(1, 0);

    final Map<String, Object> properties = new LinkedHashMap<>();
    properties.put("n", 1);
    properties.put("s", "x");
    properties.put("@cat", "d");
    properties.put("@type", "Doc");
    properties.put("@rid", "#1:0");

    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (final ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeInt(identity.getBucketId());
      out.writeLong(identity.getPosition());
      out.writeInt(properties.size());
      final Binary buffer = new Binary();
      for (final Map.Entry<String, Object> property : properties.entrySet()) {
        final byte type = BinaryTypes.getTypeFromValue(property.getValue(), null);
        buffer.clear();
        buffer.putByte(type);
        db.getSerializer().serializeValue(db, buffer, type, property.getValue());
        buffer.flip();
        out.writeUTF(property.getKey());
        out.writeInt(buffer.size());
        out.write(buffer.getContent(), 0, buffer.size());
      }
    }

    final MutableDocument target = database.newDocument("Doc");
    try (final ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      target.readExternal(in);
    }

    assertThat(target.getIdentity()).isEqualTo(identity);
    assertThat(target.toMap(false)).containsOnlyKeys("n", "s");
    assertThat(target.getInteger("n")).isEqualTo(1);
    assertThat(target.getString("s")).isEqualTo("x");
    assertThat(target.getTypeName()).as("the type is the caller's, not the stream's").isEqualTo("Doc");
  }

  /**
   * The limit the contract states: {@code ObjectInputStream.readObject} cannot build a record from scratch. The JDK instantiates
   * an {@code Externalizable} through a public no-argument constructor before it calls {@code readExternal}, and a record has
   * none - it is a handle on a page of an open database, so there is nothing to construct it from. Pinned so that lifting the
   * limit is a decision somebody takes, not an accident.
   */
  @Test
  void objectInputStreamCannotBuildARecordFromScratch() throws Exception {
    database.getSchema().createDocumentType("Doc");
    final MutableDocument doc = database.newDocument("Doc").set("n", 1);

    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (final ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(doc);
    }

    assertThatThrownBy(() -> {
      try (final ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
        in.readObject();
      }
    }).isInstanceOf(InvalidClassException.class).hasMessageContaining("no valid constructor");
  }
}
