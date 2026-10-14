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
package com.arcadedb.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.exception.SerializationException;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.BinaryTypes;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * #9985: the truncated buffer guard #4432 added protected {@link ImmutableVertex} only. {@link ImmutableEdge}, whose
 * prefix has no fixed length and which is on the live read path, and both mutable shapes through
 * {@code setBuffer()}, let a bare {@code BufferUnderflowException} with a null message escape instead, naming neither
 * the record nor the file.
 * <p>
 * The guarantee is the one #4432 could have made and did not: a buffer cut anywhere inside the prefix, fed to ALL FOUR
 * record shapes, raises a {@link SerializationException} naming the record. The cut is made twice - on an array of
 * the cut length, which underflows, and on the full array with a shorter declared size, which reads past that size
 * without failing at all unless the parser compares against it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9985TruncatedRecordPrefixTest extends TestHelper {
  private static final String VERTEX_TYPE = "Issue9985Vertex";
  private static final String EDGE_TYPE   = "Issue9985Edge";

  private RID    vertexRid;
  private RID    edgeRid;
  private byte[] vertexContent;
  private byte[] edgeContent;

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType(VERTEX_TYPE);
    database.getSchema().createEdgeType(EDGE_TYPE);

    database.transaction(() -> {
      // Vertex positions past the single-byte varint range in every bucket of the type, so each compressed RID of the
      // edge prefix spans several bytes and a cut can land inside one of them
      for (int i = 0; i < 2_000; i++)
        database.newVertex(VERTEX_TYPE).set("i", i).save();
      final MutableVertex a = database.newVertex(VERTEX_TYPE).set("name", "a").save();
      final MutableVertex b = database.newVertex(VERTEX_TYPE).set("name", "b").save();
      final MutableEdge edge = a.newEdge(EDGE_TYPE, b).set("weight", 7).save();
      vertexRid = a.getIdentity();
      edgeRid = edge.getIdentity();
    });

    vertexContent = recordContent(vertexRid);
    edgeContent = recordContent(edgeRid);
  }

  private byte[] recordContent(final RID rid) {
    final Binary buffer = database.getSchema().getBucketById(rid.getBucketId()).getRecord(rid);
    return buffer.toByteArray();
  }

  private VertexType vertexType() {
    return (VertexType) database.getSchema().getType(VERTEX_TYPE);
  }

  private EdgeType edgeType() {
    return (EdgeType) database.getSchema().getType(EDGE_TYPE);
  }

  @Test
  void theUntruncatedRecordsStillReadInEveryShape() {
    final ImmutableVertex immutableVertex = new ImmutableVertex(database, vertexType(), vertexRid, new Binary(vertexContent.clone()));
    assertThat(immutableVertex.getString("name")).isEqualTo("a");
    assertThat(immutableVertex.getOutEdgesHeadChunk()).isNotNull();
    final ImmutableEdge immutableEdge = new ImmutableEdge(database, edgeType(), edgeRid, new Binary(edgeContent.clone()));
    assertThat(immutableEdge.getInteger("weight")).isEqualTo(7);
    assertThat(immutableEdge.getOut()).isEqualTo(vertexRid);

    // setBuffer() installs the prefix; the property map of a mutable record is left alone by design
    final MutableVertex vertex = new MutableVertex(database, vertexType(), vertexRid);
    vertex.setBuffer(new Binary(vertexContent.clone()));
    assertThat(vertex.getOutEdgesHeadChunk()).isEqualTo(immutableVertex.getOutEdgesHeadChunk());

    final MutableEdge edge = new MutableEdge(database, edgeType(), edgeRid);
    edge.setBuffer(new Binary(edgeContent.clone()));
    assertThat(edge.getOut()).isEqualTo(vertexRid);
    assertThat(edge.getIn()).isEqualTo(immutableEdge.getIn());
  }

  @Test
  void immutableVertexNamesTheRecord() {
    assertEveryCutIsNamed(vertexRid, vertexContent, 25, buffer -> new ImmutableVertex(database, vertexType(), vertexRid, buffer));
  }

  @Test
  void immutableEdgeNamesTheRecord() {
    assertEveryCutIsNamed(edgeRid, edgeContent, edgePrefixSize(), buffer -> new ImmutableEdge(database, edgeType(), edgeRid, buffer));
  }

  @Test
  void mutableVertexSetBufferNamesTheRecord() {
    assertEveryCutIsNamed(vertexRid, vertexContent, 25, buffer -> {
      final MutableVertex vertex = new MutableVertex(database, vertexType(), vertexRid);
      vertex.setBuffer(buffer);
      return vertex;
    });
  }

  @Test
  void mutableEdgeSetBufferNamesTheRecord() {
    assertEveryCutIsNamed(edgeRid, edgeContent, edgePrefixSize(), buffer -> {
      final MutableEdge edge = new MutableEdge(database, edgeType(), edgeRid);
      edge.setBuffer(buffer);
      return edge;
    });
  }

  /** The secondary half: a reload of the mutable shapes re-derives the prefix from the buffer it installs. */
  @Test
  void aReloadOfTheMutableShapesReadsThePrefixOfTheNewBuffer() {
    final MutableVertex vertex = new MutableVertex(database, vertexType(), vertexRid);
    vertex.setBuffer(new Binary(vertexContent.clone()));
    final RID outHead = vertex.getOutEdgesHeadChunk();
    vertex.setOutEdgesHeadChunk(null);
    vertex.reload();
    assertThat(vertex.getOutEdgesHeadChunk()).isEqualTo(outHead);
    assertThat(vertex.getString("name")).isEqualTo("a");

    final MutableEdge edge = new MutableEdge(database, edgeType(), edgeRid, vertexRid, vertexRid);
    edge.reload();
    assertThat(edge.getIn()).isNotEqualTo(vertexRid);
    assertThat(edge.getOut()).isEqualTo(vertexRid);
    assertThat(edge.getInteger("weight")).isEqualTo(7);
  }

  private int edgePrefixSize() {
    final Binary buffer = new Binary(edgeContent.clone());
    buffer.position(1);
    final DatabaseInternal internal = (DatabaseInternal) database;
    internal.getSerializer().deserializeValue(database, buffer, BinaryTypes.TYPE_COMPRESSED_RID, null);
    internal.getSerializer().deserializeValue(database, buffer, BinaryTypes.TYPE_COMPRESSED_RID, null);
    // the fixture's RIDs need more than one byte each, so a cut can fall inside a RID and not only between them
    assertThat(buffer.position()).isGreaterThan(5);
    return buffer.position();
  }

  private static void assertEveryCutIsNamed(final RID rid, final byte[] content, final int prefixSize,
      final Function<Binary, Object> shape) {
    for (int cut = 0; cut < prefixSize; cut++) {
      final int size = cut;
      // an array of the cut length: the read underflows
      assertThatThrownBy(() -> shape.apply(new Binary(Arrays.copyOf(content, size))))
          .as("cut at %d of a %d byte prefix", size, prefixSize)
          .isInstanceOf(SerializationException.class)
          .hasMessageContaining(rid.toString())
          .hasMessageContaining("truncated or corrupted");
      // the whole array with a shorter declared size: the read does not underflow, it reads past the size
      assertThatThrownBy(() -> shape.apply(new Binary(content.clone(), size)))
          .as("declared size %d of a %d byte prefix", size, prefixSize)
          .isInstanceOf(SerializationException.class)
          .hasMessageContaining(rid.toString())
          .hasMessageContaining("truncated or corrupted");
    }
  }
}
