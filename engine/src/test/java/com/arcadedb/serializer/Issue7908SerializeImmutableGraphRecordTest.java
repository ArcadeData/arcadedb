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
import com.arcadedb.database.Record;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.ImmutableEdge;
import com.arcadedb.graph.ImmutableLightEdge;
import com.arcadedb.graph.ImmutableVertex;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Externalizable;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression for issue #7908: {@code JavaBinarySerializer.writeExternal} branched on the INTERFACES {@code Vertex}
 * and {@code Edge} but cast to the concrete MUTABLE classes {@code MutableVertex}/{@code MutableEdge}. Every graph
 * record the engine hands back - from a scan, a query or {@code lookupByRID} - is an {@code ImmutableVertex},
 * {@code ImmutableEdge} or {@code ImmutableLightEdge}, so Java-serializing one threw a {@link ClassCastException}
 * from inside {@code writeObject}, while the identical document path worked. The narrowing bought nothing: the
 * head-chunk pointers are declared on {@code VertexInternal} and the endpoints on {@code Edge} itself.
 * <p>
 * Only the WRITE half was affected, which is why the round-trips below read back into a mutable instance:
 * {@code readExternal} rejects a non-{@code MutableDocument} target up front.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7908SerializeImmutableGraphRecordTest extends TestHelper {

  private static final String VERTEX_TYPE = "Issue7908Node";
  private static final String EDGE_TYPE   = "Issue7908Link";
  private static final String LIGHT_TYPE  = "Issue7908LightLink";

  private RID v1Rid;
  private RID v2Rid;
  private RID edgeRid;

  @Override
  protected void beginTest() {
    final VertexType vertexType = database.getSchema().createVertexType(VERTEX_TYPE);
    vertexType.createProperty("id", Type.LONG);
    final EdgeType edgeType = database.getSchema().createEdgeType(EDGE_TYPE);
    edgeType.createProperty("weight", Type.LONG);
    database.getSchema().createEdgeType(LIGHT_TYPE);

    database.transaction(() -> {
      final MutableVertex v1 = database.newVertex(VERTEX_TYPE).set("id", 1L, "name", "John");
      v1.save();
      final MutableVertex v2 = database.newVertex(VERTEX_TYPE).set("id", 2L, "name", "Jay");
      v2.save();

      final MutableEdge edge = v1.newEdge(EDGE_TYPE, v2).set("weight", 7L);
      edge.save();

      v1.newLightEdge(LIGHT_TYPE, v2);

      v1Rid = v1.getIdentity();
      v2Rid = v2.getIdentity();
      edgeRid = edge.getIdentity();
    });
  }

  @Test
  void anImmutableVertexReadBackFromTheDatabaseSerializes() {
    database.transaction(() -> {
      final Vertex loaded = database.lookupByRID(v1Rid, true).asVertex();
      // The premise of the issue: what a read hands back is NOT a MutableVertex.
      assertThat(loaded).isInstanceOf(ImmutableVertex.class);
      assertThat(loaded).isNotInstanceOf(MutableVertex.class);

      // The exact shape of the repro: a plain ObjectOutputStream.writeObject, which is what a cache, a session
      // replication layer or an RMI/Spark-style transport does. This is where the ClassCastException came from.
      assertThat(javaSerialize(loaded)).isNotEmpty();

      final MutableVertex restored = database.newVertex(VERTEX_TYPE);
      roundTrip(loaded, restored);

      assertThat(restored.getIdentity()).isEqualTo(v1Rid);
      assertThat((Long) restored.get("id")).isEqualTo(1L);
      assertThat((String) restored.get("name")).isEqualTo("John");
      assertThat(restored.getOutEdgesHeadChunk()).isEqualTo(((VertexInternal) loaded).getOutEdgesHeadChunk());
      assertThat(restored.getInEdgesHeadChunk()).isEqualTo(((VertexInternal) loaded).getInEdgesHeadChunk());
    });
  }

  @Test
  void anImmutableEdgeReadBackFromTheDatabaseSerializes() {
    database.transaction(() -> {
      final Edge loaded = database.lookupByRID(edgeRid, true).asEdge();
      assertThat(loaded).isInstanceOf(ImmutableEdge.class);
      assertThat(loaded).isNotInstanceOf(MutableEdge.class);

      assertThat(javaSerialize(loaded)).isNotEmpty();

      final MutableEdge restored = new MutableEdge(database, (EdgeType) database.getSchema().getType(EDGE_TYPE), null);
      roundTrip(loaded, restored);

      assertThat(restored.getIdentity()).isEqualTo(edgeRid);
      assertThat((Long) restored.get("weight")).isEqualTo(7L);
      assertThat(restored.getOut()).isEqualTo(v1Rid);
      assertThat(restored.getIn()).isEqualTo(v2Rid);
    });
  }

  /**
   * A LIGHTWEIGHT edge takes the same {@code instanceof Edge} arm, and {@code newLightEdge} hands the immutable
   * form back directly - so this one never had a mutable instance to hide the defect behind.
   */
  @Test
  void anImmutableLightEdgeSerializes() {
    database.transaction(() -> {
      final Edge light = database.lookupByRID(v1Rid, true).asVertex()
          .getEdges(Vertex.DIRECTION.OUT, LIGHT_TYPE).iterator().next();
      assertThat(light).isInstanceOf(ImmutableLightEdge.class);

      assertThat(javaSerialize(light)).isNotEmpty();
    });
  }

  /**
   * The iteration path named in the issue, which is what a scan or a query goes through.
   */
  @Test
  void everyVertexHandedBackByAScanSerializes() {
    database.transaction(() -> {
      int count = 0;
      for (final Iterator<Record> it = database.iterateType(VERTEX_TYPE, false); it.hasNext(); ) {
        assertThat(javaSerialize(it.next())).isNotEmpty();
        ++count;
      }
      assertThat(count).isEqualTo(2);
    });
  }

  /**
   * A document was always fine; kept so a future narrowing of the document arm is caught by the same class.
   */
  @Test
  void anImmutableDocumentStillSerializes() {
    database.getSchema().getOrCreateDocumentType("Issue7908Doc");
    database.transaction(() -> {
      final RID rid = database.newDocument("Issue7908Doc").set("id", 1L).save().getIdentity();
      assertThat(javaSerialize(database.lookupByRID(rid, true))).isNotEmpty();
    });
  }

  private static byte[] javaSerialize(final Object record) {
    try (final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        final ObjectOutput out = new ObjectOutputStream(bytes)) {
      out.writeObject(record);
      out.flush();
      return bytes.toByteArray();
    } catch (final Exception e) {
      throw new IllegalStateException("Cannot Java-serialize " + record.getClass().getName(), e);
    }
  }

  /**
   * {@code writeExternal}/{@code readExternal} directly, the way the sibling {@code JavaBinarySerializerTest} does:
   * {@code readObject} cannot be used here because the immutable classes have no public no-arg constructor, and the
   * read half is not what this issue is about.
   */
  private static void roundTrip(final Object source, final Externalizable target) {
    try (final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        final ObjectOutput out = new ObjectOutputStream(bytes)) {
      ((Externalizable) source).writeExternal(out);
      out.flush();

      try (final ByteArrayInputStream in = new ByteArrayInputStream(bytes.toByteArray());
          final ObjectInput objectIn = new ObjectInputStream(in)) {
        target.readExternal(objectIn);
      }
    } catch (final Exception e) {
      throw new IllegalStateException("Cannot round-trip " + source.getClass().getName(), e);
    }
  }
}
