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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Record;
import com.arcadedb.exception.DatabaseMetadataException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.EdgeSegment;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableEdgeSegment;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.BinaryTypes;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 **/
class RecordFactoryTest extends TestHelper {

  @Test
  void newImmutableRecord() {
    final RID EMPTY_RID = new RID(database, 0, 0);

    final DocumentType documentType = database.getSchema().createDocumentType("Document");
    final Record document = ((DatabaseInternal) database).getRecordFactory().newImmutableRecord(database, documentType, EMPTY_RID, Document.RECORD_TYPE);
    assertThat(document instanceof Document).isTrue();

    final VertexType vertexType = database.getSchema().createVertexType("Vertex");
    final Record vertex = ((DatabaseInternal) database).getRecordFactory().newImmutableRecord(database, vertexType, EMPTY_RID, Vertex.RECORD_TYPE);
    assertThat(vertex instanceof Vertex).isTrue();

    final EdgeType edgeType = database.getSchema().createEdgeType("Edge");
    final Record edge = ((DatabaseInternal) database).getRecordFactory().newImmutableRecord(database, edgeType, EMPTY_RID, Edge.RECORD_TYPE);
    assertThat(edge instanceof Edge).isTrue();

    final Record edgeSegment = ((DatabaseInternal) database).getRecordFactory().newImmutableRecord(database, null, EMPTY_RID, EdgeSegment.RECORD_TYPE);
    assertThat(edgeSegment instanceof EdgeSegment).isTrue();

    final DocumentType embeddedDocumentType = database.getSchema().createDocumentType("EmbeddedDocument");
    final Record embeddedDocument = ((DatabaseInternal) database).getRecordFactory()
        .newImmutableRecord(database, embeddedDocumentType, EMPTY_RID, EmbeddedDocument.RECORD_TYPE);
    assertThat(embeddedDocument instanceof EmbeddedDocument).isTrue();

    try {
      ((DatabaseInternal) database).getRecordFactory().newImmutableRecord(database, embeddedDocumentType, EMPTY_RID, (byte) 'Z');
      fail("");
    } catch (final DatabaseMetadataException e) {
      // EXPECTED
    }
  }

  @Test
  void testNewImmutableRecord() {
    final RID EMPTY_RID = new RID(database, 0, 0);

    final Binary binary = new Binary();
    binary.putByte(Document.RECORD_TYPE);
    binary.flip();

    final DocumentType documentType = database.getSchema().createDocumentType("Document");
    final Record document = ((DatabaseInternal) database).getRecordFactory().newImmutableRecord(database, documentType, EMPTY_RID, binary, null);
    assertThat(document instanceof Document).isTrue();

    binary.clear();
    binary.putByte(Vertex.RECORD_TYPE);
    binary.putInt(0);
    binary.putLong(0);
    binary.putInt(1);
    binary.putLong(1);
    binary.flip();

    final VertexType vertexType = database.getSchema().createVertexType("Vertex");
    final Record vertex = ((DatabaseInternal) database).getRecordFactory().newImmutableRecord(database, vertexType, EMPTY_RID, binary, null);
    assertThat(vertex instanceof Vertex).isTrue();

    binary.clear();
    binary.putByte(Edge.RECORD_TYPE);
    ((DatabaseInternal) database).getSerializer().serializeValue(database, binary, BinaryTypes.TYPE_COMPRESSED_RID, EMPTY_RID);
    ((DatabaseInternal) database).getSerializer().serializeValue(database, binary, BinaryTypes.TYPE_COMPRESSED_RID, EMPTY_RID);
    binary.flip();

    final EdgeType edgeType = database.getSchema().createEdgeType("Edge");
    final Record edge = ((DatabaseInternal) database).getRecordFactory().newImmutableRecord(database, edgeType, EMPTY_RID, binary, null);
    assertThat(edge instanceof Edge).isTrue();

    binary.clear();
    binary.putByte(EdgeSegment.RECORD_TYPE);
    binary.flip();

    final Record edgeSegment = ((DatabaseInternal) database).getRecordFactory().newImmutableRecord(database, null, EMPTY_RID, binary, null);
    assertThat(edgeSegment instanceof EdgeSegment).isTrue();

    binary.clear();
    binary.putByte(EmbeddedDocument.RECORD_TYPE);
    binary.flip();

    final DocumentType embeddedDocumentType = database.getSchema().createDocumentType("EmbeddedDocument");
    final Record embeddedDocument = ((DatabaseInternal) database).getRecordFactory()
        .newImmutableRecord(database, embeddedDocumentType, EMPTY_RID, binary, null);
    assertThat(embeddedDocument instanceof EmbeddedDocument).isTrue();

    binary.clear();
    binary.putByte((byte) 'Z');
    binary.flip();

    try {
      ((DatabaseInternal) database).getRecordFactory().newImmutableRecord(database, embeddedDocumentType, EMPTY_RID, binary, null);
      fail("");
    } catch (final DatabaseMetadataException e) {
      // EXPECTED
    }
  }

  @Test
  void newMutableRecord() {
    final RID EMPTY_RID = new RID(database, 0, 0);

    final Binary binary = new Binary();
    binary.putByte(Document.RECORD_TYPE);
    binary.flip();

    final DocumentType documentType = database.getSchema().createDocumentType("Document");
    final Record document = ((DatabaseInternal) database).getRecordFactory().newMutableRecord(database, documentType, EMPTY_RID, binary, null);
    assertThat(document instanceof MutableDocument).isTrue();

    binary.clear();
    binary.putByte(Vertex.RECORD_TYPE);
    binary.putInt(0);
    binary.putLong(0);
    binary.putInt(1);
    binary.putLong(1);
    binary.flip();

    final VertexType vertexType = database.getSchema().createVertexType("Vertex");
    final Record vertex = ((DatabaseInternal) database).getRecordFactory().newMutableRecord(database, vertexType, EMPTY_RID, binary, null);
    assertThat(vertex instanceof MutableVertex).isTrue();

    binary.clear();
    binary.putByte(Edge.RECORD_TYPE);
    ((DatabaseInternal) database).getSerializer().serializeValue(database, binary, BinaryTypes.TYPE_COMPRESSED_RID, EMPTY_RID);
    ((DatabaseInternal) database).getSerializer().serializeValue(database, binary, BinaryTypes.TYPE_COMPRESSED_RID, EMPTY_RID);
    binary.flip();

    final EdgeType edgeType = database.getSchema().createEdgeType("Edge");
    final Record edge = ((DatabaseInternal) database).getRecordFactory().newMutableRecord(database, edgeType, EMPTY_RID, binary, null);
    assertThat(edge instanceof MutableEdge).isTrue();

    binary.clear();
    binary.putByte(EdgeSegment.RECORD_TYPE);
    binary.flip();

    final Record edgeSegment = ((DatabaseInternal) database).getRecordFactory().newMutableRecord(database, null, EMPTY_RID, binary, null);
    assertThat(edgeSegment instanceof MutableEdgeSegment).isTrue();

    binary.clear();
    binary.putByte(EmbeddedDocument.RECORD_TYPE);
    binary.flip();

    final DocumentType embeddedDocumentType = database.getSchema().createDocumentType("EmbeddedDocument");
    final Record embeddedDocument = ((DatabaseInternal) database).getRecordFactory().newMutableRecord(database, embeddedDocumentType, EMPTY_RID, binary, null);
    assertThat(embeddedDocument instanceof MutableEmbeddedDocument).isTrue();

    binary.clear();
    binary.putByte((byte) 'Z');
    binary.flip();

    try {
      ((DatabaseInternal) database).getRecordFactory().newMutableRecord(database, embeddedDocumentType, EMPTY_RID, binary, null);
      fail("");
    } catch (final DatabaseMetadataException e) {
      // EXPECTED
    }
  }
}
