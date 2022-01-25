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
import com.arcadedb.exception.DatabaseMetadataException;
import com.arcadedb.graph.*;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.BinaryTypes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 **/
class RecordFactoryTest extends TestHelper {

  @Test
  void newImmutableRecord() {
    final RID EMPTY_RID = new RID(database, 0, 0);

    final DocumentType documentType = database.getSchema().createDocumentType("Document");
    Record document = ((DatabaseInternal) database).getRecordFactory().newImmutableRecord(database, documentType, EMPTY_RID, Document.RECORD_TYPE);
    Assertions.assertTrue(document instanceof Document);

    final VertexType vertexType = database.getSchema().createVertexType("Vertex");
    Record vertex = ((DatabaseInternal) database).getRecordFactory().newImmutableRecord(database, vertexType, EMPTY_RID, Vertex.RECORD_TYPE);
    Assertions.assertTrue(vertex instanceof Vertex);

    final EdgeType edgeType = database.getSchema().createEdgeType("Edge");
    Record edge = ((DatabaseInternal) database).getRecordFactory().newImmutableRecord(database, edgeType, EMPTY_RID, Edge.RECORD_TYPE);
    Assertions.assertTrue(edge instanceof Edge);

    final Record edgeSegment = ((DatabaseInternal) database).getRecordFactory().newImmutableRecord(database, null, EMPTY_RID, EdgeSegment.RECORD_TYPE);
    Assertions.assertTrue(edgeSegment instanceof EdgeSegment);

    final DocumentType embeddedDocumentType = database.getSchema().createDocumentType("EmbeddedDocument");
    Record embeddedDocument = ((DatabaseInternal) database).getRecordFactory()
        .newImmutableRecord(database, embeddedDocumentType, EMPTY_RID, EmbeddedDocument.RECORD_TYPE);
    Assertions.assertTrue(embeddedDocument instanceof EmbeddedDocument);

    try {
      ((DatabaseInternal) database).getRecordFactory().newImmutableRecord(database, embeddedDocumentType, EMPTY_RID, (byte) 'Z');
      Assertions.fail();
    } catch (DatabaseMetadataException e) {
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
    Record document = ((DatabaseInternal) database).getRecordFactory().newImmutableRecord(database, documentType, EMPTY_RID, binary, null);
    Assertions.assertTrue(document instanceof Document);

    binary.clear();
    binary.putByte(Vertex.RECORD_TYPE);
    binary.putInt(0);
    binary.putLong(0);
    binary.putInt(1);
    binary.putLong(1);
    binary.flip();

    final VertexType vertexType = database.getSchema().createVertexType("Vertex");
    Record vertex = ((DatabaseInternal) database).getRecordFactory().newImmutableRecord(database, vertexType, EMPTY_RID, binary, null);
    Assertions.assertTrue(vertex instanceof Vertex);

    binary.clear();
    binary.putByte(Edge.RECORD_TYPE);
    ((DatabaseInternal) database).getSerializer().serializeValue(database, binary, BinaryTypes.TYPE_COMPRESSED_RID, EMPTY_RID);
    ((DatabaseInternal) database).getSerializer().serializeValue(database, binary, BinaryTypes.TYPE_COMPRESSED_RID, EMPTY_RID);
    binary.flip();

    final EdgeType edgeType = database.getSchema().createEdgeType("Edge");
    Record edge = ((DatabaseInternal) database).getRecordFactory().newImmutableRecord(database, edgeType, EMPTY_RID, binary, null);
    Assertions.assertTrue(edge instanceof Edge);

    binary.clear();
    binary.putByte(EdgeSegment.RECORD_TYPE);
    binary.flip();

    final Record edgeSegment = ((DatabaseInternal) database).getRecordFactory().newImmutableRecord(database, null, EMPTY_RID, binary, null);
    Assertions.assertTrue(edgeSegment instanceof EdgeSegment);

    binary.clear();
    binary.putByte(EmbeddedDocument.RECORD_TYPE);
    binary.flip();

    final DocumentType embeddedDocumentType = database.getSchema().createDocumentType("EmbeddedDocument");
    Record embeddedDocument = ((DatabaseInternal) database).getRecordFactory().newImmutableRecord(database, embeddedDocumentType, EMPTY_RID, binary, null);
    Assertions.assertTrue(embeddedDocument instanceof EmbeddedDocument);

    binary.clear();
    binary.putByte((byte) 'Z');
    binary.flip();

    try {
      ((DatabaseInternal) database).getRecordFactory().newImmutableRecord(database, embeddedDocumentType, EMPTY_RID, binary, null);
      Assertions.fail();
    } catch (DatabaseMetadataException e) {
      // EXPECTED
    }
  }

  @Test
  void newModifiableRecord() {
    final RID EMPTY_RID = new RID(database, 0, 0);

    final Binary binary = new Binary();
    binary.putByte(Document.RECORD_TYPE);
    binary.flip();

    final DocumentType documentType = database.getSchema().createDocumentType("Document");
    Record document = ((DatabaseInternal) database).getRecordFactory().newModifiableRecord(database, documentType, EMPTY_RID, binary, null);
    Assertions.assertTrue(document instanceof MutableDocument);

    binary.clear();
    binary.putByte(Vertex.RECORD_TYPE);
    binary.putInt(0);
    binary.putLong(0);
    binary.putInt(1);
    binary.putLong(1);
    binary.flip();

    final VertexType vertexType = database.getSchema().createVertexType("Vertex");
    Record vertex = ((DatabaseInternal) database).getRecordFactory().newModifiableRecord(database, vertexType, EMPTY_RID, binary, null);
    Assertions.assertTrue(vertex instanceof MutableVertex);

    binary.clear();
    binary.putByte(Edge.RECORD_TYPE);
    ((DatabaseInternal) database).getSerializer().serializeValue(database, binary, BinaryTypes.TYPE_COMPRESSED_RID, EMPTY_RID);
    ((DatabaseInternal) database).getSerializer().serializeValue(database, binary, BinaryTypes.TYPE_COMPRESSED_RID, EMPTY_RID);
    binary.flip();

    final EdgeType edgeType = database.getSchema().createEdgeType("Edge");
    Record edge = ((DatabaseInternal) database).getRecordFactory().newModifiableRecord(database, edgeType, EMPTY_RID, binary, null);
    Assertions.assertTrue(edge instanceof MutableEdge);

    binary.clear();
    binary.putByte(EdgeSegment.RECORD_TYPE);
    binary.flip();

    final Record edgeSegment = ((DatabaseInternal) database).getRecordFactory().newModifiableRecord(database, null, EMPTY_RID, binary, null);
    Assertions.assertTrue(edgeSegment instanceof MutableEdgeSegment);

    binary.clear();
    binary.putByte(EmbeddedDocument.RECORD_TYPE);
    binary.flip();

    final DocumentType embeddedDocumentType = database.getSchema().createDocumentType("EmbeddedDocument");
    Record embeddedDocument = ((DatabaseInternal) database).getRecordFactory().newModifiableRecord(database, embeddedDocumentType, EMPTY_RID, binary, null);
    Assertions.assertTrue(embeddedDocument instanceof MutableEmbeddedDocument);

    binary.clear();
    binary.putByte((byte) 'Z');
    binary.flip();

    try {
      ((DatabaseInternal) database).getRecordFactory().newModifiableRecord(database, embeddedDocumentType, EMPTY_RID, binary, null);
      Assertions.fail();
    } catch (DatabaseMetadataException e) {
      // EXPECTED
    }
  }
}
