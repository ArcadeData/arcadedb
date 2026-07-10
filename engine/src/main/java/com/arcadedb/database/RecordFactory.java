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

import com.arcadedb.database.Record;
import com.arcadedb.exception.DatabaseMetadataException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.EdgeSegment;
import com.arcadedb.graph.ImmutableEdge;
import com.arcadedb.graph.ImmutableVertex;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableEdgeSegment;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.LocalEdgeType;
import com.arcadedb.schema.LocalVertexType;
import com.arcadedb.schema.VertexType;

public class RecordFactory {
  public Record newImmutableRecord(final Database database, final DocumentType type, final RID rid, final byte recordType) {
    return switch (recordType) {
      case Document.RECORD_TYPE -> new ImmutableDocument(database, type, rid, null);
      case Vertex.RECORD_TYPE -> new ImmutableVertex(database, type, rid, null);
      case Edge.RECORD_TYPE -> new ImmutableEdge(database, type, rid, null);
      case EdgeSegment.RECORD_TYPE -> new MutableEdgeSegment(database, rid, null);
      case EmbeddedDocument.RECORD_TYPE -> new ImmutableEmbeddedDocument(database, type, null, null);
      default -> throw new DatabaseMetadataException("Cannot find record type '" + recordType + "'");
    };
  }

  public Record newImmutableRecord(final Database database, final DocumentType type, final RID rid, final Binary content, final EmbeddedModifier modifier) {
    final byte recordType = content.getByte();

    return switch (recordType) {
      case Document.RECORD_TYPE -> new ImmutableDocument(database, type, rid, content);
      case Vertex.RECORD_TYPE -> new ImmutableVertex(database, type, rid, content);
      case Edge.RECORD_TYPE -> new ImmutableEdge(database, type, rid, content);
      case EdgeSegment.RECORD_TYPE -> new MutableEdgeSegment(database, rid, content);
      case EmbeddedDocument.RECORD_TYPE -> new ImmutableEmbeddedDocument(database, type, content, modifier);
      default -> throw new DatabaseMetadataException("Cannot find record type '" + recordType + "'");
    };
  }

  public Record newMutableRecord(final Database database, final DocumentType type) {
    if (type instanceof LocalVertexType vertexType) return new MutableVertex(database, vertexType, null);
    if (type instanceof LocalEdgeType edgeType) return new MutableEdge(database, edgeType, null);
    return new MutableDocument(database, type, null);
  }

  public Record newMutableRecord(final Database database, final DocumentType type, final RID rid, final Binary content, final EmbeddedModifier modifier) {
    final int pos = content.position();
    final byte recordType = content.getByte();
    content.position(pos);

    return switch (recordType) {
      case Document.RECORD_TYPE -> new MutableDocument(database, type, rid, content);
      case Vertex.RECORD_TYPE -> new MutableVertex(database, (VertexType) type, rid);
      case Edge.RECORD_TYPE -> new MutableEdge(database, (EdgeType) type, rid);
      case EdgeSegment.RECORD_TYPE -> new MutableEdgeSegment(database, rid);
      case EmbeddedDocument.RECORD_TYPE -> new MutableEmbeddedDocument(database, type, content, modifier);
      default -> throw new DatabaseMetadataException("Cannot find record type '" + recordType + "'");
    };
  }
}
