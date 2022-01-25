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

import com.arcadedb.exception.DatabaseMetadataException;
import com.arcadedb.graph.*;
import com.arcadedb.schema.DocumentType;

public class RecordFactory {
  public Record newImmutableRecord(final Database database, final DocumentType type, final RID rid, final byte recordType) {
    switch (recordType) {
    case Document.RECORD_TYPE:
      return new ImmutableDocument(database, type, rid, null);
    case Vertex.RECORD_TYPE:
      return new ImmutableVertex(database, type, rid, null);
    case Edge.RECORD_TYPE:
      return new ImmutableEdge(database, type, rid, null);
    case EdgeSegment.RECORD_TYPE:
      return new MutableEdgeSegment(database, rid, null);
    case EmbeddedDocument.RECORD_TYPE:
      return new ImmutableEmbeddedDocument(database, type, null, null);
    }
    throw new DatabaseMetadataException("Cannot find record type '" + recordType + "'");
  }

  public Record newImmutableRecord(final Database database, final DocumentType type, final RID rid, final Binary content, final EmbeddedModifier modifier) {
    final byte recordType = content.getByte();

    switch (recordType) {
    case Document.RECORD_TYPE:
      return new ImmutableDocument(database, type, rid, content);
    case Vertex.RECORD_TYPE:
      return new ImmutableVertex(database, type, rid, content);
    case Edge.RECORD_TYPE:
      return new ImmutableEdge(database, type, rid, content);
    case EdgeSegment.RECORD_TYPE:
      return new MutableEdgeSegment(database, rid, content);
    case EmbeddedDocument.RECORD_TYPE:
      return new ImmutableEmbeddedDocument(database, type, content, modifier);
    }
    throw new DatabaseMetadataException("Cannot find record type '" + recordType + "'");
  }

  public Record newModifiableRecord(final Database database, final DocumentType type, final RID rid, final Binary content, final EmbeddedModifier modifier) {
    final int pos = content.position();
    final byte recordType = content.getByte();
    content.position(pos);

    switch (recordType) {
    case Document.RECORD_TYPE:
      return new MutableDocument(database, type, rid, content);
    case Vertex.RECORD_TYPE:
      return new MutableVertex(database, type, rid);
    case Edge.RECORD_TYPE:
      return new MutableEdge(database, type, rid);
    case EdgeSegment.RECORD_TYPE:
      return new MutableEdgeSegment(database, rid);
    case EmbeddedDocument.RECORD_TYPE:
      return new MutableEmbeddedDocument(database, type, content, modifier);
    }
    throw new DatabaseMetadataException("Cannot find record type '" + recordType + "'");
  }
}
