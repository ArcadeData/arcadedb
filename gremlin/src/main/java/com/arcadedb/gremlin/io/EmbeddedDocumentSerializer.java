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
package com.arcadedb.gremlin.io;

import com.arcadedb.database.EmbeddedDocument;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.DataType;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.io.binary.types.CustomTypeSerializer;

import java.io.IOException;
import java.util.Map;

/**
 * GraphBinary custom-type serializer for an ArcadeDB {@link EmbeddedDocument}, the value of an EMBEDDED property (issue #13043).
 * Without it, any Gremlin Server traversal whose result carries such a value fails with "Serializer for type
 * com.arcadedb.database.ImmutableEmbeddedDocument not found" and the whole result is lost.
 * <p>
 * The document goes on the wire as the map {@link EmbeddedDocument#toMap(boolean) toMap(true)} answers: its own properties and
 * the {@code @type} / {@code @cat} metadata, the way the HTTP endpoint returns it. The engine's own object graph (the
 * {@code database} the document belongs to) never reaches the wire, and the client reads a plain {@link Map}: it needs no
 * database to rebuild a document from. Nested values (a list or a map of embedded documents, an embedded document in an embedded
 * document) go through the same registry one by one.
 * <p>
 * The serializer is registered for the {@link EmbeddedDocument} interface, which covers the immutable and the mutable
 * implementations alike. It is typed on {@link Object} because it writes an {@link EmbeddedDocument} and reads a {@link Map}.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
public class EmbeddedDocumentSerializer implements CustomTypeSerializer<Object> {
  public static final String TYPE_NAME = "arcadedb.EmbeddedDocument";

  @Override
  public String getTypeName() {
    return TYPE_NAME;
  }

  @Override
  public DataType getDataType() {
    return DataType.CUSTOM;
  }

  @Override
  public Object read(final Buffer buffer, final GraphBinaryReader context) throws IOException {
    // FULLY-QUALIFIED FORM: THE {type_code}{type_name} HAVE ALREADY BEEN CONSUMED BY THE READER
    return readValue(buffer, context, true);
  }

  @Override
  public Object readValue(final Buffer buffer, final GraphBinaryReader context, final boolean nullable) throws IOException {
    if (nullable) {
      final byte valueFlag = buffer.readByte();
      if ((valueFlag & 1) == 1)
        return null;
    }
    return context.readValue(buffer, Map.class, false);
  }

  @Override
  public void write(final Object value, final Buffer buffer, final GraphBinaryWriter context) throws IOException {
    // FULLY-QUALIFIED FORM: THE {type_code}{type_name} HAVE ALREADY BEEN WRITTEN BY THE WRITER
    writeValue(value, buffer, context, true);
  }

  @Override
  public void writeValue(final Object value, final Buffer buffer, final GraphBinaryWriter context, final boolean nullable)
      throws IOException {
    if (value == null) {
      if (!nullable)
        throw new IOException("Unexpected null value when nullable is false");
      context.writeValueFlagNull(buffer);
      return;
    }

    if (nullable)
      context.writeValueFlagNone(buffer);

    context.writeValue(((EmbeddedDocument) value).toMap(true), buffer, false);
  }
}
