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

import com.arcadedb.database.BaseRecord;
import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.EmbeddedDatabase;
import com.arcadedb.database.EmbeddedDocument;
import com.arcadedb.database.EmbeddedModifier;
import com.arcadedb.database.EmbeddedModifierProperty;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.engine.Dictionary;
import com.arcadedb.exception.SerializationException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.EdgeSegment;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.Result;

import java.lang.reflect.*;
import java.math.*;
import java.util.*;
import java.util.logging.*;

/**
 * Default serializer implementation.
 * <p>
 * TODO: check on storing all the property ids at the beginning of the buffer, so to partial deserialize values is much more
 * <p>
 * TODO: efficient, because it doesn't need to unmarshall all the values first.
 */
public class BinarySerializer {
  private final BinaryComparator comparator = new BinaryComparator(this);

  public BinaryComparator getComparator() {
    return comparator;
  }

  public Binary serialize(final Database database, final Record record) {
    switch (record.getRecordType()) {
    case Document.RECORD_TYPE:
    case EmbeddedDocument.RECORD_TYPE:
      return serializeDocument(database, (MutableDocument) record);
    case Vertex.RECORD_TYPE:
      return serializeVertex(database, (MutableVertex) record);
    case Edge.RECORD_TYPE:
      return serializeEdge(database, (MutableEdge) record);
    case EdgeSegment.RECORD_TYPE:
      return serializeEdgeContainer(database, (EdgeSegment) record);
    default:
      throw new IllegalArgumentException("Cannot serialize a record of type=" + record.getRecordType());
    }
  }

  public Binary serializeDocument(final Database database, final Document document) {
    Binary header = ((BaseRecord) document).getBuffer();

    final boolean serializeProperties;
    if (header == null || (document instanceof MutableDocument && ((MutableDocument) document).isDirty())) {
      header = ((EmbeddedDatabase) database).getContext().getTemporaryBuffer1();
      header.putByte(document.getRecordType()); // RECORD TYPE
      serializeProperties = true;
    } else {
      // COPY THE CONTENT (THE BUFFER IS IMMUTABLE)
      header = header.copy();
      header.position(Binary.BYTE_SERIALIZED_SIZE);
      serializeProperties = false;
    }

    if (serializeProperties)
      return serializeProperties(database, document, header, ((EmbeddedDatabase) database).getContext().getTemporaryBuffer2());

    return header;
  }

  public Binary serializeVertex(final Database database, final VertexInternal vertex) {
    Binary header = ((BaseRecord) vertex).getBuffer();

    final boolean serializeProperties;
    if (header == null || (vertex instanceof MutableVertex && ((MutableVertex) vertex).isDirty())) {
      header = ((EmbeddedDatabase) database).getContext().getTemporaryBuffer1();
      header.putByte(vertex.getRecordType()); // RECORD TYPE
      serializeProperties = true;
    } else {
      // COPY THE CONTENT (THE BUFFER IS IMMUTABLE)
      header = header.copy();
      header.position(Binary.BYTE_SERIALIZED_SIZE);
      serializeProperties = false;
    }

    // WRITE OUT AND IN EDGES POINTER FIRST, THEN SERIALIZE THE VERTEX PROPERTIES (AS A DOCUMENT)
    final RID outEdges = vertex.getOutEdgesHeadChunk();
    if (outEdges != null) {
      header.putInt(outEdges.getBucketId());
      header.putLong(outEdges.getPosition());
    } else {
      header.putInt(-1);
      header.putLong(-1);
    }

    final RID inEdges = vertex.getInEdgesHeadChunk();
    if (inEdges != null) {
      header.putInt(inEdges.getBucketId());
      header.putLong(inEdges.getPosition());
    } else {
      header.putInt(-1);
      header.putLong(-1);
    }

    if (serializeProperties)
      return serializeProperties(database, vertex, header, ((EmbeddedDatabase) database).getContext().getTemporaryBuffer2());

    return header;
  }

  public Binary serializeEdge(final Database database, final Edge edge) {
    Binary header = ((BaseRecord) edge).getBuffer();

    final boolean serializeProperties;
    if (header == null || (edge instanceof MutableEdge && ((MutableEdge) edge).isDirty())) {
      header = ((EmbeddedDatabase) database).getContext().getTemporaryBuffer1();
      header.putByte(edge.getRecordType()); // RECORD TYPE
      serializeProperties = true;
    } else {
      // COPY THE CONTENT (THE BUFFER IS IMMUTABLE)
      header = header.copy();
      header.position(Binary.BYTE_SERIALIZED_SIZE);
      serializeProperties = false;
    }

    // WRITE OUT AND IN EDGES POINTER FIRST, THEN SERIALIZE THE VERTEX PROPERTIES (AS A DOCUMENT)
    serializeValue(database, header, BinaryTypes.TYPE_COMPRESSED_RID, edge.getOut());
    serializeValue(database, header, BinaryTypes.TYPE_COMPRESSED_RID, edge.getIn());

    if (serializeProperties)
      return serializeProperties(database, edge, header, ((EmbeddedDatabase) database).getContext().getTemporaryBuffer2());

    return header;
  }

  public Binary serializeEdgeContainer(final Database database, final EdgeSegment record) {
    return record.getContent();
  }

  public Set<String> getPropertyNames(final Database database, final Binary buffer) {
    buffer.getInt(); // HEADER-SIZE
    final int properties = (int) buffer.getUnsignedNumber();
    final Set<String> result = new LinkedHashSet<>(properties);

    for (int i = 0; i < properties; ++i) {
      final int nameId = (int) buffer.getUnsignedNumber();
      buffer.getUnsignedNumber(); //contentPosition
      final String name = database.getSchema().getDictionary().getNameById(nameId);
      result.add(name);
    }

    return result;
  }

  public Map<String, Object> deserializeProperties(final Database database, final Binary buffer, final EmbeddedModifier embeddedModifier,
      final String... fieldNames) {
    final int headerEndOffset = buffer.getInt();
    final int properties = (int) buffer.getUnsignedNumber();

    if (properties < 0)
      throw new SerializationException("Error on deserialize record. It may be corrupted (properties=" + properties + ")");

    final Map<String, Object> values = new LinkedHashMap<>(properties);

    int lastHeaderPosition;

    final int[] fieldIds = new int[fieldNames.length];

    final Dictionary dictionary = database.getSchema().getDictionary();
    for (int i = 0; i < fieldNames.length; ++i)
      fieldIds[i] = dictionary.getIdByName(fieldNames[i], false);

    for (int i = 0; i < properties; ++i) {
      final int nameId = (int) buffer.getUnsignedNumber();
      final int contentPosition = (int) buffer.getUnsignedNumber();

      lastHeaderPosition = buffer.position();

      if (fieldIds.length > 0) {
        boolean found = false;
        // FILTER BY FIELD
        for (int f : fieldIds)
          if (f == nameId) {
            found = true;
            break;
          }

        if (!found)
          continue;
      }

      final String propertyName = dictionary.getNameById(nameId);

      buffer.position(headerEndOffset + contentPosition);

      final byte type = buffer.getByte();

      final EmbeddedModifierProperty propertyModifier =
          embeddedModifier != null ? new EmbeddedModifierProperty(embeddedModifier.getOwner(), propertyName) : null;

      Object propertyValue = deserializeValue(database, buffer, type, propertyModifier);

      if (type == BinaryTypes.TYPE_COMPRESSED_STRING)
        propertyValue = dictionary.getNameById(((Long) propertyValue).intValue());

      values.put(propertyName, propertyValue);

      buffer.position(lastHeaderPosition);

      if (fieldIds.length > 0 && values.size() >= fieldIds.length)
        // ALL REQUESTED PROPERTIES ALREADY FOUND
        break;
    }

    return values;
  }

  public void serializeValue(final Database database, Binary content, final byte type, Object value) {
    if( value==null)
      return;

    switch (type) {
    case BinaryTypes.TYPE_NULL:
      break;
    case BinaryTypes.TYPE_COMPRESSED_STRING:
      content.putUnsignedNumber((Integer) value);
      break;
    case BinaryTypes.TYPE_BINARY:
      if (value instanceof byte[])
        content.putBytes((byte[]) value);
      else if (value instanceof Binary)
        content.putBytes(((Binary) value).getContent());
      break;
    case BinaryTypes.TYPE_STRING:
      if (value instanceof byte[])
        content.putBytes((byte[]) value);
      else
        content.putString(value.toString());
      break;
    case BinaryTypes.TYPE_BYTE:
      content.putByte((Byte) value);
      break;
    case BinaryTypes.TYPE_BOOLEAN:
      content.putByte((byte) ((Boolean) value ? 1 : 0));
      break;
    case BinaryTypes.TYPE_SHORT:
      content.putNumber(((Number) value).shortValue());
      break;
    case BinaryTypes.TYPE_INT:
      content.putNumber(((Number) value).intValue());
      break;
    case BinaryTypes.TYPE_LONG:
      content.putNumber(((Number) value).longValue());
      break;
    case BinaryTypes.TYPE_FLOAT:
      final int fg = Float.floatToIntBits(((Number) value).floatValue());
      content.putNumber(fg);
      break;
    case BinaryTypes.TYPE_DOUBLE:
      final long dg = Double.doubleToLongBits(((Number) value).doubleValue());
      content.putNumber(dg);
      break;
    case BinaryTypes.TYPE_DATE:
    case BinaryTypes.TYPE_DATETIME:
      content.putUnsignedNumber(((Date) value).getTime());
      break;
    case BinaryTypes.TYPE_DECIMAL:
      content.putNumber(((BigDecimal) value).scale());
      content.putBytes(((BigDecimal) value).unscaledValue().toByteArray());
      break;
    case BinaryTypes.TYPE_COMPRESSED_RID: {
      final RID rid = ((Identifiable) value).getIdentity();
      content.putNumber(rid.getBucketId());
      content.putNumber(rid.getPosition());
      break;
    }
    case BinaryTypes.TYPE_RID: {
      if (value instanceof Result)
        // COMING FROM A QUERY
        value = ((Result) value).getElement().get();

      final RID rid = ((Identifiable) value).getIdentity();
      content.putInt(rid.getBucketId());
      content.putLong(rid.getPosition());
      break;
    }
    case BinaryTypes.TYPE_UUID: {
      final UUID uuid = (UUID) value;
      content.putNumber(uuid.getMostSignificantBits());
      content.putNumber(uuid.getLeastSignificantBits());
      break;
    }
    case BinaryTypes.TYPE_LIST: {
      if (value instanceof Collection) {
        final Collection list = (Collection) value;
        content.putUnsignedNumber(list.size());
        for (Iterator it = list.iterator(); it.hasNext(); ) {
          final Object entryValue = it.next();
          final byte entryType = BinaryTypes.getTypeFromValue(entryValue);
          content.putByte(entryType);
          serializeValue(database, content, entryType, entryValue);
        }
      } else if (value instanceof Object[]) {
        // ARRAY
        final Object[] array = (Object[]) value;
        content.putUnsignedNumber(array.length);
        for (Object entryValue : array) {
          final byte entryType = BinaryTypes.getTypeFromValue(entryValue);
          content.putByte(entryType);
          serializeValue(database, content, entryType, entryValue);
        }
      } else if (value instanceof Iterable) {
        final Iterable iter = (Iterable) value;

        final List list = new ArrayList();
        for (Iterator it = iter.iterator(); it.hasNext(); )
          list.add(it.next());

        content.putUnsignedNumber(list.size());
        for (Iterator it = list.iterator(); it.hasNext(); ) {
          final Object entryValue = it.next();
          final byte entryType = BinaryTypes.getTypeFromValue(entryValue);
          content.putByte(entryType);
          serializeValue(database, content, entryType, entryValue);
        }
      } else {
        // ARRAY
        final int length = Array.getLength(value);
        content.putUnsignedNumber(length);
        for (int i = 0; i < length; ++i) {
          final Object entryValue = Array.get(value, i);
          final byte entryType = BinaryTypes.getTypeFromValue(entryValue);
          content.putByte(entryType);
          serializeValue(database, content, entryType, entryValue);
        }
      }
      break;
    }
    case BinaryTypes.TYPE_MAP: {
      final Map<Object, Object> map = (Map<Object, Object>) value;
      content.putUnsignedNumber(map.size());
      for (Map.Entry<Object, Object> entry : map.entrySet()) {
        // WRITE THE KEY
        final Object entryKey = entry.getKey();
        final byte entryKeyType = BinaryTypes.getTypeFromValue(entryKey);
        content.putByte(entryKeyType);
        serializeValue(database, content, entryKeyType, entryKey);

        // WRITE THE VALUE
        final Object entryValue = entry.getValue();
        final byte entryValueType = BinaryTypes.getTypeFromValue(entryValue);
        content.putByte(entryValueType);
        serializeValue(database, content, entryValueType, entryValue);
      }
      break;
    }
    case BinaryTypes.TYPE_EMBEDDED: {
      final Document document = (Document) value;
      final long schemaId = database.getSchema().getDictionary().getIdByName(document.getTypeName(), false);
      if (schemaId == -1)
        throw new IllegalArgumentException("Cannot find type '" + document.getTypeName() + "' declared in embedded document");
      content.putUnsignedNumber(schemaId);

      final Binary header = new Binary(8192);
      header.setAllocationChunkSize(2048);
      final Binary body = new Binary(8192);
      body.setAllocationChunkSize(2048);

      header.putByte(EmbeddedDocument.RECORD_TYPE);
      serializeProperties(database, document, header, body);

      content.putUnsignedNumber(header.size());
      content.append(header);
      break;
    }

    default:
      LogManager.instance().log(this, Level.INFO, "Error on serializing value '" + value + "', type not supported");
    }
  }

  public Object deserializeValue(final Database database, final Binary content, final byte type, final EmbeddedModifier embeddedModifier) {
    Object value;
    switch (type) {
    case BinaryTypes.TYPE_NULL:
      value = null;
      break;
    case BinaryTypes.TYPE_STRING:
      value = content.getString();
      break;
    case BinaryTypes.TYPE_COMPRESSED_STRING:
      value = content.getUnsignedNumber();
      break;
    case BinaryTypes.TYPE_BINARY:
      value = content.getBytes();
      break;
    case BinaryTypes.TYPE_BYTE:
      value = content.getByte();
      break;
    case BinaryTypes.TYPE_BOOLEAN:
      value = content.getByte() == 1;
      break;
    case BinaryTypes.TYPE_SHORT:
      value = (short) content.getNumber();
      break;
    case BinaryTypes.TYPE_INT:
      value = (int) content.getNumber();
      break;
    case BinaryTypes.TYPE_LONG:
      value = content.getNumber();
      break;
    case BinaryTypes.TYPE_FLOAT:
      value = Float.intBitsToFloat((int) content.getNumber());
      break;
    case BinaryTypes.TYPE_DOUBLE:
      value = Double.longBitsToDouble(content.getNumber());
      break;
    case BinaryTypes.TYPE_DATE:
    case BinaryTypes.TYPE_DATETIME:
      value = new Date(content.getUnsignedNumber());
      break;
    case BinaryTypes.TYPE_DECIMAL:
      final int scale = (int) content.getNumber();
      final byte[] unscaledValue = content.getBytes();
      value = new BigDecimal(new BigInteger(unscaledValue), scale);
      break;
    case BinaryTypes.TYPE_COMPRESSED_RID:
      value = new RID(database, (int) content.getNumber(), content.getNumber());
      break;
    case BinaryTypes.TYPE_RID:
      value = new RID(database, content.getInt(), content.getLong());
      break;
    case BinaryTypes.TYPE_UUID:
      value = new UUID(content.getNumber(), content.getNumber());
      break;
    case BinaryTypes.TYPE_LIST: {
      final int count = (int) content.getUnsignedNumber();
      final List list = new ArrayList(count);
      for (int i = 0; i < count; ++i) {
        final byte entryType = content.getByte();
        list.add(deserializeValue(database, content, entryType, embeddedModifier));
      }
      value = list;
      break;
    }
    case BinaryTypes.TYPE_MAP: {
      final int count = (int) content.getUnsignedNumber();
      final Map<Object, Object> map = new HashMap<>(count);
      for (int i = 0; i < count; ++i) {
        final byte entryKeyType = content.getByte();
        final Object entryKey = deserializeValue(database, content, entryKeyType, embeddedModifier);

        final byte entryValueType = content.getByte();
        final Object entryValue = deserializeValue(database, content, entryValueType, embeddedModifier);

        map.put(entryKey, entryValue);
      }
      value = map;
      break;
    }
    case BinaryTypes.TYPE_EMBEDDED: {
      final String typeName = database.getSchema().getDictionary().getNameById((int) content.getUnsignedNumber());

      final int embeddedObjectSize = (int) content.getUnsignedNumber();

      final Binary embeddedBuffer = content.slice(content.position(), embeddedObjectSize);

      value = ((DatabaseInternal) database).getRecordFactory()
          .newImmutableRecord(database, database.getSchema().getType(typeName), null, embeddedBuffer, embeddedModifier);

      content.position(content.position() + embeddedObjectSize);
      break;
    }

    default:
      LogManager.instance().log(this, Level.INFO, "Error on deserializing value of type " + type);
      value = null;
    }
    return value;
  }

  public Binary serializeProperties(final Database database, final Document record, final Binary header, final Binary content) {
    final int headerSizePosition = header.position();
    header.putInt(0); // TEMPORARY PLACEHOLDER FOR HEADER SIZE

    final Map<String, Object> properties = record.toMap();
    header.putUnsignedNumber(properties.size());

    final Dictionary dictionary = database.getSchema().getDictionary();

    for (Map.Entry<String, Object> entry : properties.entrySet()) {
      // WRITE PROPERTY ID FROM THE DICTIONARY
      header.putUnsignedNumber(dictionary.getIdByName(entry.getKey(), true));

      Object value = entry.getValue();

      final int startContentPosition = content.position();

      byte type = BinaryTypes.getTypeFromValue(value);

      if (value != null && type == BinaryTypes.TYPE_STRING) {
        final int id = dictionary.getIdByName((String) value, false);
        if (id > -1) {
          // WRITE THE COMPRESSED STRING
          type = BinaryTypes.TYPE_COMPRESSED_STRING;
          value = id;
        }
      }

      content.putByte(type);
      serializeValue(database, content, type, value);

      // WRITE PROPERTY CONTENT POSITION
      header.putUnsignedNumber(startContentPosition);
    }

    content.flip();

    final int headerEndOffset = header.position();

    header.append(content);

    // UPDATE HEADER SIZE
    header.putInt(headerSizePosition, headerEndOffset);

    header.position(header.size());
    header.flip();
    return header;
  }

  public Binary serializeProperties(final Database database, final Map<String, Object> properties, final Binary header, final Binary content) {
    final int headerSizePosition = header.position();
    header.putInt(0); // TEMPORARY PLACEHOLDER FOR HEADER SIZE

    header.putUnsignedNumber(properties.size());

    final Dictionary dictionary = database.getSchema().getDictionary();

    for (Map.Entry<String, Object> entry : properties.entrySet()) {
      // WRITE PROPERTY ID FROM THE DICTIONARY
      header.putUnsignedNumber(dictionary.getIdByName(entry.getKey(), true));

      Object value = entry.getValue();

      final int startContentPosition = content.position();

      byte type = BinaryTypes.getTypeFromValue(value);

      if (value != null && type == BinaryTypes.TYPE_STRING) {
        final int id = dictionary.getIdByName((String) value, false);
        if (id > -1) {
          // WRITE THE COMPRESSED STRING
          type = BinaryTypes.TYPE_COMPRESSED_STRING;
          value = id;
        }
      }

      content.putByte(type);
      serializeValue(database, content, type, value);

      // WRITE PROPERTY CONTENT POSITION
      header.putUnsignedNumber(startContentPosition);
    }

    content.flip();

    final int headerEndOffset = header.position();

    header.append(content);

    // UPDATE HEADER SIZE
    header.putInt(headerSizePosition, headerEndOffset);

    header.position(header.size());
    header.flip();
    return header;
  }
}
