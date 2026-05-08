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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.compression.CompressionFactory;
import com.arcadedb.compression.LZ4Compression;
import com.arcadedb.database.BaseRecord;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DataEncryption;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.EmbeddedDocument;
import com.arcadedb.database.EmbeddedModifier;
import com.arcadedb.database.EmbeddedModifierProperty;
import com.arcadedb.database.ExternalValueRecord;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.Dictionary;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.SerializationException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.EdgeSegment;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.VertexInternal;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.function.sql.geo.GeoUtils;
import com.arcadedb.database.BaseDocument;
import com.arcadedb.database.DocumentInternal;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.DateUtils;

import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.io.ShapeWriter;
import org.locationtech.spatial4j.io.SupportedFormats;
import org.locationtech.spatial4j.shape.Circle;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;

import java.lang.reflect.*;
import java.math.*;
import java.time.*;
import java.time.temporal.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

/**
 * Default serializer implementation.
 * <p>
 * TODO: check on storing all the property ids at the beginning of the buffer, so to partial deserialize values is much more
 * <p>
 * TODO: efficient, because it doesn't need to unmarshall all the values first.
 */
public class BinarySerializer {
  private final BinaryComparator comparator = new BinaryComparator();
  private       Class<?>         dateImplementation;
  private       Class<?>         dateTimeImplementation;
  private       DataEncryption   dataEncryption;

  // Cached WKT writer for fast Point serialization (avoid recreating writer for each Point)
  private static volatile ShapeWriter cachedWktWriter;

  /**
   * Process-cumulative count of records whose OLD buffer could not be parsed by
   * {@link #findExistingExternalRids}. Each parse failure means the record's external blobs (if any) were
   * NOT discovered and could be left as orphans in the paired bucket. CHECK DATABASE surfaces this counter
   * so an operator can see corruption-driven leaks accumulating without waiting for the next bucket scan.
   * Static so the value survives instance churn and tracks the JVM lifetime.
   */
  private static final AtomicLong externalRidScanFailures = new AtomicLong(0);

  /** Returns the JVM-cumulative count of {@link #findExistingExternalRids} parse failures since process start. */
  public static long getExternalRidScanFailures() {
    return externalRidScanFailures.get();
  }

  public BinarySerializer(final ContextConfiguration configuration) throws ClassNotFoundException {
    setDateImplementation(configuration.getValue(GlobalConfiguration.DATE_IMPLEMENTATION));
    setDateTimeImplementation(configuration.getValue(GlobalConfiguration.DATE_TIME_IMPLEMENTATION));
  }

  public Binary serialize(final DatabaseInternal database, final Record record) {
    return switch (record.getRecordType()) {
      case Document.RECORD_TYPE, EmbeddedDocument.RECORD_TYPE -> serializeDocument(database, (MutableDocument) record);
      case Vertex.RECORD_TYPE -> serializeVertex(database, (MutableVertex) record);
      case Edge.RECORD_TYPE -> serializeEdge(database, (MutableEdge) record);
      case EdgeSegment.RECORD_TYPE -> serializeEdgeContainer((EdgeSegment) record);
      case ExternalValueRecord.RECORD_TYPE -> ((ExternalValueRecord) record).getContent();
      default -> throw new IllegalArgumentException("Cannot serialize a record of type=" + record.getRecordType());
    };
  }

  public Binary serializeDocument(final DatabaseInternal database, final Document document) {
    Binary header = ((BaseRecord) document).getBuffer();

    final DatabaseContext.DatabaseContextTL context = database.getContext();

    final boolean serializeProperties;
    if (header == null || (document instanceof MutableDocument mutableDocument && mutableDocument.isDirty())) {
      header = context.getTemporaryBuffer1();
      header.putByte(document.getRecordType()); // RECORD TYPE
      serializeProperties = true;
    } else {
      // COPY THE CONTENT (THE BUFFER IS IMMUTABLE)
      header = header.copyOfContent();
      header.position(Binary.BYTE_SERIALIZED_SIZE);
      serializeProperties = false;
    }

    if (serializeProperties)
      return serializeProperties(database, document, header, context.getTemporaryBuffer2());

    return header;
  }

  public Binary serializeVertex(final DatabaseInternal database, final VertexInternal vertex) {
    Binary header = ((BaseRecord) vertex).getBuffer();

    final DatabaseContext.DatabaseContextTL context = database.getContext();

    final boolean serializeProperties;
    if (header == null || (vertex instanceof MutableVertex mutableVertex && mutableVertex.isDirty())) {
      header = context.getTemporaryBuffer1();
      header.putByte(vertex.getRecordType()); // RECORD TYPE
      serializeProperties = true;
    } else {
      // COPY THE CONTENT (THE BUFFER IS IMMUTABLE)
      header = header.copyOfContent();
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
      return serializeProperties(database, vertex, header, context.getTemporaryBuffer2());

    return header;
  }

  public Binary serializeEdge(final DatabaseInternal database, final Edge edge) {
    Binary header = ((BaseRecord) edge).getBuffer();

    final DatabaseContext.DatabaseContextTL context = database.getContext();

    final boolean serializeProperties;
    if (header == null || (edge instanceof MutableEdge mutableEdge && mutableEdge.isDirty())) {
      header = context.getTemporaryBuffer1();
      header.putByte(edge.getRecordType()); // RECORD TYPE
      serializeProperties = true;
    } else {
      // COPY THE CONTENT (THE BUFFER IS IMMUTABLE)
      header = header.copyOfContent();
      header.position(Binary.BYTE_SERIALIZED_SIZE);
      serializeProperties = false;
    }

    // WRITE OUT AND IN EDGES POINTER FIRST, THEN SERIALIZE THE VERTEX PROPERTIES (AS A DOCUMENT)
    serializeValue(database, header, BinaryTypes.TYPE_COMPRESSED_RID, edge.getOut());
    serializeValue(database, header, BinaryTypes.TYPE_COMPRESSED_RID, edge.getIn());

    if (serializeProperties)
      return serializeProperties(database, edge, header, context.getTemporaryBuffer2());

    return header;
  }

  public Binary serializeEdgeContainer(final EdgeSegment record) {
    return record.getContent();
  }

  public Set<String> getPropertyNames(final Database database, final Binary buffer, final RID rid) {
    final Set<String> result = new LinkedHashSet<>();
    try {
      buffer.getInt(); // HEADER-SIZE
      final int properties = (int) buffer.getUnsignedNumber();

      for (int i = 0; i < properties; ++i) {
        final int nameId = (int) buffer.getUnsignedNumber();
        buffer.getUnsignedNumber(); //contentPosition
        final String name = database.getSchema().getDictionary().getNameById(nameId);
        result.add(name);
      }
    } catch (Exception e) {
      LogManager.instance().log(this, Level.WARNING, "Possible corrupted record %s, returning %d names read so far", e, rid,
          result.size());
    }
    return result;
  }

  public Map<String, Object> deserializeProperties(final Database database, final Binary buffer,
      final EmbeddedModifier embeddedModifier, final RID rid, final String... fieldNames) {
    final Map<String, Object> values = new LinkedHashMap<>();
    try {
      final int initialPosition = buffer.position();
      final int headerEndOffset = buffer.getInt();
      if (headerEndOffset < 0)
        throw new SerializationException(
            "Error on deserialize record. It may be corrupted (headerEndOffset=" + headerEndOffset + " at position "
                + initialPosition + ")");

      final int properties = (int) buffer.getUnsignedNumber();

      if (properties < 0)
        throw new SerializationException("Error on deserialize record. It may be corrupted (properties=" + properties + ")");
      else if (properties == 0)
        // EMPTY: NOT FOUND
        return values;

      final int[] fieldIds = new int[fieldNames.length];

      final Dictionary dictionary = database.getSchema().getDictionary();
      for (int i = 0; i < fieldNames.length; ++i)
        fieldIds[i] = dictionary.getIdByName(fieldNames[i], false);

      for (int i = 0; i < properties; ++i) {
        final int nameId = (int) buffer.getUnsignedNumber();
        final int contentPosition = (int) buffer.getUnsignedNumber();

        final int lastHeaderPosition = buffer.position();

        if (fieldIds.length > 0) {
          boolean found = false;
          // FILTER BY FIELD
          for (final int f : fieldIds)
            if (f == nameId) {
              found = true;
              break;
            }

          if (!found)
            continue;
        }

        final String propertyName = dictionary.getNameById(nameId);

        // Per-property recovery: if one property's value is corrupted, skip it and keep the rest.
        // The header has been read already, so we can safely jump back and continue.
        try {
          buffer.position(headerEndOffset + contentPosition);

          final byte type = buffer.getByte();

          final EmbeddedModifierProperty propertyModifier =
              embeddedModifier != null ? new EmbeddedModifierProperty(embeddedModifier.getOwner(), propertyName) : null;

          final Object propertyValue;
          if (isExternalType(type)) {
            final int extBucketId = (int) buffer.getNumber();
            final long extPosition = buffer.getNumber();
            propertyValue = readExternalValue((DatabaseInternal) database, extBucketId, extPosition, propertyModifier,
                isExternalCompressedType(type));
          } else {
            propertyValue = deserializeValue(database, buffer, type, propertyModifier);
          }

          values.put(propertyName, propertyValue);
        } catch (Exception e) {
          LogManager.instance().log(this, Level.WARNING,
              "Skipping corrupted property '%s' in record %s: %s", propertyName, rid, e.getMessage());
        }

        buffer.position(lastHeaderPosition);

        if (fieldIds.length > 0 && values.size() >= fieldIds.length)
          // ALL REQUESTED PROPERTIES ALREADY FOUND
          break;
      }
    } catch (Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Possible corrupted record %s, returning %d properties read so far", e, rid,
          values.size());
    }
    return values;
  }

  public boolean hasProperty(final Database database, final Binary buffer, final String fieldName, final RID rid) {
    try {
      buffer.getInt(); // headerEndOffset
      final int properties = (int) buffer.getUnsignedNumber();
      if (properties < 0)
        throw new SerializationException("Error on deserialize record. It may be corrupted (properties=" + properties + ")");
      else if (properties == 0)
        // EMPTY: NOT FOUND
        return false;

      final int fieldId = database.getSchema().getDictionary().getIdByName(fieldName, false);

      for (int i = 0; i < properties; ++i) {
        if (fieldId == (int) buffer.getUnsignedNumber())
          return true;
        buffer.getUnsignedNumber(); // contentPosition
      }
    } catch (Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Possible corrupted record %s", e, rid);
    }

    return false;
  }

  public Object deserializeProperty(final Database database, final Binary buffer, final EmbeddedModifier embeddedModifier,
      final String fieldName, final RID rid) {
    try {
      final int headerEndOffset = buffer.getInt();
      final int properties = (int) buffer.getUnsignedNumber();

      if (properties < 0)
        throw new SerializationException("Error on deserialize record. It may be corrupted (properties=" + properties + ")");
      else if (properties == 0)
        // EMPTY: NOT FOUND
        return null;

      final Dictionary dictionary = database.getSchema().getDictionary();
      final int fieldId = dictionary.getIdByName(fieldName, false);

      for (int i = 0; i < properties; ++i) {
        final int nameId = (int) buffer.getUnsignedNumber();
        final int contentPosition = (int) buffer.getUnsignedNumber();

        if (fieldId != nameId)
          continue;

        buffer.position(headerEndOffset + contentPosition);

        final byte type = buffer.getByte();

        final EmbeddedModifierProperty propertyModifier =
            embeddedModifier != null ? new EmbeddedModifierProperty(embeddedModifier.getOwner(), fieldName) : null;

        if (isExternalType(type)) {
          final int extBucketId = (int) buffer.getNumber();
          final long extPosition = buffer.getNumber();
          return readExternalValue((DatabaseInternal) database, extBucketId, extPosition, propertyModifier,
              isExternalCompressedType(type));
        }

        return deserializeValue(database, buffer, type, propertyModifier);
      }
    } catch (Exception e) {
      LogManager.instance().log(this, Level.SEVERE, "Possible corrupted record %s", e, rid);
    }
    return null;
  }

  public void serializeValue(final Database database, final Binary serialized, final byte type, Object value) {
    serializeValue(database, serialized, type, value, true);
  }

  public void serializeValue(final Database database, final Binary serialized, final byte type, Object value,
      final boolean applyEncryption) {
    if (value == null)
      return;
    final boolean encrypt = applyEncryption && dataEncryption != null;
    Binary content = encrypt ? new Binary() : serialized;

    switch (type) {
    case BinaryTypes.TYPE_NULL:
      break;
    case BinaryTypes.TYPE_COMPRESSED_STRING:
      content.putUnsignedNumber((Integer) value);
      break;
    case BinaryTypes.TYPE_BINARY:
      if (value instanceof byte[] bytes)
        content.putBytes(bytes);
      else if (value instanceof Binary binary)
        content.putBytes(binary.getContent());
      break;
    case BinaryTypes.TYPE_COMPRESSED_GEOMETRY:
      serializeGeometryBinary(content, (Shape) value);
      break;
    case BinaryTypes.TYPE_STRING:
      if (value instanceof byte[] bytes)
        content.putBytes(bytes);
      else if (BinaryTypes.isGeoSpatialShape(value))
        content.putString(convertShapeToWKT(value));
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
      content.putNumber(Float.floatToIntBits(((Number) value).floatValue()));
      break;
    case BinaryTypes.TYPE_DOUBLE:
      content.putNumber(Double.doubleToLongBits(((Number) value).doubleValue()));
      break;
    case BinaryTypes.TYPE_DATE:
      if (value instanceof Date date)
        content.putUnsignedNumber(date.getTime() / DateUtils.MS_IN_A_DAY);
      else if (value instanceof LocalDate date)
        content.putUnsignedNumber(date.toEpochDay());
      break;
    case BinaryTypes.TYPE_DATETIME_SECOND:
    case BinaryTypes.TYPE_DATETIME:
    case BinaryTypes.TYPE_DATETIME_MICROS:
    case BinaryTypes.TYPE_DATETIME_NANOS:
      serializeDateTime(database, content, value, type);
      break;
    case BinaryTypes.TYPE_DECIMAL:
      content.putNumber(((BigDecimal) value).scale());
      content.putBytes(((BigDecimal) value).unscaledValue().toByteArray());
      break;
    case BinaryTypes.TYPE_COMPRESSED_RID: {
      final RID rid = ((Identifiable) value).getIdentity();
      serialized.putNumber(rid.getBucketId());
      serialized.putNumber(rid.getPosition());
      break;
    }
    case BinaryTypes.TYPE_RID: {
      if (value instanceof Result result)
        // COMING FROM A QUERY
        value = result.getElement().get();

      final RID rid = ((Identifiable) value).getIdentity();
      serialized.putInt(rid.getBucketId());
      serialized.putLong(rid.getPosition());
      break;
    }
    case BinaryTypes.TYPE_UUID: {
      final UUID uuid = (UUID) value;
      content.putNumber(uuid.getMostSignificantBits());
      content.putNumber(uuid.getLeastSignificantBits());
      break;
    }
    case BinaryTypes.TYPE_LIST: {
      switch (value) {
      case Collection<?> list -> serializeListEntries(database, content, list, list.size(), "list", applyEncryption);
      case Object[] array -> serializeListEntries(database, content, Arrays.asList(array), array.length, "array", applyEncryption);
      case Iterable<?> iter -> {
        final List<Object> list = new ArrayList<>();
        for (Object o : iter)
          list.add(o);
        serializeListEntries(database, content, list, list.size(), "iterable", applyEncryption);
      }
      default -> {
        // PRIMITIVE ARRAY (component type not matched by the cases above)
        final int length = Array.getLength(value);
        content.putUnsignedNumber(length);
        for (int i = 0; i < length; ++i) {
          final Object entryValue = Array.get(value, i);
          try {
            byte entryType = BinaryTypes.getTypeFromValue(entryValue, null);
            Object valueToWrite = entryValue;
            if (entryType == -1) {
              LogManager.instance()
                  .log(BinaryTypes.class, Level.WARNING,
                      "Cannot serialize entry in array of type %s, value %s. Stored as null",
                      entryValue.getClass(), entryValue);
              entryType = BinaryTypes.TYPE_NULL;
              valueToWrite = null;
            }
            content.putByte(entryType);
            serializeValue(database, content, entryType, valueToWrite, applyEncryption);
          } catch (Exception e) {
            LogManager.instance().log(this, Level.SEVERE, "Error on serializing array value for element %d = '%s'",
                i, entryValue);
            throw new SerializationException(
                "Error on serializing array value for element " + i + " = '" + entryValue + "'");
          }
        }
      }
      }
      break;
    }
    case BinaryTypes.TYPE_MAP: {
      final Dictionary dictionary = database.getSchema().getDictionary();

      if (value instanceof JSONObject object)
        value = object.toMap();

      final Map<Object, Object> map = (Map<Object, Object>) value;
      final int mapSize = map.size();
      // Pre-resolve types so the count matches what is written. A partially-written entry
      // (key without value) desyncs the reader and corrupts the record.
      // An invalid key drops the entry; an invalid value preserves the key and stores null.
      final Object[] keys = new Object[mapSize];
      final Object[] values = new Object[mapSize];
      final byte[] keyTypes = new byte[mapSize];
      final byte[] valueTypes = new byte[mapSize];
      int validCount = 0;
      for (final Map.Entry<Object, Object> entry : map.entrySet()) {
        try {
          Object entryKey = entry.getKey();
          byte entryKeyType = BinaryTypes.getTypeFromValue(entryKey, null);
          if (entryKeyType == -1) {
            LogManager.instance()
                .log(BinaryTypes.class, Level.WARNING,
                    "Cannot serialize entry key in map of type %s, value %s. The entry will be ignored",
                    entryKey.getClass(), entryKey);
            continue;
          }

          Object entryValue = entry.getValue();
          byte entryValueType = BinaryTypes.getTypeFromValue(entryValue, null);
          if (entryValueType == -1) {
            LogManager.instance()
                .log(BinaryTypes.class, Level.WARNING,
                    "Cannot serialize entry value in map of type %s, value %s. Stored as null",
                    entryValue.getClass(), entryValue);
            entryValueType = BinaryTypes.TYPE_NULL;
            entryValue = null;
          }

          if (entryKey != null && entryKeyType == BinaryTypes.TYPE_STRING) {
            final int id = dictionary.getIdByName((String) entryKey, false);
            if (id > -1) {
              // COMPRESSED STRING AS MAP KEY
              entryKeyType = BinaryTypes.TYPE_COMPRESSED_STRING;
              entryKey = id;
            }
          }

          keys[validCount] = entryKey;
          values[validCount] = entryValue;
          keyTypes[validCount] = entryKeyType;
          valueTypes[validCount] = entryValueType;
          ++validCount;
        } catch (Exception e) {
          LogManager.instance().log(this, Level.SEVERE, "Error on serializing map value for key '%s' = '%s'",
              entry.getKey(), entry.getValue());
          throw new SerializationException(
              "Error on serializing map value for key '" + entry.getKey() + "' = '" + entry.getValue() + "'", e);
        }
      }
      content.putUnsignedNumber(validCount);
      for (int i = 0; i < validCount; ++i) {
        content.putByte(keyTypes[i]);
        serializeValue(database, content, keyTypes[i], keys[i], applyEncryption);
        content.putByte(valueTypes[i]);
        serializeValue(database, content, valueTypes[i], values[i], applyEncryption);
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
    case BinaryTypes.TYPE_ARRAY_OF_SHORTS: {
      final int length = Array.getLength(value);
      content.putUnsignedNumber(length);
      for (int i = 0; i < length; ++i)
        content.putNumber(Array.getShort(value, i));
      break;
    }
    case BinaryTypes.TYPE_ARRAY_OF_INTEGERS: {
      final int length = Array.getLength(value);
      content.putUnsignedNumber(length);
      for (int i = 0; i < length; ++i)
        content.putNumber(Array.getInt(value, i));
      break;
    }
    case BinaryTypes.TYPE_ARRAY_OF_LONGS: {
      final int length = Array.getLength(value);
      content.putUnsignedNumber(length);
      for (int i = 0; i < length; ++i)
        content.putNumber(Array.getLong(value, i));
      break;
    }
    case BinaryTypes.TYPE_ARRAY_OF_FLOATS: {
      final int length = Array.getLength(value);
      content.putUnsignedNumber(length);
      for (int i = 0; i < length; ++i)
        content.putNumber(Float.floatToIntBits(Array.getFloat(value, i)));
      break;
    }
    case BinaryTypes.TYPE_ARRAY_OF_DOUBLES: {
      final int length = Array.getLength(value);
      content.putUnsignedNumber(length);
      for (int i = 0; i < length; ++i)
        content.putNumber(Double.doubleToLongBits(Array.getDouble(value, i)));
      break;
    }
    default:
      LogManager.instance().log(this, Level.INFO, "Error on serializing value '" + value + "', type not supported");
    }

    if (encrypt) {
      switch (type) {
      case BinaryTypes.TYPE_NULL:
      case BinaryTypes.TYPE_COMPRESSED_RID:
      case BinaryTypes.TYPE_RID:
        break;
      default:
        serialized.putBytes(dataEncryption.encrypt(content.toByteArray()));
      }
    }
  }

  /**
   * Serialize an ordered sequence of entries as a list. Preserves positions: entries whose type can't
   * be determined are written as TYPE_NULL (and logged) so the deserialized list keeps the same size
   * and index layout.
   */
  private void serializeListEntries(final Database database, final Binary content, final Iterable<?> entries,
      final int expectedSize, final String kind, final boolean applyEncryption) {
    content.putUnsignedNumber(expectedSize);
    for (final Object entryValue : entries) {
      byte entryType = BinaryTypes.getTypeFromValue(entryValue, null);
      Object valueToWrite = entryValue;
      if (entryType == -1) {
        LogManager.instance()
            .log(BinaryTypes.class, Level.WARNING,
                "Cannot serialize entry in " + kind + " of type %s, value %s. Stored as null",
                entryValue.getClass(), entryValue);
        entryType = BinaryTypes.TYPE_NULL;
        valueToWrite = null;
      }
      content.putByte(entryType);
      serializeValue(database, content, entryType, valueToWrite, applyEncryption);
    }
  }

  public Object deserializeValue(final Database database, final Binary deserialized, final byte type,
      final EmbeddedModifier embeddedModifier) {
    return deserializeValue(database, deserialized, type, embeddedModifier, true);
  }

  public Object deserializeValue(final Database database, final Binary deserialized, final byte type,
      final EmbeddedModifier embeddedModifier, final boolean applyEncryption) {
    final Binary content = applyEncryption && dataEncryption != null &&
        type != BinaryTypes.TYPE_NULL &&
        type != BinaryTypes.TYPE_COMPRESSED_RID &&
        type != BinaryTypes.TYPE_RID ? new Binary(dataEncryption.decrypt(deserialized.getBytes())) : deserialized;

    final Object value;
    switch (type) {
    case BinaryTypes.TYPE_NULL:
      value = null;
      break;
    case BinaryTypes.TYPE_STRING:
      final String str = content.getString();
      // Backward compatibility: parse WKT geometry strings from old databases
      Object parsedValue = str;
      if (str != null && (str.startsWith("POINT") || str.startsWith("CIRCLE") ||
          str.startsWith("LINESTRING") || str.startsWith("POLYGON") ||
          str.startsWith("ENVELOPE") || str.startsWith("BUFFER"))) {
        try {
          final SpatialContext ctx = GeoUtils.getSpatialContext();
          parsedValue = ctx.getFormats().getWktReader().read(str);
        } catch (Exception e) {
          // If WKT parsing fails, return as string
          parsedValue = str;
        }
      }
      value = parsedValue;
      break;
    case BinaryTypes.TYPE_COMPRESSED_STRING:
      value = database.getSchema().getDictionary().getNameById((int) content.getUnsignedNumber());
      break;
    case BinaryTypes.TYPE_BINARY:
      value = content.getBytes();
      break;
    case BinaryTypes.TYPE_COMPRESSED_GEOMETRY:
      value = deserializeGeometryBinary(content);
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
      value = DateUtils.date(database, content.getUnsignedNumber(), dateImplementation);
      break;
    case BinaryTypes.TYPE_DATETIME_SECOND:
      value = DateUtils.dateTime(database, content.getUnsignedNumber(), ChronoUnit.SECONDS, dateTimeImplementation,
          ChronoUnit.SECONDS);
      break;
    case BinaryTypes.TYPE_DATETIME:
      value = DateUtils.dateTime(database, content.getUnsignedNumber(), ChronoUnit.MILLIS, dateTimeImplementation,
          ChronoUnit.MILLIS);
      break;
    case BinaryTypes.TYPE_DATETIME_MICROS:
      value = DateUtils.dateTime(database, content.getUnsignedNumber(), ChronoUnit.MICROS, dateTimeImplementation,
          ChronoUnit.MICROS);
      break;
    case BinaryTypes.TYPE_DATETIME_NANOS:
      value = DateUtils.dateTime(database, content.getUnsignedNumber(), ChronoUnit.NANOS, dateTimeImplementation, ChronoUnit.NANOS);
      break;
    case BinaryTypes.TYPE_DECIMAL:
      final int scale = (int) content.getNumber();
      final byte[] unscaledValue = content.getBytes();
      value = new BigDecimal(new BigInteger(unscaledValue), scale);
      break;
    case BinaryTypes.TYPE_COMPRESSED_RID:
      value = RID.create(database, (int) deserialized.getNumber(), deserialized.getNumber());
      break;
    case BinaryTypes.TYPE_RID:
      value = RID.create(database, deserialized.getInt(), deserialized.getLong());
      break;
    case BinaryTypes.TYPE_UUID:
      value = new UUID(content.getNumber(), content.getNumber());
      break;
    case BinaryTypes.TYPE_LIST: {
      final int count = (int) content.getUnsignedNumber();
      final List<Object> list = new ArrayList<>(count);
      for (int i = 0; i < count; ++i) {
        final byte entryType = content.getByte();
        list.add(deserializeValue(database, content, entryType, embeddedModifier, applyEncryption));
      }
      value = list;
      break;
    }
    case BinaryTypes.TYPE_MAP: {
      final int count = (int) content.getUnsignedNumber();
      final Map<Object, Object> map = new LinkedHashMap<>(count);
      for (int i = 0; i < count; ++i) {
        final byte entryKeyType = content.getByte();
        final Object entryKey = deserializeValue(database, content, entryKeyType, embeddedModifier, applyEncryption);

        final byte entryValueType = content.getByte();
        final Object entryValue = deserializeValue(database, content, entryValueType, embeddedModifier, applyEncryption);

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
    case BinaryTypes.TYPE_ARRAY_OF_SHORTS: {
      final int count = (int) content.getUnsignedNumber();
      final short[] array = new short[count];
      for (int i = 0; i < count; ++i)
        array[i] = (short) content.getNumber();
      value = array;
      break;
    }
    case BinaryTypes.TYPE_ARRAY_OF_INTEGERS: {
      final int count = (int) content.getUnsignedNumber();
      final int[] array = new int[count];
      for (int i = 0; i < count; ++i)
        array[i] = (int) content.getNumber();
      value = array;
      break;
    }
    case BinaryTypes.TYPE_ARRAY_OF_LONGS: {
      final int count = (int) content.getUnsignedNumber();
      final long[] array = new long[count];
      for (int i = 0; i < count; ++i)
        array[i] = content.getNumber();
      value = array;
      break;
    }
    case BinaryTypes.TYPE_ARRAY_OF_FLOATS: {
      final int count = (int) content.getUnsignedNumber();
      final float[] array = new float[count];
      for (int i = 0; i < count; ++i)
        array[i] = Float.intBitsToFloat((int) content.getNumber());
      value = array;
      break;
    }
    case BinaryTypes.TYPE_ARRAY_OF_DOUBLES: {
      final int count = (int) content.getUnsignedNumber();
      final double[] array = new double[count];
      for (int i = 0; i < count; ++i)
        array[i] = Double.longBitsToDouble(content.getNumber());
      value = array;
      break;
    }

    default:
      throw new SerializationException("Error on deserializing value of unknown type " + type);
    }
    return value;
  }

  public Binary serializeProperties(final Database database, final Document record, final Binary header, final Binary content) {
    final int headerSizePosition = header.position();
    header.putInt(0); // TEMPORARY PLACEHOLDER FOR HEADER SIZE

    final Map<String, Object> properties = record.propertiesAsMap();
    final Dictionary dictionary = database.getSchema().getDictionary();
    final DocumentType documentType = record.getType();
    // For records being UPDATED, look up existing external RIDs from the old buffer so we can update the external bucket
    // record in place rather than allocating a new one. New records (no identity yet) get an empty map.
    final Map<String, RID> existingExternalRids = findExistingExternalRids(database, record);
    // Track which existing external RIDs we re-used (kept) so we can delete the rest as orphans below. An entry is
    // orphaned when the property is no longer EXTERNAL (toggled off via ALTER), was renamed, or was dropped entirely.
    final Set<String> consumedExternalProperties = existingExternalRids.isEmpty() ? null : new HashSet<>();

    // Pre-resolve types so the property count matches what is actually written.
    // Skipping an invalid property after writing its nameId would desync the header on read.
    final int propertiesSize = properties.size();
    final String[] validNames = new String[propertiesSize];
    final Object[] validValues = new Object[propertiesSize];
    final byte[] validTypes = new byte[propertiesSize];
    int validCount = 0;

    for (final Map.Entry<String, Object> entry : properties.entrySet()) {
      final String propertyName = entry.getKey();
      final Object value = entry.getValue();
      final Property propertyType = documentType.getPropertyIfExists(propertyName);
      final byte type = BinaryTypes.getTypeFromValue(value, propertyType);
      if (type == -1) {
        // INVALID: SKIP IT
        LogManager.instance()
            .log(BinaryTypes.class, Level.WARNING,
                "Cannot serialize property '%s' of type %s, value %s. The property will be ignored",
                propertyName, value.getClass(), value);
        continue;
      }
      validNames[validCount] = propertyName;
      validValues[validCount] = value;
      validTypes[validCount] = type;
      ++validCount;
    }

    header.putUnsignedNumber(validCount);

    for (int i = 0; i < validCount; i++) {
      final String propertyName = validNames[i];
      Object value = validValues[i];
      byte type = validTypes[i];

      // WRITE PROPERTY ID FROM THE DICTIONARY
      header.putUnsignedNumber(dictionary.getIdByName(propertyName, true));

      final int startContentPosition = content.position();

      final Property propertyDef = documentType.getPropertyIfExists(propertyName);
      if (propertyDef != null && propertyDef.isExternal()) {
        // NULL is not externalised. set("field", null) is treated semantically the same as remove("field")
        // for storage purposes: we write a TYPE_NULL byte INLINE in the main record and let the orphan-
        // cleanup pass at the end of this method delete any pre-existing external blob (the property is
        // intentionally NOT added to consumedExternalProperties below, so orphan cleanup runs). This keeps
        // user mental model intuitive (null means "no payload") and avoids charging external-bucket space
        // for null markers, which on a paired-bucket layout would otherwise force one external record per
        // null-valued record.
        if (value == null) {
          content.putByte(BinaryTypes.TYPE_NULL);
        } else {
          // Externalised property: write the value to the paired external bucket and put a TYPE_EXTERNAL marker (with
          // the external RID) in the main record's content. The main record stays small and traversal-only reads never
          // hit the external bucket. See LocalDocumentType.getExternalBucketIdFor.
          final RID identity = record.getIdentity();
          if (identity == null)
            throw new SerializationException(
                "Cannot serialize EXTERNAL property '" + propertyName + "' on type '" + documentType.getName()
                    + "': record has no target bucket. The bucket layer must set a provisional identity before serialize.");
          final int primaryBucketId = identity.getBucketId();
          // Look up the external bucket via the type that ACTUALLY owns the primary bucket. This may differ from
          // record.getType(): polymorphic scans (scanType POLYMORPHIC, MATCH, etc.) tag every record with the queried
          // parent type even when the record physically lives in a subtype's bucket. Trusting documentType in that
          // case would miss the subtype's external bucket map.
          final LocalDocumentType ownerType = (LocalDocumentType) database.getSchema().getEmbedded().getTypeByBucketId(primaryBucketId);
          final Integer extBucketId = (ownerType != null ? ownerType : (LocalDocumentType) documentType)
              .getExternalBucketIdFor(primaryBucketId);
          if (extBucketId == null)
            throw new SerializationException(
                "Cannot serialize EXTERNAL property '" + propertyName + "' on type '" + documentType.getName()
                    + "': no external bucket is paired with primary bucket " + primaryBucketId);

          final RID existingExtRid = existingExternalRids.get(propertyName);
          if (consumedExternalProperties != null && existingExtRid != null)
            consumedExternalProperties.add(propertyName);

          final ExternalWriteResult written = writeExternalPropertyValue((DatabaseInternal) database, extBucketId,
              existingExtRid, type, value, propertyDef.getCompression());

          // The persisted type byte tells the reader which decoder to use. Bucket id and position are varints,
          // mirroring TYPE_COMPRESSED_RID, so each pointer averages 3-7 bytes vs 12 fixed.
          content.putByte(written.typeByte);
          content.putNumber(written.rid.getBucketId());
          content.putNumber(written.rid.getPosition());
        }
      } else {
        if (value instanceof String stringValue && type == BinaryTypes.TYPE_STRING) {
          final int id = dictionary.getIdByName(stringValue, false);
          if (id > -1) {
            // WRITE THE COMPRESSED STRING
            type = BinaryTypes.TYPE_COMPRESSED_STRING;
            value = id;
          }
        }

        content.putByte(type);
        serializeValue(database, content, type, value);
      }

      // WRITE PROPERTY CONTENT POSITION
      header.putUnsignedNumber(startContentPosition);
    }

    content.flip();

    final int headerEndOffset = header.position();

    // UPDATE HEADER SIZE
    header.putInt(headerSizePosition, headerEndOffset);

    header.append(content);
    header.flip();

    // Orphan cleanup: any existing external RID that was NOT re-used (property no longer EXTERNAL, was renamed, or was
    // dropped) must be deleted from the external bucket so we don't leak storage. Same transaction as the primary write.
    if (consumedExternalProperties != null) {
      for (final Map.Entry<String, RID> entry : existingExternalRids.entrySet()) {
        if (consumedExternalProperties.contains(entry.getKey()))
          continue;
        final RID orphanRid = entry.getValue();
        final LocalBucket externalBucket = database.getSchema().getEmbedded().getBucketById(orphanRid.getBucketId(), false);
        if (externalBucket != null) {
          externalBucket.deleteRecord(orphanRid);
          ((DatabaseInternal) database).getTransaction().updateBucketRecordDelta(externalBucket.getFileId(), -1);
        }
      }
    }

    return header;
  }

  /**
   * Holder for {@link #writeExternalPropertyValue}: the RID where the value blob lives and the type byte the
   * caller should embed in the main record. Field order matches the conceptual weight (the RID is the bulk of
   * the pointer; the type byte is the 1-byte discriminator). Package-private along with the writer; tests
   * reach it via {@code BinarySerializerTestHelper}.
   */
  static final class ExternalWriteResult {
    final RID  rid;
    final byte typeByte;

    ExternalWriteResult(final RID rid, final byte typeByte) {
      this.rid = rid;
      this.typeByte = typeByte;
    }
  }

  /** True for any of TYPE_EXTERNAL, TYPE_EXTERNAL_COMPRESSED_FAST, TYPE_EXTERNAL_COMPRESSED_MAX. */
  private static boolean isExternalType(final byte type) {
    return type == BinaryTypes.TYPE_EXTERNAL
        || type == BinaryTypes.TYPE_EXTERNAL_COMPRESSED_FAST
        || type == BinaryTypes.TYPE_EXTERNAL_COMPRESSED_MAX;
  }

  /**
   * True only for the compressed external types. LZ4 fast and LZ4 HC share the same byte format, so the read
   * path needs only the compressed/raw distinction; both compressed types decode through the same
   * {@code LZ4Compression.decompress} call.
   */
  private static boolean isExternalCompressedType(final byte type) {
    return type == BinaryTypes.TYPE_EXTERNAL_COMPRESSED_FAST
        || type == BinaryTypes.TYPE_EXTERNAL_COMPRESSED_MAX;
  }

  /**
   * Serialises an EXTERNAL property value per the property's compression policy, writes the resulting blob to
   * the paired external bucket, and returns the type byte the caller should put in the main record.
   * <p>
   * Policy values:
   * <ul>
   *   <li>{@code none} (or null/empty) - store raw, type byte = TYPE_EXTERNAL.</li>
   *   <li>{@code fast} - LZ4 fast encoder, type byte = TYPE_EXTERNAL_COMPRESSED_FAST.</li>
   *   <li>{@code max}  - LZ4 HC encoder (slower compress, ~10pp smaller), type byte = TYPE_EXTERNAL_COMPRESSED_MAX.</li>
   *   <li>{@code auto} - try LZ4 fast; keep only when it saves more than 10% of bytes, otherwise store raw.</li>
   * </ul>
   * The decision is per-record, so a single property may mix compressed and uncompressed records freely.
   * Made package-private; tests reach it through {@code BinarySerializerTestHelper}.
   */
  ExternalWriteResult writeExternalPropertyValue(final DatabaseInternal database, final int externalBucketId,
      final RID existingExternalRid, final byte valueType, final Object value, final String compressionPolicy) {
    if (existingExternalRid != null && existingExternalRid.getBucketId() != externalBucketId)
      throw new SerializationException(
          "Existing external RID " + existingExternalRid + " does not match the paired external bucket id "
              + externalBucketId + " for this record. The schema's external bucket mapping is inconsistent.");

    // The "lz4" legacy alias is normalised to "fast" by LocalProperty.setCompression(), so by the time the
    // policy reaches us it's already one of: null, "none", "fast", "max", "auto". No second alias check.
    final boolean fastMode = "fast".equalsIgnoreCase(compressionPolicy);
    final boolean maxMode = "max".equalsIgnoreCase(compressionPolicy);
    final boolean autoMode = "auto".equalsIgnoreCase(compressionPolicy);
    final boolean tryCompress = fastMode || maxMode || autoMode;

    final Binary blob = new Binary();

    if (!tryCompress) {
      // Fast path (no compression policy): serialise straight into the blob. Avoids the extra Binary buffer +
      // toByteArray() roundtrip the compressed path needs. This is the dense-vector default (compression is a
      // loss for embeddings) and the throughput-critical case for write-heavy workloads.
      blob.putByte(ExternalValueRecord.RECORD_TYPE);
      blob.putByte(valueType);
      serializeValue(database, blob, valueType, value);
      blob.flip();
      return finalizeExternalWrite(database, externalBucketId, existingExternalRid, blob, BinaryTypes.TYPE_EXTERNAL);
    }

    // Compressed path: we need the raw bytes both to compress and to fall back on auto-mode no-win, so this is
    // the one case where the intermediate Binary is unavoidable.
    //
    // AUTO-MODE OVERHEAD (accepted, not optimised). When auto-mode picks the no-win branch the work done is:
    //   1. serializeValue once into rawValueBytes      (always paid)
    //   2. rawValueBytes.toByteArray() byte[] copy     (paid even when we end up not keeping the compressed form)
    //   3. lz4.compress() call                          (CPU spent for nothing)
    //   4. blob.append(rawValueBytes) byte copy         (effectively a third copy of the raw bytes)
    // versus the non-compress fast path's single serialise-straight-into-blob. The serialise itself runs once,
    // not twice; the cost is the extra byte[] copy + the discarded compress call. For workloads that actually
    // tip into the no-win branch routinely (e.g. dense float32 embeddings), use {@code none} explicitly. Auto
    // is correct for mixed workloads where most records benefit and the throughput hit on the minority is
    // acceptable. We do not memoise the compressor output across calls because it is per-record state.
    final Binary rawValueBytes = new Binary();
    serializeValue(database, rawValueBytes, valueType, value);
    rawValueBytes.flip();

    byte typeByte = BinaryTypes.TYPE_EXTERNAL;
    byte[] compressedPayload = null;
    int uncompressedSize = 0;

    if (rawValueBytes.size() > 0) {
      final byte[] raw = rawValueBytes.toByteArray();
      final LZ4Compression lz4 = CompressionFactory.getLZ4();
      final byte[] compressed = maxMode ? lz4.compressMax(raw) : lz4.compress(raw);
      // In auto mode skip compression unless it saves >10%. In fast/max mode always keep the compressed form
      // (the user explicitly asked to compress, even if a particular record happens not to gain much).
      if (!autoMode || compressed.length < raw.length * 0.9) {
        typeByte = maxMode ? BinaryTypes.TYPE_EXTERNAL_COMPRESSED_MAX : BinaryTypes.TYPE_EXTERNAL_COMPRESSED_FAST;
        compressedPayload = compressed;
        uncompressedSize = raw.length;
      }
    }

    blob.putByte(ExternalValueRecord.RECORD_TYPE);
    blob.putByte(valueType);
    if (isExternalCompressedType(typeByte)) {
      blob.putUnsignedNumber(uncompressedSize);
      // Length-prefixed payload (Binary.putBytes writes a varint length followed by the bytes). The earlier
      // version used putByteArray and reconstructed the length from buffer.size() - position(), which tied
      // the format to "compressed bytes fill the rest of the record". Storing the length explicitly costs
      // 1-4 bytes per record and lets future versions append fields (checksum, footer, etc.) without
      // requiring a one-shot data migration on every external bucket.
      blob.putBytes(compressedPayload);
    } else {
      blob.append(rawValueBytes);
    }
    blob.flip();
    return finalizeExternalWrite(database, externalBucketId, existingExternalRid, blob, typeByte);
  }

  /**
   * Inserts or updates the given blob in the paired external bucket and returns the type byte + RID for the
   * caller to embed in the main record. Extracted from {@link #writeExternalPropertyValue} to keep the two
   * write paths (raw / compressed) short and to avoid duplicating the bucket-side accounting.
   */
  private ExternalWriteResult finalizeExternalWrite(final DatabaseInternal database, final int externalBucketId,
      final RID existingExternalRid, final Binary blob, final byte typeByte) {
    final LocalBucket externalBucket = database.getSchema().getEmbedded().getBucketById(externalBucketId);
    final RID rid;
    if (existingExternalRid == null) {
      final ExternalValueRecord rec = new ExternalValueRecord(database, null, blob);
      rid = externalBucket.createRecord(rec, true);
      database.getTransaction().updateBucketRecordDelta(externalBucket.getFileId(), +1);
    } else {
      // Identity already passed to the constructor (BaseRecord stores it). No need to call setIdentity again.
      final ExternalValueRecord rec = new ExternalValueRecord(database, existingExternalRid, blob);
      externalBucket.updateRecord(rec, true);
      rid = existingExternalRid;
    }
    return new ExternalWriteResult(rid, typeByte);
  }

  public Object readExternalValue(final DatabaseInternal database, final int externalBucketId, final long position,
      final EmbeddedModifier embeddedModifier) {
    return readExternalValue(database, externalBucketId, position, embeddedModifier, false);
  }

  /**
   * Reads the value blob at the given external RID. When {@code compressed} is true, the blob's value-bytes are
   * LZ4-compressed and prefixed by an uncompressed-size varint. The compression flag is supplied by the caller -
   * usually derived from the main record's type byte. LZ4 fast and LZ4 HC share the same byte format, so this
   * method does not need to know which encoder produced the bytes.
   */
  public Object readExternalValue(final DatabaseInternal database, final int externalBucketId, final long position,
      final EmbeddedModifier embeddedModifier, final boolean compressed) {
    final LocalBucket externalBucket = database.getSchema().getEmbedded().getBucketById(externalBucketId);
    if (externalBucket == null)
      // Typical cause: the paired external bucket was created on a tiered path (configured via
      // arcadedb.externalPropertyBucketPath) and the database was reopened with that config unset or
      // pointing elsewhere, so FileManager's secondary scan never picked the file up. The schema still
      // references the old bucket id, but the file isn't loaded -> we'd otherwise NPE on getRecord.
      throw new SerializationException(
          "Cannot read EXTERNAL property: external bucket id=" + externalBucketId + " is not loaded. "
              + "If the bucket was tiered to a secondary path, set 'arcadedb.externalPropertyBucketPath' "
              + "to the same value used at creation time and reopen the database.");
    final RID rid = RID.create(database, externalBucketId, position);
    final Binary buffer = externalBucket.getRecord(rid).copyOfContent();
    buffer.position(Binary.BYTE_SERIALIZED_SIZE); // SKIP RECORD TYPE BYTE
    final byte valueType = buffer.getByte();
    if (!compressed)
      return deserializeValue(database, buffer, valueType, embeddedModifier);

    final int uncompressedSize = (int) buffer.getUnsignedNumber();
    // Length-prefixed payload (Binary.getBytes reads varint length + the bytes). Trailing space (if any) is
    // ignored, leaving the format extensible to a future footer/checksum without a migration.
    final byte[] compressedBytes = buffer.getBytes();
    final byte[] decompressed = CompressionFactory.getLZ4().decompress(compressedBytes, uncompressedSize);
    return deserializeValue(database, new Binary(decompressed), valueType, embeddedModifier);
  }

  /**
   * Reused by cascade-delete and the orphan-cleanup-on-update path inside {@link #serializeProperties}: scans
   * the OLD buffer for TYPE_EXTERNAL pointers, keyed by property name.
   * <p>
   * <b>Gating check: {@code hasExternalBuckets()}, not {@code hasExternalProperties()}.</b>
   * The two are different and the distinction is load-bearing:
   * <ul>
   *   <li>{@code hasExternalProperties()} reflects the CURRENT schema (count of properties currently flagged
   *       EXTERNAL). It flips to false the instant {@code setExternal(false)} commits, before any record has
   *       been re-serialised. Using it as the gate would skip orphan-cleanup during the EXTERNAL→inline
   *       migration window and leak external blobs.</li>
   *   <li>{@code hasExternalBuckets()} reflects the lifetime of the paired-bucket mapping
   *       ({@code externalBucketIdByPrimaryBucketId} non-empty). It stays true through the entire migration
   *       window: schema flag flipped → REBUILD TYPE re-serialises every record → orphan-cleanup empties the
   *       paired buckets → {@code reclaimEmptyExternalBuckets()} drops them and clears the map. Only at that
   *       point does it return false, and by then no record can carry a stale TYPE_EXTERNAL pointer.</li>
   * </ul>
   * Types that have never used the EXTERNAL feature have an empty map, so this fast-exit pays for itself on
   * the (very common) update hot path of plain-vanilla types.
   */
  public Map<String, RID> findExistingExternalRids(final Database database, final Document record) {
    final RID identity = record.getIdentity();
    if (identity == null)
      return Collections.emptyMap();
    if (!(record instanceof BaseDocument))
      return Collections.emptyMap();
    if (!(record.getType() instanceof LocalDocumentType ldt) || !ldt.hasExternalBuckets())
      return Collections.emptyMap();
    final Binary buf = ((BaseRecord) record).getBuffer();
    if (buf == null)
      return Collections.emptyMap();

    // Scan the record's own buffer in place (no copy). The buffer is the read-side of the record's content,
    // which by the time serializeProperties reaches us has finished its own reads (it pulls the property map
    // first via propertiesAsMap, which deserialises and caches into a Map, then calls us). Cascade-delete and
    // CHECK DATABASE both call us outside any other buffer iteration. We still save/restore the position so
    // a future caller mid-iteration would not be disturbed - cheap insurance for a hot update path.
    final int savedPosition = buf.position();
    try {
      buf.position(((DocumentInternal) record).getPropertiesStartingPosition());

      final int headerEndOffset = buf.getInt();
      final int properties = (int) buf.getUnsignedNumber();
      if (properties <= 0)
        return Collections.emptyMap();

      final Dictionary dictionary = database.getSchema().getDictionary();
      Map<String, RID> result = null;

      for (int i = 0; i < properties; i++) {
        final int nameId = (int) buf.getUnsignedNumber();
        final int contentPosition = (int) buf.getUnsignedNumber();
        final int afterHeader = buf.position();

        buf.position(headerEndOffset + contentPosition);
        final byte type = buf.getByte();
        // All three external type bytes carry the same [bucketIdVarint][positionVarint] RID; only the blob
        // decoder differs (raw / LZ4 fast / LZ4 HC). Cascade-delete and orphan cleanup only need the RID.
        if (isExternalType(type)) {
          final int extBucketId = (int) buf.getNumber();
          final long extPosition = buf.getNumber();
          if (result == null)
            result = new HashMap<>();
          result.put(dictionary.getNameById(nameId), RID.create(database, extBucketId, extPosition));
        }

        buf.position(afterHeader);
      }
      return result == null ? Collections.emptyMap() : result;
    } catch (Exception e) {
      // Bump the JVM-cumulative counter so CHECK DATABASE surfaces the leak rate without waiting for the
      // next paired-bucket scan. The orphan blob (if any) will eventually be caught by the orphan scan,
      // but counting failures gives an early signal that something is corrupting record buffers.
      externalRidScanFailures.incrementAndGet();
      LogManager.instance().log(this, Level.WARNING,
          "Could not parse old buffer to recover external RIDs for record %s: %s. External records linked to this "
              + "record may be orphaned in the paired bucket. (cumulative scan failures since process start: %d)",
          e, identity, e.getMessage(), externalRidScanFailures.get());
      return Collections.emptyMap();
    } finally {
      buf.position(savedPosition);
    }
  }

  public Class<?> getDateImplementation() {
    return dateImplementation;
  }

  public void setDateImplementation(final Object dateImplementation) throws ClassNotFoundException {
    this.dateImplementation = dateImplementation instanceof Class<?> c ?
        c :
        Class.forName(dateImplementation.toString());
  }

  public Class<?> getDateTimeImplementation() {
    return dateTimeImplementation;
  }

  public void setDateTimeImplementation(final Object dateTimeImplementation) throws ClassNotFoundException {
    this.dateTimeImplementation = dateTimeImplementation instanceof Class<?> c ?
        c :
        Class.forName(dateTimeImplementation.toString());
  }

  public BinaryComparator getComparator() {
    return comparator;
  }

  private void serializeDateTime(final Database database, final Binary content, final Object value, final byte type) {
    content.putUnsignedNumber(DateUtils.dateTimeToTimestamp(database, value, DateUtils.getPrecisionFromBinaryType(type)));
  }

  public void setDataEncryption(final DataEncryption dataEncryption) {
    this.dataEncryption = dataEncryption;
  }

  /**
   * Converts a spatial4j Shape object to WKT (Well-Known Text) format.
   * Optimized with cached WKT writer to avoid creating a new writer for each Point.
   * This eliminates all reflection overhead from the previous implementation.
   */
  private String convertShapeToWKT(final Object shape) {
    try {
      // Cast to Shape interface (direct - no reflection!)
      final Shape spatialShape = (Shape) shape;

      // Initialize cached WKT writer on first use (thread-safe via volatile)
      if (cachedWktWriter == null) {
        synchronized (BinarySerializer.class) {
          if (cachedWktWriter == null) {
            final SpatialContext spatialContext = spatialShape.getContext();
            final SupportedFormats formats = spatialContext.getFormats();
            cachedWktWriter = formats.getWktWriter();
          }
        }
      }

      // Convert shape to WKT using cached writer (fast - no reflection, no object creation!)
      return cachedWktWriter.toString(spatialShape);

    } catch (Exception e) {
      // Fallback to toString() if WKT conversion fails
      LogManager.instance().log(this, Level.WARNING, "Failed to convert shape to WKT, using toString(): %s", e, e.getMessage());
      return shape.toString();
    }
  }

  /**
   * Serializes a spatial4j Shape to binary format.
   * Format: subtype(1 byte) + shape-specific data
   * - Point: x(double) + y(double) = 17 bytes total
   * - Circle: x(double) + y(double) + radius(double) = 25 bytes total
   * - Rectangle: minX + minY + maxX + maxY = 33 bytes total
   * This is much more efficient than WKT strings (e.g., "POINT(48.856 2.352)" = 20+ chars).
   */
  private void serializeGeometryBinary(final Binary content, final Shape shape) {
    if (shape instanceof Point point) {
      // Point: 1 byte subtype + 16 bytes (2 doubles)
      content.putByte(BinaryTypes.GEOMETRY_SUBTYPE_POINT);
      content.putNumber(Double.doubleToLongBits(point.getX()));
      content.putNumber(Double.doubleToLongBits(point.getY()));

    } else if (shape instanceof Circle circle) {
      // Circle: 1 byte subtype + 24 bytes (3 doubles)
      content.putByte(BinaryTypes.GEOMETRY_SUBTYPE_CIRCLE);
      content.putNumber(Double.doubleToLongBits(circle.getCenter().getX()));
      content.putNumber(Double.doubleToLongBits(circle.getCenter().getY()));
      content.putNumber(Double.doubleToLongBits(circle.getRadius()));

    } else if (shape instanceof Rectangle rect) {
      // Rectangle: 1 byte subtype + 32 bytes (4 doubles)
      content.putByte(BinaryTypes.GEOMETRY_SUBTYPE_RECTANGLE);
      content.putNumber(Double.doubleToLongBits(rect.getMinX()));
      content.putNumber(Double.doubleToLongBits(rect.getMinY()));
      content.putNumber(Double.doubleToLongBits(rect.getMaxX()));
      content.putNumber(Double.doubleToLongBits(rect.getMaxY()));

    } else {
      // Unknown shape type - fall back to WKT string for compatibility
      LogManager.instance().log(this, Level.WARNING,
          "Unknown shape type %s, falling back to WKT string serialization", shape.getClass().getName());
      // Write a special subtype 0 to indicate WKT fallback
      content.putByte((byte) 0);
      content.putString(convertShapeToWKT(shape));
    }
  }

  /**
   * Deserializes a binary geometry back to a spatial4j Shape object.
   * Reads subtype byte and reconstructs the appropriate shape.
   */
  private Shape deserializeGeometryBinary(final Binary content) {
    final byte subtype = content.getByte();
    final SpatialContext ctx = GeoUtils.getSpatialContext();

    return switch (subtype) {
      case BinaryTypes.GEOMETRY_SUBTYPE_POINT -> {
        final double x = Double.longBitsToDouble(content.getNumber());
        final double y = Double.longBitsToDouble(content.getNumber());
        yield ctx.getShapeFactory().pointXY(x, y);
      }
      case BinaryTypes.GEOMETRY_SUBTYPE_CIRCLE -> {
        final double x = Double.longBitsToDouble(content.getNumber());
        final double y = Double.longBitsToDouble(content.getNumber());
        final double radius = Double.longBitsToDouble(content.getNumber());
        final Point center = ctx.getShapeFactory().pointXY(x, y);
        yield ctx.getShapeFactory().circle(center, radius);
      }
      case BinaryTypes.GEOMETRY_SUBTYPE_RECTANGLE -> {
        final double minX = Double.longBitsToDouble(content.getNumber());
        final double minY = Double.longBitsToDouble(content.getNumber());
        final double maxX = Double.longBitsToDouble(content.getNumber());
        final double maxY = Double.longBitsToDouble(content.getNumber());
        yield ctx.getShapeFactory().rect(minX, maxX, minY, maxY);
      }
      case 0 -> {
        // WKT fallback format
        final String wkt = content.getString();
        try {
          yield ctx.getFormats().getWktReader().read(wkt);
        } catch (Exception e) {
          throw new SerializationException("Failed to parse WKT geometry: " + wkt, e);
        }
      }
      default -> throw new SerializationException("Unknown geometry subtype: " + subtype);
    };
  }

}
