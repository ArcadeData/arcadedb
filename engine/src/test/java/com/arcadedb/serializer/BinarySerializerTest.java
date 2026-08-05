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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.EmbeddedDocument;
import com.arcadedb.database.EmbeddedModifierProperty;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.MutableEmbeddedDocument;
import com.arcadedb.exception.SerializationException;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BinarySerializerTest extends TestHelper {

  @Test
  void varNumber() {
    final Binary binary = new Binary();
    binary.putUnsignedNumber(0);
    binary.putUnsignedNumber(3);
    binary.putUnsignedNumber(Short.MIN_VALUE);
    binary.putUnsignedNumber(Short.MAX_VALUE);
    binary.putUnsignedNumber(Integer.MIN_VALUE);
    binary.putUnsignedNumber(Integer.MAX_VALUE);
    binary.putUnsignedNumber(Long.MIN_VALUE);
    binary.putUnsignedNumber(Long.MAX_VALUE);

    binary.putNumber(0);
    binary.putNumber(3);
    binary.putNumber(Short.MIN_VALUE);
    binary.putNumber(Short.MAX_VALUE);
    binary.putNumber(Integer.MIN_VALUE);
    binary.putNumber(Integer.MAX_VALUE);
    binary.putNumber(Long.MIN_VALUE);
    binary.putNumber(Long.MAX_VALUE);

    binary.putShort((short) 0);
    binary.putShort(Short.MIN_VALUE);
    binary.putShort(Short.MAX_VALUE);

    binary.putInt(0);
    binary.putInt(Integer.MIN_VALUE);
    binary.putInt(Integer.MAX_VALUE);

    binary.putLong(0l);
    binary.putLong(Long.MIN_VALUE);
    binary.putLong(Long.MAX_VALUE);

    binary.flip();

    final ByteBuffer dBuffer = ByteBuffer.allocate(1024);
    final Binary buffer = new Binary(dBuffer);
    dBuffer.put(binary.toByteArray());

    binary.rewind();
    buffer.rewind();

    assertThat(binary.getUnsignedNumber()).isEqualTo(0);
    assertThat(buffer.getUnsignedNumber()).isEqualTo(0);
    assertThat(binary.getUnsignedNumber()).isEqualTo(3);
    assertThat(buffer.getUnsignedNumber()).isEqualTo(3);
    assertThat(binary.getUnsignedNumber()).isEqualTo(Short.MIN_VALUE);
    assertThat(buffer.getUnsignedNumber()).isEqualTo(Short.MIN_VALUE);
    assertThat(binary.getUnsignedNumber()).isEqualTo(Short.MAX_VALUE);
    assertThat(buffer.getUnsignedNumber()).isEqualTo(Short.MAX_VALUE);
    assertThat(binary.getUnsignedNumber()).isEqualTo(Integer.MIN_VALUE);
    assertThat(buffer.getUnsignedNumber()).isEqualTo(Integer.MIN_VALUE);
    assertThat(binary.getUnsignedNumber()).isEqualTo(Integer.MAX_VALUE);
    assertThat(buffer.getUnsignedNumber()).isEqualTo(Integer.MAX_VALUE);
    assertThat(binary.getUnsignedNumber()).isEqualTo(Long.MIN_VALUE);
    assertThat(buffer.getUnsignedNumber()).isEqualTo(Long.MIN_VALUE);
    assertThat(binary.getUnsignedNumber()).isEqualTo(Long.MAX_VALUE);
    assertThat(buffer.getUnsignedNumber()).isEqualTo(Long.MAX_VALUE);

    assertThat(binary.getNumber()).isEqualTo(0);
    assertThat(buffer.getNumber()).isEqualTo(0);
    assertThat(binary.getNumber()).isEqualTo(3);
    assertThat(buffer.getNumber()).isEqualTo(3);
    assertThat(binary.getNumber()).isEqualTo(Short.MIN_VALUE);
    assertThat(buffer.getNumber()).isEqualTo(Short.MIN_VALUE);
    assertThat(binary.getNumber()).isEqualTo(Short.MAX_VALUE);
    assertThat(buffer.getNumber()).isEqualTo(Short.MAX_VALUE);
    assertThat(binary.getNumber()).isEqualTo(Integer.MIN_VALUE);
    assertThat(buffer.getNumber()).isEqualTo(Integer.MIN_VALUE);
    assertThat(binary.getNumber()).isEqualTo(Integer.MAX_VALUE);
    assertThat(buffer.getNumber()).isEqualTo(Integer.MAX_VALUE);
    assertThat(binary.getNumber()).isEqualTo(Long.MIN_VALUE);
    assertThat(buffer.getNumber()).isEqualTo(Long.MIN_VALUE);
    assertThat(binary.getNumber()).isEqualTo(Long.MAX_VALUE);
    assertThat(buffer.getNumber()).isEqualTo(Long.MAX_VALUE);

    assertThat(binary.getShort()).isEqualTo((short) 0);
    assertThat(buffer.getShort()).isEqualTo((short) 0);

    assertThat(binary.getShort()).isEqualTo(Short.MIN_VALUE);
    assertThat(buffer.getShort()).isEqualTo(Short.MIN_VALUE);

    assertThat(binary.getShort()).isEqualTo(Short.MAX_VALUE);
    assertThat(buffer.getShort()).isEqualTo(Short.MAX_VALUE);

    assertThat(binary.getInt()).isEqualTo(0);
    assertThat(buffer.getInt()).isEqualTo(0);

    assertThat(binary.getInt()).isEqualTo(Integer.MIN_VALUE);
    assertThat(buffer.getInt()).isEqualTo(Integer.MIN_VALUE);

    assertThat(binary.getInt()).isEqualTo(Integer.MAX_VALUE);
    assertThat(buffer.getInt()).isEqualTo(Integer.MAX_VALUE);

    assertThat(binary.getLong()).isEqualTo(0l);
    assertThat(buffer.getLong()).isEqualTo(0l);

    assertThat(binary.getLong()).isEqualTo(Long.MIN_VALUE);
    assertThat(buffer.getLong()).isEqualTo(Long.MIN_VALUE);

    assertThat(binary.getLong()).isEqualTo(Long.MAX_VALUE);
    assertThat(buffer.getLong()).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void literalPropertiesInDocument() throws Exception {
    final BinarySerializer serializer = new BinarySerializer(database.getConfiguration());

    database.transaction(() -> {
      database.getSchema().createDocumentType("Test");
      database.commit();

      database.begin();

      final MutableDocument v = database.newDocument("Test");
      v.set("minInt", Integer.MIN_VALUE);
      v.set("maxInt", Integer.MAX_VALUE);
      v.set("minLong", Long.MIN_VALUE);
      v.set("maxLong", Long.MAX_VALUE);
      v.set("minShort", Short.MIN_VALUE);
      v.set("maxShort", Short.MAX_VALUE);
      v.set("minByte", Byte.MIN_VALUE);
      v.set("maxByte", Byte.MAX_VALUE);
      v.set("decimal", new BigDecimal("9876543210.0123456789"));
      v.set("string", "Miner");

      final Binary buffer = serializer.serialize((DatabaseInternal) database, v);

      final ByteBuffer buffer2 = ByteBuffer.allocate((int) GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getDefValue());
      buffer2.put(buffer.toByteArray());
      buffer2.flip();

      final Binary buffer3 = new Binary(buffer2);
      buffer3.getByte(); // SKIP RECORD TYPE
      final Map<String, Object> record2 = serializer.deserializeProperties(database, buffer3, null, null);

      assertThat(record2.get("minInt")).isEqualTo(Integer.MIN_VALUE);
      assertThat(record2.get("maxInt")).isEqualTo(Integer.MAX_VALUE);

      assertThat(record2.get("minLong")).isEqualTo(Long.MIN_VALUE);
      assertThat(record2.get("maxLong")).isEqualTo(Long.MAX_VALUE);

      assertThat(record2.get("minShort")).isEqualTo(Short.MIN_VALUE);
      assertThat(record2.get("maxShort")).isEqualTo(Short.MAX_VALUE);

      assertThat(record2.get("minByte")).isEqualTo(Byte.MIN_VALUE);
      assertThat(record2.get("maxByte")).isEqualTo(Byte.MAX_VALUE);

      assertThat(record2.get("decimal") instanceof BigDecimal).isTrue();
      assertThat(record2.get("decimal")).isEqualTo(new BigDecimal("9876543210.0123456789"));
      assertThat(record2.get("string")).isEqualTo("Miner");
    });
  }

  @Test
  void listPropertiesInDocument() throws Exception {
    final BinarySerializer serializer = new BinarySerializer(database.getConfiguration());

    database.transaction(() -> {
      database.getSchema().createDocumentType("Test");
      database.commit();

      final List<Boolean> listOfBooleans = new ArrayList<>();
      listOfBooleans.add(true);
      listOfBooleans.add(false);

      final List<Integer> listOfIntegers = new ArrayList<>();
      for (int i = 0; i < 100; ++i)
        listOfIntegers.add(i);

      final List<Long> listOfLongs = new ArrayList<>();
      for (int i = 0; i < 100; ++i)
        listOfLongs.add((long) i);

      final List<Short> listOfShorts = new ArrayList<>();
      for (int i = 0; i < 100; ++i)
        listOfShorts.add((short) i);

      final List<Float> listOfFloats = new ArrayList<>();
      for (int i = 0; i < 100; ++i)
        listOfFloats.add(((float) i) + 0.123f);

      final List<Double> listOfDoubles = new ArrayList<>();
      for (int i = 0; i < 100; ++i)
        listOfDoubles.add(((double) i) + 0.123f);

      final List<String> listOfStrings = new ArrayList<>();
      for (int i = 0; i < 100; ++i)
        listOfStrings.add("" + i);

      final List<Object> listOfMixed = new ArrayList<>();
      listOfMixed.add(0);
      listOfMixed.add((long) 1);
      listOfMixed.add((short) 2);
      listOfMixed.add("3");

      database.begin();
      final MutableDocument v = database.newDocument("Test");

      v.set("listOfBooleans", listOfBooleans);
      v.set("arrayOfBooleans", listOfBooleans.toArray());

      v.set("listOfIntegers", listOfIntegers);
      v.set("arrayOfIntegers", listOfIntegers.toArray());

      v.set("listOfLongs", listOfLongs);
      v.set("arrayOfLongs", listOfLongs.toArray());

      v.set("listOfShorts", listOfShorts);
      v.set("arrayOfShorts", listOfShorts.toArray());

      v.set("listOfFloats", listOfFloats);
      v.set("arrayOfFloats", listOfFloats.toArray());

      v.set("listOfDoubles", listOfDoubles);
      v.set("arrayOfDoubles", listOfDoubles.toArray());

      v.set("listOfStrings", listOfStrings);
      v.set("arrayOfStrings", listOfStrings.toArray());

      v.set("listOfMixed", listOfMixed);
      v.set("arrayOfMixed", listOfMixed.toArray());

      final Binary buffer = serializer.serialize((DatabaseInternal) database, v);

      final ByteBuffer buffer2 = ByteBuffer.allocate((int) GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getDefValue());
      buffer2.put(buffer.toByteArray());
      buffer2.flip();

      final Binary buffer3 = new Binary(buffer2);
      buffer3.getByte(); // SKIP RECORD TYPE
      final Map<String, Object> record2 = serializer.deserializeProperties(database, buffer3, null, null);

      assertThat(record2.get("listOfBooleans")).isEqualTo(listOfBooleans);
      assertThat(record2.get("arrayOfBooleans")).isEqualTo(listOfBooleans);

      assertThat(record2.get("listOfIntegers")).isEqualTo(listOfIntegers);
      assertThat(record2.get("arrayOfIntegers")).isEqualTo(listOfIntegers);

      assertThat(record2.get("listOfLongs")).isEqualTo(listOfLongs);
      assertThat(record2.get("arrayOfLongs")).isEqualTo(listOfLongs);

      assertThat(record2.get("listOfShorts")).isEqualTo(listOfShorts);
      assertThat(record2.get("arrayOfShorts")).isEqualTo(listOfShorts);

      assertThat(record2.get("listOfFloats")).isEqualTo(listOfFloats);
      assertThat(record2.get("arrayOfFloats")).isEqualTo(listOfFloats);

      assertThat(record2.get("listOfDoubles")).isEqualTo(listOfDoubles);
      assertThat(record2.get("arrayOfDoubles")).isEqualTo(listOfDoubles);

      assertThat(record2.get("listOfStrings")).isEqualTo(listOfStrings);
      assertThat(record2.get("arrayOfStrings")).isEqualTo(listOfStrings);

      assertThat(record2.get("listOfMixed")).isEqualTo(listOfMixed);
      assertThat(record2.get("arrayOfMixed")).isEqualTo(listOfMixed);
    });
  }

  @Test
  void arraysOfPrimitive() throws Exception {
    final BinarySerializer serializer = new BinarySerializer(database.getConfiguration());

    database.transaction(() -> {
      database.getSchema().createDocumentType("Test");
      database.commit();

      final int[] arrayOfIntegers = new int[100];
      for (int i = 0; i < 100; ++i)
        arrayOfIntegers[i] = i;

      final long[] arrayOfLongs = new long[100];
      for (int i = 0; i < 100; ++i)
        arrayOfLongs[i] = (long) i;

      final short[] arrayOfShorts = new short[100];
      for (int i = 0; i < 100; ++i)
        arrayOfShorts[i] = (short) i;

      final float[] arrayOfFloats = new float[100];
      for (int i = 0; i < 100; ++i)
        arrayOfFloats[i] = (float) i + 0.123f;

      final double[] arrayOfDoubles = new double[100];
      for (int i = 0; i < 100; ++i)
        arrayOfDoubles[i] = (double) i + 0.123f;

      database.begin();
      final MutableDocument v = database.newDocument("Test");

      v.set("arrayOfIntegers", arrayOfIntegers);
      v.set("arrayOfLongs", arrayOfLongs);
      v.set("arrayOfShorts", arrayOfShorts);
      v.set("arrayOfFloats", arrayOfFloats);
      v.set("arrayOfDoubles", arrayOfDoubles);

      final Binary buffer = serializer.serialize((DatabaseInternal) database, v);

      final ByteBuffer buffer2 = ByteBuffer.allocate((int) GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getDefValue());
      buffer2.put(buffer.toByteArray());
      buffer2.flip();

      final Binary buffer3 = new Binary(buffer2);
      buffer3.getByte(); // SKIP RECORD TYPE
      final Map<String, Object> record2 = serializer.deserializeProperties(database, buffer3, null, null);

      assertThat((int[]) record2.get("arrayOfIntegers")).isEqualTo(arrayOfIntegers);
      assertThat((long[]) record2.get("arrayOfLongs")).isEqualTo(arrayOfLongs);
      assertThat((short[]) record2.get("arrayOfShorts")).isEqualTo(arrayOfShorts);
      assertThat((float[]) record2.get("arrayOfFloats")).isEqualTo(arrayOfFloats);
      assertThat((double[]) record2.get("arrayOfDoubles")).isEqualTo(arrayOfDoubles);
    });
  }

  @Test
  void mapPropertiesInDocument() throws Exception {
    final BinarySerializer serializer = new BinarySerializer(database.getConfiguration());

    database.transaction(() -> {
      database.getSchema().createDocumentType("Test");
      database.commit();

      final Map<String, Boolean> mapOfStringsBooleans = new HashMap<>();
      mapOfStringsBooleans.put("true", true);
      mapOfStringsBooleans.put("false", false);

      final Map<Integer, Integer> mapOfIntegers = new LinkedHashMap<>();
      for (int i = 0; i < 100; ++i)
        mapOfIntegers.put(i, i);

      final Map<Long, Long> mapOfLongs = new HashMap<>();
      for (int i = 0; i < 100; ++i)
        mapOfLongs.put((long) i, (long) i);

      final Map<Short, Short> mapOfShorts = new LinkedHashMap<>();
      for (int i = 0; i < 100; ++i)
        mapOfShorts.put((short) i, (short) i);

      final Map<Float, Float> mapOfFloats = new LinkedHashMap<>();
      for (int i = 0; i < 100; ++i)
        mapOfFloats.put(((float) i) + 0.123f, ((float) i) + 0.123f);

      final Map<Double, Double> mapOfDoubles = new LinkedHashMap<>();
      for (int i = 0; i < 100; ++i)
        mapOfDoubles.put(((double) i) + 0.123f, ((double) i) + 0.123f);

      final Map<String, String> mapOfStrings = new HashMap<>();
      for (int i = 0; i < 100; ++i)
        mapOfStrings.put("" + i, "" + i);

      final Map<Object, Object> mapOfMixed = new HashMap<>();
      mapOfMixed.put("0", 0);
      mapOfMixed.put(1l, (long) 1);
      mapOfMixed.put("2short", (short) 2);
      mapOfMixed.put("3string", "3");

      database.begin();
      final MutableDocument v = database.newDocument("Test");

      v.set("mapOfStringsBooleans", mapOfStringsBooleans);
      v.set("mapOfIntegers", mapOfIntegers);
      v.set("mapOfLongs", mapOfLongs);
      v.set("mapOfShorts", mapOfShorts);
      v.set("mapOfFloats", mapOfFloats);
      v.set("mapOfDoubles", mapOfDoubles);
      v.set("mapOfStrings", mapOfStrings);
      v.set("mapOfMixed", mapOfMixed);

      final Binary buffer = serializer.serialize((DatabaseInternal) database, v);

      final ByteBuffer buffer2 = ByteBuffer.allocate((int) GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getDefValue());
      buffer2.put(buffer.toByteArray());
      buffer2.flip();

      final Binary buffer3 = new Binary(buffer2);
      buffer3.getByte(); // SKIP RECORD TYPE
      final Map<String, Object> record2 = serializer.deserializeProperties(database, buffer3, null, null);

      assertThat(record2.get("mapOfStringsBooleans")).isEqualTo(mapOfStringsBooleans);
      assertThat(record2.get("mapOfIntegers")).isEqualTo(mapOfIntegers);
      assertThat(record2.get("mapOfLongs")).isEqualTo(mapOfLongs);
      assertThat(record2.get("mapOfShorts")).isEqualTo(mapOfShorts);
      assertThat(record2.get("mapOfFloats")).isEqualTo(mapOfFloats);
      assertThat(record2.get("mapOfDoubles")).isEqualTo(mapOfDoubles);
      assertThat(record2.get("mapOfStrings")).isEqualTo(mapOfStrings);
      assertThat(record2.get("mapOfMixed")).isEqualTo(mapOfMixed);
    });
  }

  @Test
  void embedded() throws Exception {
    final BinarySerializer serializer = new BinarySerializer(database.getConfiguration());

    database.transaction(() -> {
      final DocumentType test = database.getSchema().createDocumentType("Test");
      test.createProperty("embedded", Type.EMBEDDED);

      final DocumentType embedded = database.getSchema().createDocumentType("Embedded");
      database.commit();

      database.begin();

      final MutableDocument testDocument = database.newDocument("Test");
      testDocument.set("id", 0);
      final MutableEmbeddedDocument embDocument1 = testDocument.newEmbeddedDocument("Embedded", "embedded");
      embDocument1.set("id", 1);
      embDocument1.save();

      final Binary buffer = serializer.serialize((DatabaseInternal) database, testDocument);

      final ByteBuffer buffer2 = ByteBuffer.allocate((int) GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getDefValue());
      buffer2.put(buffer.toByteArray());
      buffer2.flip();

      final Binary buffer3 = new Binary(buffer2);
      buffer3.getByte(); // SKIP RECORD TYPE
      final Map<String, Object> record2 = serializer.deserializeProperties(database, buffer3, new EmbeddedModifierProperty(testDocument, "embedded"), null);

      assertThat(record2.get("id")).isEqualTo(0);

      final EmbeddedDocument embeddedDoc = (EmbeddedDocument) record2.get("embedded");

      assertThat(embeddedDoc.get("id")).isEqualTo(1);
    });
  }

  @Test
  void listOfEmbedded() throws Exception {
    final BinarySerializer serializer = new BinarySerializer(database.getConfiguration());

    database.transaction(() -> {
      final DocumentType test = database.getSchema().createDocumentType("Test");
      test.createProperty("list", Type.LIST);

      final DocumentType embedded = database.getSchema().createDocumentType("Embedded");
      database.commit();

      database.begin();

      final MutableDocument testDocument = database.newDocument("Test");
      testDocument.set("id", 0);

      final List<Document> embeddedList = new ArrayList<>();
      testDocument.set("embedded", embeddedList);

      final MutableDocument embDocument1 = testDocument.newEmbeddedDocument("Embedded", "embedded");
      embDocument1.set("id", 1);
      final MutableDocument embDocument2 = testDocument.newEmbeddedDocument("Embedded", "embedded");
      embDocument2.set("id", 2);

      embDocument2.save();

      final Binary buffer = serializer.serialize((DatabaseInternal) database, testDocument);

      final ByteBuffer buffer2 = ByteBuffer.allocate((int) GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getDefValue());
      buffer2.put(buffer.toByteArray());
      buffer2.flip();

      final Binary buffer3 = new Binary(buffer2);
      buffer3.getByte(); // SKIP RECORD TYPE
      final Map<String, Object> record2 = serializer.deserializeProperties(database, buffer3, new EmbeddedModifierProperty(testDocument, "embedded"), null);

      assertThat(record2.get("id")).isEqualTo(0);

      final List<Document> embeddedList2 = (List<Document>) record2.get("embedded");

      assertThat(embeddedList).isEqualTo(embeddedList2);

      for (final Document d : embeddedList2)
        assertThat(d instanceof EmbeddedDocument).isTrue();
    });
  }

  @Test
  void mapOfEmbedded() throws Exception {
    final BinarySerializer serializer = new BinarySerializer(database.getConfiguration());

    database.transaction(() -> {
      final DocumentType test = database.getSchema().createDocumentType("Test");
      test.createProperty("list", Type.LIST);

      final DocumentType embedded = database.getSchema().createDocumentType("Embedded");
      database.commit();

      database.begin();

      final MutableDocument testDocument = database.newDocument("Test");
      testDocument.set("id", 0);

      final Map<Integer, Document> embeddedMap = new HashMap<>();
      testDocument.set("embedded", embeddedMap);

      final MutableDocument embDocument1 = testDocument.newEmbeddedDocument("Embedded", "embedded", 1);
      embDocument1.set("id", 1);
      final MutableDocument embDocument2 = testDocument.newEmbeddedDocument("Embedded", "embedded", 2);
      embDocument2.set("id", 2);

      embDocument2.save();

      embeddedMap.put(1, embDocument1);
      embeddedMap.put(2, embDocument2);

      final Binary buffer = serializer.serialize((DatabaseInternal) database, testDocument);

      final ByteBuffer buffer2 = ByteBuffer.allocate((int) GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getDefValue());
      buffer2.put(buffer.toByteArray());
      buffer2.flip();

      final Binary buffer3 = new Binary(buffer2);
      buffer3.getByte(); // SKIP RECORD TYPE
      final Map<String, Object> record2 = serializer.deserializeProperties(database, buffer3, null, null);

      assertThat(record2.get("id")).isEqualTo(0);

      final Map<Integer, Document> embeddedMap2 = (Map<Integer, Document>) record2.get("embedded");

      assertThat(embeddedMap).isEqualTo(embeddedMap2);
//      Assertions.assertIterableEquals(embeddedMap.entrySet(), embeddedMap2.entrySet());

      for (final Map.Entry<Integer, Document> d : embeddedMap2.entrySet()) {
        assertThat(d.getKey() instanceof Integer).isTrue();
        assertThat(d.getValue() instanceof EmbeddedDocument).isTrue();
      }
    });
  }

  @Test
  void deserializeUnknownTypeThrows() throws Exception {
    final BinarySerializer serializer = new BinarySerializer(database.getConfiguration());
    final Binary empty = new Binary();
    assertThatThrownBy(() -> serializer.deserializeValue(database, empty, (byte) 101, null))
        .isInstanceOf(SerializationException.class)
        .hasMessageContaining("101");
  }

  /**
   * Pins the defensive contract added with #4181: TYPE_DATE must reject value classes it
   * cannot encode rather than silently writing the type marker with no content bytes (which
   * mis-aligns the varint reader on every subsequent property in the record).
   */
  @Test
  void serializeDateWithUnsupportedTypeThrows() throws Exception {
    final BinarySerializer serializer = new BinarySerializer(database.getConfiguration());
    final Binary buffer = new Binary();
    assertThatThrownBy(() -> serializer.serializeValue(database, buffer, BinaryTypes.TYPE_DATE, Instant.now()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("DATE")
        .hasMessageContaining("Instant");
  }

  /**
   * Regression: a property whose value can't be serialized must not corrupt the rest of the record.
   * Before the fix, the nameId was written to the header before the type check, so skipping the
   * property left an orphan nameId and every subsequent property's contentPosition was misread.
   */
  @Test
  void propertyWithUnserializableTypeDoesNotCorruptOtherProperties() throws Exception {
    final BinarySerializer serializer = new BinarySerializer(database.getConfiguration());
    database.transaction(() -> {
      database.getSchema().createDocumentType("TestBadProp");

      final MutableDocument doc = database.newDocument("TestBadProp");
      doc.set("before", "first");
      doc.set("bad", new StringBuilder("unserializable"));
      doc.set("after", "last");
      doc.set("num", 42);

      final Binary buffer = serializer.serialize((DatabaseInternal) database, doc);

      final ByteBuffer roundtrip = ByteBuffer.allocate((int) GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getDefValue());
      roundtrip.put(buffer.toByteArray());
      roundtrip.flip();
      final Binary readBuffer = new Binary(roundtrip);
      readBuffer.getByte(); // SKIP RECORD TYPE

      final Map<String, Object> record2 = serializer.deserializeProperties(database, readBuffer, null, null);

      assertThat(record2).containsEntry("before", "first");
      assertThat(record2).containsEntry("after", "last");
      assertThat(record2).containsEntry("num", 42);
      assertThat(record2).doesNotContainKey("bad");
    });
  }

  /**
   * Regression: a Map value containing an unserializable entry must not corrupt the surrounding record.
   * Invalid values are stored as null so the key (and map size) is preserved; invalid keys drop the entry.
   */
  @Test
  void mapWithUnserializableEntryDoesNotCorruptRecord() throws Exception {
    final BinarySerializer serializer = new BinarySerializer(database.getConfiguration());
    database.transaction(() -> {
      database.getSchema().createDocumentType("TestBadMap");

      final MutableDocument doc = database.newDocument("TestBadMap");
      doc.set("id", 1);

      final Map<String, Object> mixedMap = new LinkedHashMap<>();
      mixedMap.put("good1", "hello");
      mixedMap.put("badValue", new StringBuilder("nope"));
      mixedMap.put("good2", 123);
      doc.set("m", mixedMap);

      doc.set("tail", "end");

      final Binary buffer = serializer.serialize((DatabaseInternal) database, doc);

      final ByteBuffer roundtrip = ByteBuffer.allocate((int) GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getDefValue());
      roundtrip.put(buffer.toByteArray());
      roundtrip.flip();
      final Binary readBuffer = new Binary(roundtrip);
      readBuffer.getByte(); // SKIP RECORD TYPE

      final Map<String, Object> record2 = serializer.deserializeProperties(database, readBuffer, null, null);

      assertThat(record2).containsEntry("id", 1);
      assertThat(record2).containsEntry("tail", "end");

      final Map<String, Object> roundtrippedMap = (Map<String, Object>) record2.get("m");
      assertThat(roundtrippedMap).hasSize(3);
      assertThat(roundtrippedMap).containsEntry("good1", "hello");
      assertThat(roundtrippedMap).containsEntry("good2", 123);
      assertThat(roundtrippedMap).containsKey("badValue");
      assertThat(roundtrippedMap.get("badValue")).isNull();
    });
  }

  /**
   * Regression: a List value containing an unserializable entry must not corrupt the surrounding record.
   * The invalid entry is stored as null so positions and list size are preserved.
   */
  @Test
  void listWithUnserializableEntryDoesNotCorruptRecord() throws Exception {
    final BinarySerializer serializer = new BinarySerializer(database.getConfiguration());
    database.transaction(() -> {
      database.getSchema().createDocumentType("TestBadList");

      final MutableDocument doc = database.newDocument("TestBadList");
      doc.set("id", 1);

      final List<Object> mixed = new ArrayList<>();
      mixed.add("a");
      mixed.add(new StringBuilder("nope"));
      mixed.add("b");
      mixed.add(99);
      doc.set("l", mixed);

      doc.set("tail", "end");

      final Binary buffer = serializer.serialize((DatabaseInternal) database, doc);

      final ByteBuffer roundtrip = ByteBuffer.allocate((int) GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getDefValue());
      roundtrip.put(buffer.toByteArray());
      roundtrip.flip();
      final Binary readBuffer = new Binary(roundtrip);
      readBuffer.getByte(); // SKIP RECORD TYPE

      final Map<String, Object> record2 = serializer.deserializeProperties(database, readBuffer, null, null);

      assertThat(record2).containsEntry("id", 1);
      assertThat(record2).containsEntry("tail", "end");

      final List<Object> roundtrippedList = (List<Object>) record2.get("l");
      assertThat(roundtrippedList).containsExactly("a", null, "b", 99);
    });
  }

  /**
   * Regression: a corrupted value byte for one property must not discard the whole record.
   * Per-property recovery keeps the other properties readable; the bad one is logged and skipped.
   */
  @Test
  void deserializeSkipsCorruptedPropertyAndReturnsRest() throws Exception {
    final BinarySerializer serializer = new BinarySerializer(database.getConfiguration());
    database.transaction(() -> {
      database.getSchema().createDocumentType("Corruptible");

      final MutableDocument doc = database.newDocument("Corruptible");
      doc.set("a", "alpha");
      doc.set("b", "beta");
      doc.set("c", "gamma");

      final Binary original = serializer.serialize((DatabaseInternal) database, doc);

      // Parse the header to find the absolute offset of property "b"'s type byte, then overwrite it
      // with an undefined type (101) to simulate corruption of that single property's value.
      original.position(1); // SKIP RECORD TYPE
      final int headerEndOffset = original.getInt();
      final int count = (int) original.getUnsignedNumber();

      int contentPosForB = -1;
      for (int i = 0; i < count; i++) {
        final int nameId = (int) original.getUnsignedNumber();
        final int contentPos = (int) original.getUnsignedNumber();
        if ("b".equals(database.getSchema().getDictionary().getNameById(nameId)))
          contentPosForB = contentPos;
      }
      assertThat(contentPosForB).isGreaterThanOrEqualTo(0);

      original.putByte(headerEndOffset + contentPosForB, (byte) 101);

      final ByteBuffer dest = ByteBuffer.allocate((int) GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getDefValue());
      dest.put(original.toByteArray());
      dest.flip();
      final Binary corrupted = new Binary(dest);
      corrupted.getByte(); // SKIP RECORD TYPE

      final Map<String, Object> result = serializer.deserializeProperties(database, corrupted, null, null);

      assertThat(result).doesNotContainKey("b");
      assertThat(result).containsEntry("a", "alpha");
      assertThat(result).containsEntry("c", "gamma");
    });
  }

  /**
   * Regression: a buffer positioned on the record type byte instead of on the properties section must not be answered
   * with property names the record never had. The misread decodes a zero header end offset and a garbage count that
   * both pass a bound against the buffer size alone, so the loop walks off the header into the values and resolves
   * whatever it finds against the dictionary: the accessors used to return {beta=null, Misaligned=null}, mixing a real
   * property name with the type name.
   */
  @Test
  void deserializeReportsMisalignedReadInsteadOfInventingProperties() throws Exception {
    final BinarySerializer serializer = new BinarySerializer(database.getConfiguration());
    database.transaction(() -> {
      database.getSchema().createDocumentType("Misaligned");

      final MutableDocument doc = database.newDocument("Misaligned");
      doc.set("alpha", "one");
      doc.set("beta", "two");

      final byte[] record = serializer.serialize((DatabaseInternal) database, doc).toByteArray();

      // Aligned read at position 1, right after the record type byte: the record is intact and stays readable.
      assertThat(serializer.deserializeProperties(database, bufferPositionedAt(record, 1), null, null))
          .containsEntry("alpha", "one")
          .containsEntry("beta", "two");

      // Misaligned read at position 0, the record type byte. Every accessor reading the property header must refuse
      // the header rather than invent properties out of the garbage it decodes.
      assertThat(serializer.deserializeProperties(database, bufferPositionedAt(record, 0), null, null)).isEmpty();
      assertThat(serializer.getPropertyNames(database, bufferPositionedAt(record, 0), null)).isEmpty();
      assertThat(serializer.hasProperty(database, bufferPositionedAt(record, 0), "beta", null)).isFalse();
      assertThat(serializer.deserializeProperty(database, bufferPositionedAt(record, 0), null, "beta", null)).isNull();
    });
  }

  /**
   * Regression: a property count that does not fit in the bytes left before the header ends is refused outright rather
   * than half-read. Bounding the count against the buffer size alone is not enough, because a count of 10 in a 20 byte
   * record passes that bound and still walks the header loop past the end of the header into the values section, where
   * whatever it decodes is invented. This particular byte pattern happens to yield the two real properties before it
   * runs out of buffer, but that is luck, not a contract: the same read shape is what answers
   * {beta=null, Misaligned=null} one byte earlier. Refusing a header that cannot be true is the point.
   */
  @Test
  void deserializeRejectsPropertyCountThatDoesNotFitTheHeader() throws Exception {
    final BinarySerializer serializer = new BinarySerializer(database.getConfiguration());
    database.transaction(() -> {
      database.getSchema().createDocumentType("Inflated");

      final MutableDocument doc = database.newDocument("Inflated");
      doc.set("alpha", "one");
      doc.set("beta", "two");

      final byte[] record = serializer.serialize((DatabaseInternal) database, doc).toByteArray();

      // The count varint follows the record type byte and the header end offset int.
      final int countPosition = Binary.BYTE_SERIALIZED_SIZE + Binary.INT_SERIALIZED_SIZE;
      assertThat(record[countPosition]).isEqualTo((byte) 2);
      record[countPosition] = 10;

      assertThat(serializer.deserializeProperties(database, bufferPositionedAt(record, 1), null, null)).isEmpty();
      assertThat(serializer.getPropertyNames(database, bufferPositionedAt(record, 1), null)).isEmpty();
    });
  }

  /**
   * Regression: a misalignment whose garbage header end offset lands past the end of the buffer used to return an
   * empty map with nothing logged at all, indistinguishable from a record that genuinely has no properties. The empty
   * map is the right answer, but it has to be reported.
   */
  @Test
  void deserializeReportsMisalignedReadThatDecodesToAnEmptyMap() throws Exception {
    final BinarySerializer serializer = new BinarySerializer(database.getConfiguration());
    final List<String> reported = new CopyOnWriteArrayList<>();
    final Logger originalLogger = LogManager.instance().getLogger();
    LogManager.instance().setLogger(new CapturingLogger(reported, originalLogger));
    try {
      database.transaction(() -> {
        database.getSchema().createDocumentType("SilentlyEmpty");

        final MutableDocument doc = database.newDocument("SilentlyEmpty");
        doc.set("alpha", "one");
        doc.set("beta", "two");

        final byte[] record = serializer.serialize((DatabaseInternal) database, doc).toByteArray();

        // Position 2 is one byte past the properties section: the header end offset decodes to a value far beyond the
        // buffer, which used to fall through to a silent empty map.
        assertThat(serializer.deserializeProperties(database, bufferPositionedAt(record, 2), null, null)).isEmpty();
      });

      assertThat(reported.stream().filter(m -> m.contains("Possible corrupted record")).toList())
          .as("a misaligned read must be reported instead of passing for an empty record (captured=%s)", reported)
          .isNotEmpty();
    } finally {
      LogManager.instance().setLogger(originalLogger);
    }
  }

  /**
   * Wraps a serialized record in a fresh {@link Binary} whose size is exactly the record size, positioned at the given
   * offset. A separate copy per call keeps each accessor under test independent of the others.
   */
  private static Binary bufferPositionedAt(final byte[] record, final int position) {
    final ByteBuffer dest = ByteBuffer.allocate(record.length);
    dest.put(record);
    dest.flip();
    final Binary buffer = new Binary(dest);
    buffer.position(position);
    return buffer;
  }

  /**
   * Captures WARNING-and-above messages into a list while forwarding every record to the production logger, so the
   * assertion does not depend on JUL configuration left behind by earlier tests in the same JVM.
   */
  private static final class CapturingLogger implements Logger {
    private final List<String> messages;
    private final Logger       delegate;

    CapturingLogger(final List<String> messages, final Logger delegate) {
      this.messages = messages;
      this.delegate = delegate;
    }

    private void capture(final Level level, final String message, final Object... args) {
      if (message == null || level.intValue() < Level.WARNING.intValue())
        return;
      String formatted = message;
      if (args != null && args.length > 0) {
        try {
          formatted = message.formatted(args);
        } catch (final Exception ignored) {
          // Fall back to the raw template, good enough for the substring matching above.
        }
      }
      messages.add(formatted);
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5,
        final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11,
        final Object arg12, final Object arg13, final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
      capture(level, message, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15,
          arg16, arg17);
      delegate.log(requester, level, message, exception, context, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10,
          arg11, arg12, arg13, arg14, arg15, arg16, arg17);
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object... args) {
      capture(level, message, args);
      delegate.log(requester, level, message, exception, context, args);
    }

    @Override
    public void flush() {
      delegate.flush();
    }
  }
}
