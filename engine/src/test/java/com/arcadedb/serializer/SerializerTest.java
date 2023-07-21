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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.*;
import java.nio.*;
import java.util.*;

public class SerializerTest extends TestHelper {

  @Test
  public void testVarNumber() {
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

    Assertions.assertEquals(0, binary.getUnsignedNumber());
    Assertions.assertEquals(0, buffer.getUnsignedNumber());
    Assertions.assertEquals(3, binary.getUnsignedNumber());
    Assertions.assertEquals(3, buffer.getUnsignedNumber());
    Assertions.assertEquals(Short.MIN_VALUE, binary.getUnsignedNumber());
    Assertions.assertEquals(Short.MIN_VALUE, buffer.getUnsignedNumber());
    Assertions.assertEquals(Short.MAX_VALUE, binary.getUnsignedNumber());
    Assertions.assertEquals(Short.MAX_VALUE, buffer.getUnsignedNumber());
    Assertions.assertEquals(Integer.MIN_VALUE, binary.getUnsignedNumber());
    Assertions.assertEquals(Integer.MIN_VALUE, buffer.getUnsignedNumber());
    Assertions.assertEquals(Integer.MAX_VALUE, binary.getUnsignedNumber());
    Assertions.assertEquals(Integer.MAX_VALUE, buffer.getUnsignedNumber());
    Assertions.assertEquals(Long.MIN_VALUE, binary.getUnsignedNumber());
    Assertions.assertEquals(Long.MIN_VALUE, buffer.getUnsignedNumber());
    Assertions.assertEquals(Long.MAX_VALUE, binary.getUnsignedNumber());
    Assertions.assertEquals(Long.MAX_VALUE, buffer.getUnsignedNumber());

    Assertions.assertEquals(0, binary.getNumber());
    Assertions.assertEquals(0, buffer.getNumber());
    Assertions.assertEquals(3, binary.getNumber());
    Assertions.assertEquals(3, buffer.getNumber());
    Assertions.assertEquals(Short.MIN_VALUE, binary.getNumber());
    Assertions.assertEquals(Short.MIN_VALUE, buffer.getNumber());
    Assertions.assertEquals(Short.MAX_VALUE, binary.getNumber());
    Assertions.assertEquals(Short.MAX_VALUE, buffer.getNumber());
    Assertions.assertEquals(Integer.MIN_VALUE, binary.getNumber());
    Assertions.assertEquals(Integer.MIN_VALUE, buffer.getNumber());
    Assertions.assertEquals(Integer.MAX_VALUE, binary.getNumber());
    Assertions.assertEquals(Integer.MAX_VALUE, buffer.getNumber());
    Assertions.assertEquals(Long.MIN_VALUE, binary.getNumber());
    Assertions.assertEquals(Long.MIN_VALUE, buffer.getNumber());
    Assertions.assertEquals(Long.MAX_VALUE, binary.getNumber());
    Assertions.assertEquals(Long.MAX_VALUE, buffer.getNumber());

    Assertions.assertEquals(0, binary.getShort());
    Assertions.assertEquals(0, buffer.getShort());

    Assertions.assertEquals(Short.MIN_VALUE, binary.getShort());
    Assertions.assertEquals(Short.MIN_VALUE, buffer.getShort());

    Assertions.assertEquals(Short.MAX_VALUE, binary.getShort());
    Assertions.assertEquals(Short.MAX_VALUE, buffer.getShort());

    Assertions.assertEquals(0, binary.getInt());
    Assertions.assertEquals(0, buffer.getInt());

    Assertions.assertEquals(Integer.MIN_VALUE, binary.getInt());
    Assertions.assertEquals(Integer.MIN_VALUE, buffer.getInt());

    Assertions.assertEquals(Integer.MAX_VALUE, binary.getInt());
    Assertions.assertEquals(Integer.MAX_VALUE, buffer.getInt());

    Assertions.assertEquals(0l, binary.getLong());
    Assertions.assertEquals(0l, buffer.getLong());

    Assertions.assertEquals(Long.MIN_VALUE, binary.getLong());
    Assertions.assertEquals(Long.MIN_VALUE, buffer.getLong());

    Assertions.assertEquals(Long.MAX_VALUE, binary.getLong());
    Assertions.assertEquals(Long.MAX_VALUE, buffer.getLong());
  }

  @Test
  public void testLiteralPropertiesInDocument() throws ClassNotFoundException {
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

      final ByteBuffer buffer2 = ByteBuffer.allocate(((int) GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getDefValue()));
      buffer2.put(buffer.toByteArray());
      buffer2.flip();

      final Binary buffer3 = new Binary(buffer2);
      buffer3.getByte(); // SKIP RECORD TYPE
      final Map<String, Object> record2 = serializer.deserializeProperties(database, buffer3, null, null);

      Assertions.assertEquals(Integer.MIN_VALUE, record2.get("minInt"));
      Assertions.assertEquals(Integer.MAX_VALUE, record2.get("maxInt"));

      Assertions.assertEquals(Long.MIN_VALUE, record2.get("minLong"));
      Assertions.assertEquals(Long.MAX_VALUE, record2.get("maxLong"));

      Assertions.assertEquals(Short.MIN_VALUE, record2.get("minShort"));
      Assertions.assertEquals(Short.MAX_VALUE, record2.get("maxShort"));

      Assertions.assertEquals(Byte.MIN_VALUE, record2.get("minByte"));
      Assertions.assertEquals(Byte.MAX_VALUE, record2.get("maxByte"));

      Assertions.assertTrue(record2.get("decimal") instanceof BigDecimal);
      Assertions.assertEquals(new BigDecimal("9876543210.0123456789"), record2.get("decimal"));
      Assertions.assertEquals("Miner", record2.get("string"));
    });
  }

  @Test
  public void testListPropertiesInDocument() throws ClassNotFoundException {
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

      final ByteBuffer buffer2 = ByteBuffer.allocate(((int) GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getDefValue()));
      buffer2.put(buffer.toByteArray());
      buffer2.flip();

      final Binary buffer3 = new Binary(buffer2);
      buffer3.getByte(); // SKIP RECORD TYPE
      final Map<String, Object> record2 = serializer.deserializeProperties(database, buffer3, null, null);

      Assertions.assertIterableEquals(listOfBooleans, (Iterable<?>) record2.get("listOfBooleans"));
      Assertions.assertIterableEquals(listOfBooleans, (Iterable<?>) record2.get("arrayOfBooleans"));

      Assertions.assertIterableEquals(listOfIntegers, (Iterable<?>) record2.get("listOfIntegers"));
      Assertions.assertIterableEquals(listOfIntegers, (Iterable<?>) record2.get("arrayOfIntegers"));

      Assertions.assertIterableEquals(listOfLongs, (Iterable<?>) record2.get("listOfLongs"));
      Assertions.assertIterableEquals(listOfLongs, (Iterable<?>) record2.get("arrayOfLongs"));

      Assertions.assertIterableEquals(listOfShorts, (Iterable<?>) record2.get("listOfShorts"));
      Assertions.assertIterableEquals(listOfShorts, (Iterable<?>) record2.get("arrayOfShorts"));

      Assertions.assertIterableEquals(listOfFloats, (Iterable<?>) record2.get("listOfFloats"));
      Assertions.assertIterableEquals(listOfFloats, (Iterable<?>) record2.get("arrayOfFloats"));

      Assertions.assertIterableEquals(listOfDoubles, (Iterable<?>) record2.get("listOfDoubles"));
      Assertions.assertIterableEquals(listOfDoubles, (Iterable<?>) record2.get("arrayOfDoubles"));

      Assertions.assertIterableEquals(listOfStrings, (Iterable<?>) record2.get("listOfStrings"));
      Assertions.assertIterableEquals(listOfStrings, (Iterable<?>) record2.get("arrayOfStrings"));

      Assertions.assertIterableEquals(listOfMixed, (Iterable<?>) record2.get("listOfMixed"));
      Assertions.assertIterableEquals(listOfMixed, (Iterable<?>) record2.get("arrayOfMixed"));
    });
  }

  @Test
  public void testArraysOfPrimitive() throws ClassNotFoundException {
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

      final ByteBuffer buffer2 = ByteBuffer.allocate(((int) GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getDefValue()));
      buffer2.put(buffer.toByteArray());
      buffer2.flip();

      final Binary buffer3 = new Binary(buffer2);
      buffer3.getByte(); // SKIP RECORD TYPE
      final Map<String, Object> record2 = serializer.deserializeProperties(database, buffer3, null, null);

      Assertions.assertArrayEquals(arrayOfIntegers, (int[]) record2.get("arrayOfIntegers"));
      Assertions.assertArrayEquals(arrayOfLongs, (long[]) record2.get("arrayOfLongs"));
      Assertions.assertArrayEquals(arrayOfShorts, (short[]) record2.get("arrayOfShorts"));
      Assertions.assertArrayEquals(arrayOfFloats, (float[]) record2.get("arrayOfFloats"));
      Assertions.assertArrayEquals(arrayOfDoubles, (double[]) record2.get("arrayOfDoubles"));
    });
  }

  @Test
  public void testMapPropertiesInDocument() throws ClassNotFoundException {
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

      final ByteBuffer buffer2 = ByteBuffer.allocate(((int) GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getDefValue()));
      buffer2.put(buffer.toByteArray());
      buffer2.flip();

      final Binary buffer3 = new Binary(buffer2);
      buffer3.getByte(); // SKIP RECORD TYPE
      final Map<String, Object> record2 = serializer.deserializeProperties(database, buffer3, null, null);

      Assertions.assertEquals(mapOfStringsBooleans, record2.get("mapOfStringsBooleans"));
      Assertions.assertEquals(mapOfIntegers, record2.get("mapOfIntegers"));
      Assertions.assertEquals(mapOfLongs, record2.get("mapOfLongs"));
      Assertions.assertEquals(mapOfShorts, record2.get("mapOfShorts"));
      Assertions.assertEquals(mapOfFloats, record2.get("mapOfFloats"));
      Assertions.assertEquals(mapOfDoubles, record2.get("mapOfDoubles"));
      Assertions.assertEquals(mapOfStrings, record2.get("mapOfStrings"));
      Assertions.assertEquals(mapOfMixed, record2.get("mapOfMixed"));
    });
  }

  @Test
  public void testEmbedded() throws ClassNotFoundException {
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

      final ByteBuffer buffer2 = ByteBuffer.allocate(((int) GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getDefValue()));
      buffer2.put(buffer.toByteArray());
      buffer2.flip();

      final Binary buffer3 = new Binary(buffer2);
      buffer3.getByte(); // SKIP RECORD TYPE
      final Map<String, Object> record2 = serializer.deserializeProperties(database, buffer3, new EmbeddedModifierProperty(testDocument, "embedded"), null);

      Assertions.assertEquals(0, record2.get("id"));

      final EmbeddedDocument embeddedDoc = (EmbeddedDocument) record2.get("embedded");

      Assertions.assertEquals(1, embeddedDoc.get("id"));
    });
  }

  @Test
  public void testListOfEmbedded() throws ClassNotFoundException {
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

      final ByteBuffer buffer2 = ByteBuffer.allocate(((int) GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getDefValue()));
      buffer2.put(buffer.toByteArray());
      buffer2.flip();

      final Binary buffer3 = new Binary(buffer2);
      buffer3.getByte(); // SKIP RECORD TYPE
      final Map<String, Object> record2 = serializer.deserializeProperties(database, buffer3, new EmbeddedModifierProperty(testDocument, "embedded"), null);

      Assertions.assertEquals(0, record2.get("id"));

      final List<Document> embeddedList2 = (List<Document>) record2.get("embedded");

      Assertions.assertIterableEquals(embeddedList, embeddedList2);

      for (final Document d : embeddedList2)
        Assertions.assertTrue(d instanceof EmbeddedDocument);
    });
  }

  @Test
  public void testMapOfEmbedded() throws ClassNotFoundException {
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

      final ByteBuffer buffer2 = ByteBuffer.allocate(((int) GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getDefValue()));
      buffer2.put(buffer.toByteArray());
      buffer2.flip();

      final Binary buffer3 = new Binary(buffer2);
      buffer3.getByte(); // SKIP RECORD TYPE
      final Map<String, Object> record2 = serializer.deserializeProperties(database, buffer3, null, null);

      Assertions.assertEquals(0, record2.get("id"));

      final Map<Integer, Document> embeddedMap2 = (Map<Integer, Document>) record2.get("embedded");

      Assertions.assertIterableEquals(embeddedMap.entrySet(), embeddedMap2.entrySet());

      for (final Map.Entry<Integer, Document> d : embeddedMap2.entrySet()) {
        Assertions.assertTrue(d.getKey() instanceof Integer);
        Assertions.assertTrue(d.getValue() instanceof EmbeddedDocument);
      }
    });
  }
}
