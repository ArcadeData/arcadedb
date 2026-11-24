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
import com.arcadedb.database.Document;
import com.arcadedb.database.EmbeddedDocument;
import com.arcadedb.database.ImmutableEmbeddedDocument;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.MutableEmbeddedDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.SerializationException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.GraphBatch;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.opencypher.temporal.CypherDuration;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12624: an enum constant held by an undeclared property was skipped by the serializer (only a WARNING), so the property
 * was missing after the commit, and an enum constant inside a list or a map was stored as null. {@code Type} maps {@link Enum} to
 * STRING and a declared STRING property already stored its text, so it is stored as that text, as a {@link Character} is since
 * #12021.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12624EnumPropertyTest extends TestHelper {

  enum Color {RED, GREEN}

  enum Shade {RED}

  /** A constant with a body is an instance of an anonymous subclass of the enum, and its text is its toString(). */
  enum Level {
    LOW {
      @Override
      public String toString() {
        return "low level";
      }
    }, HIGH
  }

  private Document writeAndReload(final String typeName, final String property, final Object value) {
    if (!database.getSchema().existsType(typeName))
      database.getSchema().createDocumentType(typeName);
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument(typeName).set(property, value).set("k", 1).save().getIdentity());
    return database.lookupByRID(rid[0], true).asDocument();
  }

  @Test
  void undeclaredEnumPropertyIsStoredAsItsText() {
    final Document doc = writeAndReload("EnumDoc", "color", Color.RED);
    assertThat(doc.getPropertyNames()).contains("color", "k");
    assertThat(doc.get("color")).isEqualTo("RED");
    assertThat(database.query("sql", "SELECT FROM EnumDoc WHERE color = 'RED'").stream().count()).isEqualTo(1L);
  }

  @Test
  void undeclaredAndDeclaredPropertiesStoreTheSameText() {
    database.getSchema().createDocumentType("EnumDeclared").createProperty("level", Type.STRING);
    for (final Level level : Level.values()) {
      final Object declared = writeAndReload("EnumDeclared", "level", level).get("level");
      final Object undeclared = writeAndReload("EnumUndeclared", "level", level).get("level");
      assertThat(undeclared).isEqualTo(declared).isEqualTo(level.toString());
    }
  }

  @Test
  void enumInsideACollectionIsNotStoredAsNull() {
    final List<Object> list = new ArrayList<>();
    list.add(Color.RED);
    list.add("keep");
    assertThat(writeAndReload("EnumList", "l", list).get("l")).isEqualTo(List.of("RED", "keep"));

    final Map<String, Object> map = new LinkedHashMap<>();
    map.put("c", Color.GREEN);
    assertThat(writeAndReload("EnumMapValue", "m", map).get("m")).isEqualTo(Map.of("c", "GREEN"));

    // A SET IS STORED AS A LIST
    assertThat(writeAndReload("EnumSet", "s", Set.of(Level.LOW)).get("s")).isEqualTo(List.of("low level"));

    // AN ARRAY OF ENUM CONSTANTS IS STORED AS A LIST
    assertThat(writeAndReload("EnumArray", "a", new Color[] { Color.GREEN, Color.RED }).get("a")).isEqualTo(List.of("GREEN", "RED"));
  }

  @Test
  void enumMapKeyIsStoredAsItsText() {
    final Map<Color, Object> byColor = new EnumMap<>(Color.class);
    byColor.put(Color.RED, 1);
    byColor.put(Color.GREEN, Level.HIGH);
    assertThat(writeAndReload("EnumMapKey", "m", byColor).get("m")).isEqualTo(Map.of("RED", 1, "GREEN", "HIGH"));
  }

  /**
   * An enum key is stored as its text, the same key a String or another enum's constant of that text is: the map is refused
   * instead of coming back with one of the two values.
   */
  @Test
  void enumKeyAndAnotherKeyOfTheSameTextAreRefused() {
    database.getSchema().createDocumentType("EnumCollide");

    final Map<Object, Object> withString = new LinkedHashMap<>();
    withString.put("RED", 1);
    withString.put(Color.RED, 2);
    final Map<Object, Object> twoEnums = new LinkedHashMap<>();
    twoEnums.put(Color.RED, 1);
    twoEnums.put(Shade.RED, 2);
    for (final Map<Object, Object> map : List.of(withString, twoEnums))
      assertThatThrownBy(() -> database.transaction(() -> database.newDocument("EnumCollide").set("m", map).save()))
          .isInstanceOf(SerializationException.class).hasMessageContaining("'RED'");
    assertThat(database.countType("EnumCollide", true)).isZero();
  }

  @Test
  void vertexAndEmbeddedEnumPropertiesSurviveTheCommit() {
    database.getSchema().createVertexType("EnumV");
    database.getSchema().createDocumentType("EnumEmbedded");
    final RID[] rid = new RID[1];
    database.transaction(() -> {
      final MutableDocument vertex = database.newVertex("EnumV").set("c", Color.GREEN);
      vertex.newEmbeddedDocument("EnumEmbedded", "e").set("c", Color.RED);
      rid[0] = vertex.save().getIdentity();
    });
    final Vertex vertex = database.lookupByRID(rid[0], true).asVertex();
    assertThat(vertex.get("c")).isEqualTo("GREEN");
    assertThat(vertex.getEmbedded("e").get("c")).isEqualTo("RED");
  }

  @Test
  void graphBatchEdgeEnumPropertyIsStoredAsItsText() {
    database.getSchema().createVertexType("EnumBatchV");
    database.getSchema().createEdgeType("EnumBatchE");
    final RID[] vertices = new RID[2];
    database.transaction(() -> {
      vertices[0] = database.newVertex("EnumBatchV").save().getIdentity();
      vertices[1] = database.newVertex("EnumBatchV").save().getIdentity();
    });

    try (final GraphBatch batch = GraphBatch.builder(database).build()) {
      batch.newEdge(vertices[0], "EnumBatchE", vertices[1], "c", Color.RED, "k", 1);
    }

    database.transaction(() -> {
      final Iterator<Edge> edges = vertices[0].asVertex().getEdges(Vertex.DIRECTION.OUT, "EnumBatchE").iterator();
      assertThat(edges.hasNext()).isTrue();
      final Edge edge = edges.next();
      assertThat(edge.get("c")).isEqualTo("RED");
      assertThat(edge.get("k")).isEqualTo(1);
    });
  }

  /**
   * The bulk edge writer of {@link GraphBatch} wrote the -1 "unknown type" tag of a value the serializer cannot write into the
   * record, which no read could decode afterwards ("Error on deserializing value of unknown type -1"): the enum constant did
   * that before this fix, and any other unsupported value still reaches the same branch. Such a value is stored as null, and the
   * rest of the edge stays readable.
   */
  @Test
  void graphBatchEdgeWithAnUnsupportedValueStaysReadable() {
    database.getSchema().createVertexType("EnumUnsupportedV");
    database.getSchema().createEdgeType("EnumUnsupportedE");
    final RID[] vertices = new RID[2];
    database.transaction(() -> {
      vertices[0] = database.newVertex("EnumUnsupportedV").save().getIdentity();
      vertices[1] = database.newVertex("EnumUnsupportedV").save().getIdentity();
    });

    try (final GraphBatch batch = GraphBatch.builder(database).build()) {
      batch.newEdge(vertices[0], "EnumUnsupportedE", vertices[1], "u", new Object(), "k", 1);
    }

    database.transaction(() -> {
      final Iterator<Edge> edges = vertices[0].asVertex().getEdges(Vertex.DIRECTION.OUT, "EnumUnsupportedE").iterator();
      assertThat(edges.hasNext()).isTrue();
      final Edge edge = edges.next();
      assertThat(edge.get("u")).isNull();
      assertThat(edge.get("k")).isEqualTo(1);
    });
  }

  /**
   * Census over {@code Type.TYPES_BY_USERTYPE}, the table {@code Type.validateValue} accepts a value by: every Java class it maps
   * to a type must be one the serializer can write, or a value the schema layer accepts is dropped at save. Character (#12021)
   * and Enum (#12624) were both in the table and both dropped. A class added to the table needs a sample below.
   */
  @Test
  void everyClassTypeMapsIsWrittenByTheSerializer() throws Exception {
    final Field field = Type.class.getDeclaredField("TYPES_BY_USERTYPE");
    field.setAccessible(true);
    final Map<Class<?>, Type> table = (Map<Class<?>, Type>) field.get(null);

    final Map<Class<?>, Object> samples = samples();
    final List<String> dropped = new ArrayList<>();
    for (final Class<?> javaClass : table.keySet()) {
      assertThat(samples).as("Type.TYPES_BY_USERTYPE maps %s: add a sample of it to this census", javaClass).containsKey(javaClass);
      final Object sample = samples.get(javaClass);
      assertThat(boxed(javaClass).isInstance(sample)).as("the sample of %s is an instance of it", javaClass).isTrue();
      if (BinaryTypes.getTypeFromValue(sample, null) == -1)
        dropped.add(javaClass.getName());
    }
    assertThat(dropped).as("classes Type.TYPES_BY_USERTYPE accepts that the serializer drops").isEmpty();

    // THE SCAN STILL REACHES THE TWO CLASSES THAT WERE DROPPED, SO IT RANGES OVER THE TABLE THAT ADMITTED THEM
    assertThat(table.keySet()).as("the census reads Type.TYPES_BY_USERTYPE").contains(Enum.class, Character.class, String.class);
  }

  private Map<Class<?>, Object> samples() {
    database.getSchema().createDocumentType("EnumCensusEmbedded");
    database.getSchema().createDocumentType("EnumCensusOwner");
    final MutableEmbeddedDocument[] mutableEmbedded = new MutableEmbeddedDocument[1];
    final RID[] owner = new RID[1];
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("EnumCensusOwner");
      mutableEmbedded[0] = doc.newEmbeddedDocument("EnumCensusEmbedded", "e");
      mutableEmbedded[0].set("x", 1);
      owner[0] = doc.save().getIdentity();
    });
    final EmbeddedDocument immutableEmbedded = database.lookupByRID(owner[0], true).asDocument().getEmbedded("e");
    assertThat(immutableEmbedded).isInstanceOf(ImmutableEmbeddedDocument.class);

    final Map<Class<?>, Object> samples = new HashMap<>();
    samples.put(Boolean.class, true);
    samples.put(Boolean.TYPE, true);
    samples.put(Integer.class, 1);
    samples.put(Integer.TYPE, 1);
    samples.put(BigInteger.class, BigInteger.TEN);
    samples.put(Short.class, (short) 1);
    samples.put(Short.TYPE, (short) 1);
    samples.put(Long.class, 1L);
    samples.put(Long.TYPE, 1L);
    samples.put(Float.class, 1F);
    samples.put(Float.TYPE, 1F);
    samples.put(Double.class, 1D);
    samples.put(Double.TYPE, 1D);
    samples.put(Date.class, new Date());
    samples.put(Calendar.class, Calendar.getInstance());
    samples.put(LocalDateTime.class, LocalDateTime.now());
    samples.put(LocalDate.class, LocalDate.now());
    samples.put(ZonedDateTime.class, ZonedDateTime.now());
    samples.put(OffsetDateTime.class, OffsetDateTime.now());
    samples.put(Instant.class, Instant.now());
    samples.put(String.class, "s");
    samples.put(Enum.class, Color.RED);
    samples.put(byte[].class, new byte[] { 1 });
    samples.put(Byte.class, (byte) 1);
    samples.put(Byte.TYPE, (byte) 1);
    samples.put(Character.class, 'c');
    samples.put(Character.TYPE, 'c');
    samples.put(BigDecimal.class, BigDecimal.ONE);
    samples.put(List.class, new ArrayList<>(List.of(1)));
    samples.put(Map.class, new HashMap<>(Map.of("a", 1)));
    samples.put(EmbeddedDocument.class, mutableEmbedded[0]);
    samples.put(ImmutableEmbeddedDocument.class, immutableEmbedded);
    samples.put(MutableEmbeddedDocument.class, mutableEmbedded[0]);
    samples.put(short[].class, new short[] { 1 });
    samples.put(int[].class, new int[] { 1 });
    samples.put(long[].class, new long[] { 1 });
    samples.put(float[].class, new float[] { 1 });
    samples.put(double[].class, new double[] { 1 });
    samples.put(OffsetTime.class, OffsetTime.now());
    samples.put(LocalTime.class, LocalTime.now());
    samples.put(CypherDuration.class, new CypherDuration(1, 2, 3, 4));
    return samples;
  }

  private static Class<?> boxed(final Class<?> javaClass) {
    if (!javaClass.isPrimitive())
      return javaClass;
    if (javaClass == Boolean.TYPE)
      return Boolean.class;
    if (javaClass == Integer.TYPE)
      return Integer.class;
    if (javaClass == Short.TYPE)
      return Short.class;
    if (javaClass == Long.TYPE)
      return Long.class;
    if (javaClass == Float.TYPE)
      return Float.class;
    if (javaClass == Double.TYPE)
      return Double.class;
    if (javaClass == Byte.TYPE)
      return Byte.class;
    if (javaClass == Character.TYPE)
      return Character.class;
    throw new IllegalArgumentException("No wrapper for " + javaClass);
  }
}
