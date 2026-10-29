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
package com.arcadedb.index.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10938: an {@code ARRAY_OF_FLOATS} property accepted a scalar (and any other value that is not an array of numbers),
 * stored it as it came, and a later {@code CREATE INDEX ... LSM_VECTOR} skipped that row with a SEVERE log line and reported
 * success with an index that silently lacked it. A write to a declared {@code ARRAY_OF_*} property now refuses a value that is
 * not an array of numbers, whether or not an index exists yet, and a vector index build refuses rows it cannot index, naming
 * them, instead of building an index without them.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10938ArrayOfFloatsScalarTest extends TestHelper {

  @Test
  void aScalarIsRefusedForAnArrayOfFloatsPropertyWithoutAnyIndex() {
    database.command("sql", "CREATE DOCUMENT TYPE D");
    database.command("sql", "CREATE PROPERTY D.embedding ARRAY_OF_FLOATS");

    database.transaction(() -> {
      assertThatThrownBy(() -> database.command("sql", "INSERT INTO D SET embedding = ?", 0.5d))//
          .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("embedding");
      assertThatThrownBy(() -> database.newDocument("D").set("embedding", 0.5f).save())//
          .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("embedding");
      assertThatThrownBy(() -> database.newDocument("D").set("embedding", "[0.1, 0.2]").save())//
          .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("embedding");
      assertThatThrownBy(() -> database.newDocument("D").set("embedding", Map.of("a", 1)).save())//
          .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("embedding");
    });

    try (final ResultSet rs = database.query("sql", "SELECT count() AS c FROM D")) {
      assertThat(rs.next().<Long>getProperty("c")).isZero();
    }
  }

  @Test
  void everyArrayTypeRefusesAScalarAndTakesEveryArrayOfNumbers() {
    final DocumentType type = database.getSchema().createDocumentType("A");
    final Map<String, Type> properties = new HashMap<>();
    properties.put("shorts", Type.ARRAY_OF_SHORTS);
    properties.put("ints", Type.ARRAY_OF_INTEGERS);
    properties.put("longs", Type.ARRAY_OF_LONGS);
    properties.put("floats", Type.ARRAY_OF_FLOATS);
    properties.put("doubles", Type.ARRAY_OF_DOUBLES);
    for (final Map.Entry<String, Type> entry : properties.entrySet())
      type.createProperty(entry.getKey(), entry.getValue());

    final Object[] arraysOfNumbers = { new short[] { 1, 2 }, new int[] { 1, 2 }, new long[] { 1, 2 }, new float[] { 1, 2 },
        new double[] { 1, 2 }, new Short[] { 1, 2 }, new Integer[] { 1, 2 }, new Long[] { 1L, 2L }, new Float[] { 1f, 2f },
        new Double[] { 1d, 2d }, new Number[] { 1, 2L }, new Object[] { 1, 2d }, List.of(1, 2) };

    database.transaction(() -> {
      for (final Map.Entry<String, Type> entry : properties.entrySet()) {
        final String name = entry.getKey();
        final Class<?> javaClass = entry.getValue().getDefaultJavaType();

        for (final Object scalar : new Object[] { 1, 1L, 0.5f, 0.5d, (short) 1, "1", true })
          assertThatThrownBy(() -> database.newDocument("A").set(name, scalar).save())//
              .as("%s must refuse the scalar %s", name, scalar).isInstanceOf(IllegalArgumentException.class);

        for (final Object array : arraysOfNumbers) {
          final MutableDocument doc = database.newDocument("A").set(name, array).save();
          final Object stored = doc.get(name);
          assertThat(stored).as("%s from %s", name, array.getClass().getSimpleName()).isInstanceOf(javaClass);
          assertThat(Array.getLength(stored)).isEqualTo(2);
          assertThat(((Number) Array.get(stored, 1)).intValue()).isEqualTo(2);
        }

        assertThatThrownBy(() -> database.newDocument("A").set(name, new Object[] { 1, "x" }).save())//
            .as("%s must refuse an array with a non-numeric item", name).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> database.newDocument("A").set(name, new Object[] { 1, null }).save())//
            .as("%s must refuse an array with a null item", name).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> database.newDocument("A").set(name, List.of("1", "2")).save())//
            .as("%s must refuse a list of strings, even numeric ones", name).isInstanceOf(IllegalArgumentException.class);
      }
    });
  }

  @Test
  void aVectorIndexBuildRefusesRowsItCannotIndex() {
    // A row an older version let through: written while the property was not declared yet, so nothing converted it
    database.command("sql", "CREATE DOCUMENT TYPE B");
    database.command("sql", "CREATE PROPERTY B.id INTEGER");

    final List<RID> bad = new ArrayList<>();
    database.transaction(() -> {
      database.newDocument("B").set("id", 1).set("embedding", new float[] { 0.9f, 0.1f, 0f }).save();
      bad.add(database.newDocument("B").set("id", 2).set("embedding", 0.5d).save().getIdentity());
      database.newDocument("B").set("id", 3).save(); // no vector at all: not indexed, and no error
      bad.add(database.newDocument("B").set("id", 4).set("embedding", new float[] { 0.9f, 0.1f }).save().getIdentity());
      bad.add(database.newDocument("B").set("id", 5).set("embedding", new float[] { Float.NaN, 0.1f, 0f }).save().getIdentity());
    });

    database.command("sql", "CREATE PROPERTY B.embedding ARRAY_OF_FLOATS");

    // a record written before the declaration is read back as it is, and an update of another property does not refuse it
    database.transaction(() -> {
      assertThat(bad.get(0).asDocument().get("embedding")).isEqualTo(0.5d);
      bad.get(0).asDocument().modify().set("note", "touched").save();
    });

    assertThatThrownBy(() -> database.command("sql",
        "CREATE INDEX ON B (embedding) LSM_VECTOR METADATA {\"dimensions\": 3, \"similarity\": \"COSINE\"}"))//
        .hasStackTraceContaining("3 records")//
        .hasStackTraceContaining(bad.get(0).toString())//
        .hasStackTraceContaining(bad.get(1).toString())//
        .hasStackTraceContaining(bad.get(2).toString());

    assertThat(database.getSchema().existsIndex("B[embedding]")).as("an index that could not be built is gone").isFalse();

    // once the rows are fixed the build succeeds and every vector is searchable
    database.transaction(() -> {
      for (final RID rid : bad)
        rid.asDocument().modify().set("embedding", new float[] { 0.8f, 0.2f, 0f }).save();
    });

    database.command("sql", "CREATE INDEX ON B (embedding) LSM_VECTOR METADATA {\"dimensions\": 3, \"similarity\": \"COSINE\"}");

    final List<Integer> found = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT expand(vector.neighbors('B[embedding]', [0.9, 0.1, 0.0], 5))")) {
      while (rs.hasNext())
        found.add(rs.next().getProperty("id"));
    }
    assertThat(found).containsExactlyInAnyOrder(1, 2, 4, 5);
  }
}
