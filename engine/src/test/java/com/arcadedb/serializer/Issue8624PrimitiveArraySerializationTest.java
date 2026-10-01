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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8624: {@link JsonSerializer} rendered a primitive array (a vector embedding is a {@code float[]} of a thousand
 * elements or more, serialized once per search hit) by boxing it into an intermediate list and then running every element
 * back through the whole {@code serializeObject()} dispatch. The array is now written straight into the JSON array, and
 * the output must stay byte-identical to what the generic path produces for the same values held in a {@link List}:
 * that list path is the reference every assertion below compares against.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8624PrimitiveArraySerializationTest extends TestHelper {

  /** Every primitive array shape, with the values whose rendering is the easiest to get subtly wrong. */
  private static Map<String, Object> arrays() {
    final Map<String, Object> arrays = new LinkedHashMap<>();
    arrays.put("floats", new float[] { 1.5F, 0.1F, -0.0F, 3.4028235E38F, 1.4E-45F, Float.NaN, Float.POSITIVE_INFINITY,
        Float.NEGATIVE_INFINITY, Float.intBitsToFloat(0x7fc00001) });
    arrays.put("doubles", new double[] { 1.5D, 0.1D, -0.0D, Double.MAX_VALUE, Double.MIN_VALUE, Double.NaN,
        Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.longBitsToDouble(0x7ff8000000000001L) });
    arrays.put("ints", new int[] { Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE });
    arrays.put("longs", new long[] { Long.MIN_VALUE, -1L, 0L, 1L, Long.MAX_VALUE });
    arrays.put("shorts", new short[] { Short.MIN_VALUE, 0, Short.MAX_VALUE });
    arrays.put("bytes", new byte[] { Byte.MIN_VALUE, 0, 1, Byte.MAX_VALUE });
    arrays.put("booleans", new boolean[] { true, false, true });
    arrays.put("chars", new char[] { 'a', '"', '\\', 'è', '\n' });
    arrays.put("emptyFloats", new float[0]);
    arrays.put("strings", new String[] { "a", null, "c" });
    return arrays;
  }

  /** The same values boxed into a list, which goes through the generic collection path. */
  private static List<Object> boxed(final Object array) {
    final int length = Array.getLength(array);
    final List<Object> list = new ArrayList<>(length);
    for (int i = 0; i < length; i++)
      list.add(Array.get(array, i));
    return list;
  }

  @Test
  void aPrimitiveArrayRendersExactlyLikeTheSameValuesInAList() {
    for (final boolean collectionSize : new boolean[] { false, true }) {
      final JsonSerializer serializer = JsonSerializer.createJsonSerializer().setUseCollectionSize(collectionSize);

      for (final Map.Entry<String, Object> entry : arrays().entrySet()) {
        final ResultInternal asArray = new ResultInternal();
        asArray.setProperty("v", entry.getValue());
        final ResultInternal asList = new ResultInternal();
        asList.setProperty("v", boxed(entry.getValue()));

        assertThat(serializer.serializeResult(database, asArray).toString())
            .as("%s with useCollectionSize=%s", entry.getKey(), collectionSize)
            .isEqualTo(serializer.serializeResult(database, asList).toString());
      }
    }
  }

  @Test
  void nonFiniteValuesKeepTheirStringNames() {
    final ResultInternal result = new ResultInternal();
    result.setProperty("f", new float[] { Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, 2F });
    result.setProperty("d", new double[] { Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 2D });

    final JSONObject json = JsonSerializer.createJsonSerializer().serializeResult(database, result);

    assertThat(json.getJSONArray("f").toString()).isEqualTo("[\"NaN\",\"PosInfinity\",\"NegInfinity\",2.0]");
    assertThat(json.getJSONArray("d").toString()).isEqualTo("[\"NaN\",\"PosInfinity\",\"NegInfinity\",2.0]");
  }

  @Test
  void arraysNestedInACollectionOrAMapUseTheSameRendering() {
    final List<Object> embeddings = List.of(new float[] { 1F, Float.NaN }, new double[] { 0.25D });
    final Map<String, Object> byName = Map.of("e", new float[] { 0.5F, 0.75F });

    final ResultInternal asArrays = new ResultInternal();
    asArrays.setProperty("list", embeddings);
    asArrays.setProperty("map", byName);

    final ResultInternal asLists = new ResultInternal();
    asLists.setProperty("list", List.of(boxed(embeddings.get(0)), boxed(embeddings.get(1))));
    asLists.setProperty("map", Map.of("e", boxed(byName.get("e"))));

    final JsonSerializer serializer = JsonSerializer.createJsonSerializer();
    assertThat(serializer.serializeResult(database, asArrays).toString())
        .isEqualTo(serializer.serializeResult(database, asLists).toString());
  }

  /**
   * The document path: the arrays a record persists (the ARRAY_OF_* types and BINARY) are loaded back as primitive
   * arrays, and must render like the same values stored as a LIST.
   */
  @Test
  void persistedArraysRenderLikePersistedLists() {
    final Map<String, Object> persistable = new LinkedHashMap<>();
    for (final String name : List.of("floats", "doubles", "ints", "longs", "shorts", "bytes"))
      persistable.put(name, arrays().get(name));

    final RID[] rids = new RID[2];
    database.transaction(() -> {
      database.getSchema().createDocumentType("Issue8624Doc");
      final MutableDocument asArrays = database.newDocument("Issue8624Doc");
      final MutableDocument asLists = database.newDocument("Issue8624Doc");
      for (final Map.Entry<String, Object> entry : persistable.entrySet()) {
        asArrays.set(entry.getKey(), entry.getValue());
        asLists.set(entry.getKey(), boxed(entry.getValue()));
      }
      rids[0] = asArrays.save().getIdentity();
      rids[1] = asLists.save().getIdentity();
    });

    for (final boolean collectionSize : new boolean[] { false, true }) {
      final JsonSerializer serializer = JsonSerializer.createJsonSerializer().setUseCollectionSize(collectionSize);
      final JSONObject fromArrays = serializer.serializeDocument(rids[0].asDocument());
      final JSONObject fromLists = serializer.serializeDocument(rids[1].asDocument());

      for (final String name : persistable.keySet()) {
        assertThat(rids[0].asDocument().get(name).getClass().isArray()).as("%s must load back as an array", name).isTrue();
        assertThat(String.valueOf(fromArrays.get(name)))
            .as("%s with useCollectionSize=%s", name, collectionSize)
            .isEqualTo(String.valueOf(fromLists.get(name)));
      }
    }
  }
}
