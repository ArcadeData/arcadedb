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
package com.arcadedb.serializer.json;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

import java.util.*;

/**
 * JSON array.<br>
 * This API is compatible with org.json Java API, but uses Google GSON library under the hood. The main reason why we created this wrapper is
 * because the maintainer of the project org.json are not open to support ordered attributes as an option.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class JSONArray implements Iterable<Object> {
  private final JsonArray array;

  public JSONArray() {
    this.array = new JsonArray();
  }

  public JSONArray(final JsonArray input) {
    array = input;
  }

  public JSONArray(final String input) {
    try {
      array = (JsonArray) JsonParser.parseString(input);
    } catch (Exception e) {
      throw new JSONException("Invalid JSON array format");
    }
  }

  public JSONArray(final Collection<?> input) {
    this.array = new JsonArray();
    for (Object o : input)
      this.array.add(JSONObject.objectToElement(o));
  }

  public JSONArray(final String[] strings) {
    this.array = new JsonArray();
    for (String s : strings)
      this.array.add(s);
  }

  public JSONArray(final Object[] items) {
    this.array = new JsonArray();
    for (Object item : items)
      this.array.add(JSONObject.objectToElement(item));
  }

  public List<Object> toList() {
    return toList(false);
  }

  /**
   * Converts the array to a Java {@link List}.
   *
   * @param optimizeNumericArrays when {@code true}, homogeneous numeric arrays are returned as
   *                              primitive {@code float[]} instead of {@code List<Number>}. This
   *                              avoids both per-element boxing and the downstream double-to-float
   *                              narrowing required by {@link com.arcadedb.schema.Type#ARRAY_OF_FLOATS}
   *                              vector properties (issue #3864 follow-up). Used by the HTTP
   *                              command handler when receiving {@code params}. Note: callers
   *                              that need {@code double} precision should use the default
   *                              {@link #toList()} or convert downstream.
   *
   * @return the list (or a primitive array bag for nested numeric subtrees when optimized)
   */
  public List<Object> toList(final boolean optimizeNumericArrays) {
    final List<JsonElement> list = array.asList();
    final List<Object> result = new ArrayList<>(list.size());
    for (JsonElement e : list) {
      Object value = JSONObject.elementToObject(e);

      if (value instanceof JSONObject object)
        value = object.toMap(optimizeNumericArrays);
      else if (value instanceof JSONArray nArray) {
        if (optimizeNumericArrays) {
          final Object primitive = nArray.toPrimitiveNumericArrayOrNull();
          value = (primitive != null) ? primitive : nArray.toList(true);
        } else
          value = nArray.toList();
      }

      result.add(value);
    }

    return result;
  }

  /**
   * If this array contains only numeric primitive elements, returns a primitive numeric array
   * holding the values: {@code long[]} when every element is integer-valued in textual form
   * (no decimal point and no exponent), or {@code float[]} otherwise. Returns {@code null} when
   * the array is empty or contains a non-numeric element so callers can fall back to
   * {@link #toList()}.
   * <p>
   * Used by {@link JSONObject#toMap(boolean)} when parsing HTTP {@code params}.
   * <p>
   * Why two shapes: vector-embedding payloads (issue #3864 follow-up) need the {@code float[]}
   * fast path - the dominant use case is {@code ARRAY_OF_FLOATS} and storing vectors as
   * {@code float[]} halves memory vs. {@code double[]}. Integer payloads (issue #4148) cannot
   * go through {@code float[]}: float32 has 23 mantissa bits, so two distinct int64 values
   * within the same float bucket (~131072 wide near 2^40) collapse onto the same bits and the
   * {@code (long) f} cast in {@link com.arcadedb.schema.Type#convert} can't recover them.
   * Choosing {@code long[]} for integer-only arrays preserves int64 precision; downstream
   * {@code Type.convert} handles {@code long[] -> long[]/int[]/short[]/float[]/double[]}.
   */
  public Object toPrimitiveNumericArrayOrNull() {
    final List<JsonElement> list = array.asList();
    final int size = list.size();
    if (size == 0)
      return null;

    // First pass: classify elements. We check the textual form (Gson's LazilyParsedNumber holds
    // the source string) for any '.', 'e', 'E' character - if found, the JSON author intended
    // a floating-point value, even when it is integer-valued numerically (e.g. 1e3). This keeps
    // the integer-vs-float decision deterministic from the JSON text rather than from the parsed
    // numeric value.
    boolean allIntegers = true;
    for (int i = 0; i < size; i++) {
      final JsonElement e = list.get(i);
      if (!(e instanceof JsonPrimitive p) || !p.isNumber())
        return null;
      if (allIntegers) {
        final String s = p.getAsString();
        for (int c = 0, n = s.length(); c < n; c++) {
          final char ch = s.charAt(c);
          if (ch == '.' || ch == 'e' || ch == 'E') {
            allIntegers = false;
            break;
          }
        }
      }
    }

    if (allIntegers) {
      final long[] result = new long[size];
      for (int i = 0; i < size; i++)
        result[i] = ((JsonPrimitive) list.get(i)).getAsLong();
      return result;
    }

    final float[] result = new float[size];
    for (int i = 0; i < size; i++)
      result[i] = ((JsonPrimitive) list.get(i)).getAsFloat();
    return result;
  }

  public List<String> toListOfStrings() {
    return toList().stream().map(Object::toString).toList();
  }

  public List<Integer> toListOfIntegers() {
    return toList().stream().map(o -> ((Number) o).intValue()).toList();
  }

  public List<Long> toListOfLongs() {
    return toList().stream().map(o -> ((Number) o).longValue()).toList();
  }

  public List<Float> toListOfFloats() {
    return toList().stream().map(o -> ((Number) o).floatValue()).toList();
  }

  public List<Double> toListOfDoubles() {
    return toList().stream().map(o -> ((Number) o).doubleValue()).toList();
  }

  public List<Boolean> toListOfBooleans() {
    return toList().stream().map(o -> (Boolean) o).toList();
  }

  public List<JSONObject> toListOfObjects() {
    return toList().stream().map(o -> o instanceof Map map ? new JSONObject(map) : (JSONObject) o).toList();
  }

  public int length() {
    return array.size();
  }

  public String getString(final int i) {
    return array.get(i).getAsString();
  }

  public int getInt(final int i) {
    return array.get(i).getAsInt();
  }

  public long getLong(final int i) {
    return array.get(i).getAsLong();
  }

  public Number getNumber(final int i) {
    return array.get(i).getAsNumber();
  }

  public float getFloat(final int i) {
    return array.get(i).getAsFloat();
  }

  public double getDouble(final int i) {
    return array.get(i).getAsDouble();
  }

  public JSONObject getJSONObject(final int i) {
    return new JSONObject(array.get(i).getAsJsonObject());
  }

  public JSONArray getJSONArray(final int i) {
    return new JSONArray(array.get(i).getAsJsonArray());
  }

  public Object get(final int i) {
    return JSONObject.elementToObject(array.get(i));
  }

  public boolean isNull(final int i) {
    return array.get(i).isJsonNull();
  }

  public JSONArray put(final String object) {
    array.add(object);
    return this;
  }

  public JSONArray put(Number object) {
    if (Double.isNaN(object.doubleValue()) || Double.isInfinite(object.doubleValue()))
      object = 0;
    array.add(object);
    return this;
  }

  public JSONArray put(final int index, final Object object) {
    array.set(index, JSONObject.objectToElement(object));
    return this;
  }

  public JSONArray put(final Boolean object) {
    array.add(object);
    return this;
  }

  public JSONArray put(final Character object) {
    array.add(object);
    return this;
  }

  public JSONArray put(final JSONObject object) {
    array.add(object.getInternal());
    return this;
  }

  public JSONArray put(final Object object) {
    array.add(JSONObject.objectToElement(object));
    return this;
  }

  public Object remove(final int i) {
    final JsonElement old = array.remove(i);
    if (old != null)
      return JSONObject.elementToObject(old);
    return null;
  }

  public boolean isEmpty() {
    return array.isEmpty();
  }

  public String toString() {
    return JSONFactory.INSTANCE.getGson().toJson(array);
  }

  public JsonArray getInternal() {
    return array;
  }

  @Override
  public Iterator<Object> iterator() {
    final Iterator<JsonElement> iterator = array.iterator();
    return new Iterator<>() {
      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public Object next() {
        return JSONObject.elementToObject(iterator.next());
      }
    };
  }
}
