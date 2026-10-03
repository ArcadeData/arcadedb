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
import com.google.gson.JsonNull;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

import java.math.BigDecimal;
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

  /**
   * An empty array sized for {@code initialCapacity} elements, for a caller that knows how many it is about to add.
   */
  public JSONArray(final int initialCapacity) {
    this.array = new JsonArray(initialCapacity);
  }

  public JSONArray(final JsonArray input) {
    array = input;
  }

  public JSONArray(final String input) {
    try {
      array = (JsonArray) JsonParser.parseString(input);
    } catch (Exception e) {
      throw new JSONException("Invalid JSON array format: " + input, e);
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
   *                              primitive {@code long[]}/{@code double[]} instead of {@code List<Number>}. This
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
          value = primitive != null ? primitive : nArray.toList(true);
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
   * (no decimal point and no exponent), or {@code double[]} when every element has a fraction or an exponent. Returns {@code null} when
   * the array is empty or contains a non-numeric element so callers can fall back to
   * {@link #toList()}.
   * <p>
   * Used by {@link JSONObject#toMap(boolean)} when parsing HTTP {@code params}.
   * <p>
   * Why these shapes: vector-embedding payloads (issue #3864 follow-up) avoid millions of boxed numbers. The array is exact:
   * {@code long[]} when every element is integer-valued (issue #4148), {@code double[]} when every element has a fraction or an
   * exponent ({@code float[]} narrowed every element to float32, issue #9003). A mixed array, an integer beyond the long range or
   * a decimal with more digits than a double holds returns {@code null}, so the list keeps each element as written (an int
   * stays an int, a big number stays a {@link BigDecimal}). {@code Type.convert} turns {@code double[]} into the {@code float[]}
   * of an {@code ARRAY_OF_FLOATS} property, and a primitive array written to a {@code LIST} property becomes its elements.
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
    boolean allFractions = true;
    for (int i = 0; i < size; i++) {
      final JsonElement e = list.get(i);
      if (!(e instanceof JsonPrimitive p) || !p.isNumber())
        return null;
      final String s = p.getAsString();
      boolean fraction = false;
      for (int c = 0, n = s.length(); c < n; c++) {
        final char ch = s.charAt(c);
        if (ch == '.' || ch == 'e' || ch == 'E') {
          fraction = true;
          break;
        }
      }
      if (fraction)
        allIntegers = false;
      else
        allFractions = false;
      // MIXED ARRAYS STAY LISTS: A PRIMITIVE ARRAY WOULD TURN THE INTEGERS INTO FLOATING POINT VALUES (OR THE OTHER WAY AROUND)
      if (!allIntegers && !allFractions)
        return null;
    }

    if (allIntegers) {
      final long[] result = new long[size];
      for (int i = 0; i < size; i++) {
        try {
          result[i] = Long.parseLong(((JsonPrimitive) list.get(i)).getAsString());
        } catch (final NumberFormatException ex) {
          // BEYOND THE LONG RANGE: THE LIST KEEPS EVERY DIGIT (BigDecimal), A long[] WOULD WRAP IT AROUND
          return null;
        }
      }
      return result;
    }

    // EXACT: A float[] WOULD NARROW EVERY ELEMENT TO 24 BITS OF PRECISION, EVEN WHEN THE ARRAY IS NOT A VECTOR (issue #9003)
    final double[] result = new double[size];
    for (int i = 0; i < size; i++) {
      final String token = ((JsonPrimitive) list.get(i)).getAsString();
      if (token.length() <= 15)
        // FEW ENOUGH DIGITS: A double HOLDS THEM ALL
        result[i] = Double.parseDouble(token);
      else {
        // MORE DIGITS THAN A double HOLDS: THE LIST KEEPS THEM (BigDecimal)
        final Object value = JSONObject.elementToObject(list.get(i));
        if (value instanceof BigDecimal)
          return null;
        result[i] = ((Number) value).doubleValue();
      }
    }
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

  /**
   * Returns the string value at the given position.
   *
   * @throws JSONException if the position is out of range, the value is null or it is not a string.
   */
  public String getString(final int i) {
    final JsonElement value = getNotNullElement(i);
    try {
      return value.getAsString();
    } catch (UnsupportedOperationException | IllegalStateException e) {
      throw typeError(i, "string", value, e);
    }
  }

  /**
   * Returns the integer value at the given position.
   *
   * @throws JSONException if the position is out of range, the value is null or it is not a number.
   */
  public int getInt(final int i) {
    final JsonElement value = getNotNullElement(i);
    try {
      return value.getAsInt();
    } catch (UnsupportedOperationException | IllegalStateException | NumberFormatException | ClassCastException e) {
      throw typeError(i, "int", value, e);
    }
  }

  /**
   * Returns the long value at the given position.
   *
   * @throws JSONException if the position is out of range, the value is null or it is not a number.
   */
  public long getLong(final int i) {
    final JsonElement value = getNotNullElement(i);
    try {
      return value.getAsLong();
    } catch (UnsupportedOperationException | IllegalStateException | NumberFormatException | ClassCastException e) {
      throw typeError(i, "long", value, e);
    }
  }

  /**
   * Returns the number at the given position.
   *
   * @throws JSONException if the position is out of range, the value is null or it is not a number.
   */
  public Number getNumber(final int i) {
    final JsonElement value = getNotNullElement(i);
    try {
      return value.getAsNumber();
    } catch (UnsupportedOperationException | IllegalStateException | NumberFormatException | ClassCastException e) {
      throw typeError(i, "number", value, e);
    }
  }

  /**
   * Returns the float value at the given position.
   *
   * @throws JSONException if the position is out of range, the value is null or it is not a number.
   */
  public float getFloat(final int i) {
    final JsonElement value = getNotNullElement(i);
    try {
      return value.getAsFloat();
    } catch (UnsupportedOperationException | IllegalStateException | NumberFormatException | ClassCastException e) {
      throw typeError(i, "float", value, e);
    }
  }

  /**
   * Returns the double value at the given position.
   *
   * @throws JSONException if the position is out of range, the value is null or it is not a number.
   */
  public double getDouble(final int i) {
    final JsonElement value = getNotNullElement(i);
    try {
      return value.getAsDouble();
    } catch (UnsupportedOperationException | IllegalStateException | NumberFormatException | ClassCastException e) {
      throw typeError(i, "double", value, e);
    }
  }

  /**
   * Returns the nested object at the given position.
   *
   * @throws JSONException if the position is out of range, the value is null or it is not a JSON object.
   */
  public JSONObject getJSONObject(final int i) {
    final JsonElement value = getNotNullElement(i);
    if (!value.isJsonObject())
      throw typeError(i, "JSON object", value, null);
    return new JSONObject(value.getAsJsonObject());
  }

  /**
   * Returns the nested array at the given position.
   *
   * @throws JSONException if the position is out of range, the value is null or it is not a JSON array.
   */
  public JSONArray getJSONArray(final int i) {
    final JsonElement value = getNotNullElement(i);
    if (!value.isJsonArray())
      throw typeError(i, "JSON array", value, null);
    return new JSONArray(value.getAsJsonArray());
  }

  /**
   * Returns the value at the given position, or {@code null} if the value is JSON null.
   *
   * @throws JSONException if the position is out of range.
   */
  public Object get(final int i) {
    return JSONObject.elementToObject(getElement(i));
  }

  /**
   * Returns the value at the given position as a {@link BigDecimal}, parsed from its JSON text so no precision is lost
   * through a double.
   *
   * @throws JSONException if the position is out of range, the value is null or it is not a number.
   */
  public BigDecimal getBigDecimal(final int i) {
    final JsonElement value = getNotNullElement(i);
    try {
      return value.getAsBigDecimal();
    } catch (final UnsupportedOperationException | IllegalStateException | NumberFormatException | ClassCastException e) {
      throw typeError(i, "BigDecimal", value, e);
    }
  }

  /**
   * Returns {@code true} if the value at the given position is JSON null.
   *
   * @throws JSONException if the position is out of range.
   */
  public boolean isNull(final int i) {
    return getElement(i).isJsonNull();
  }

  public JSONArray put(final String object) {
    array.add(object);
    return this;
  }

  public JSONArray put(final Number object) {
    // NaN AND THE INFINITIES HAVE NO JSON LITERAL: null, as JSONObject.put(String, Number) does; a null number is JSON null too
    array.add(object == null || JSONObject.isNonFinite(object) ? JsonNull.INSTANCE : new JsonPrimitive(object));
    return this;
  }

  public JSONArray put(final int index, final Object object) {
    checkIndex(index);
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

  /**
   * Removes the value at the given position and returns it.
   *
   * @throws JSONException if the position is out of range.
   */
  public Object remove(final int i) {
    checkIndex(i);
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

  /**
   * Validates the position against the array bounds, reporting the failure as a {@link JSONException} instead of the
   * {@code IndexOutOfBoundsException} raised by the backing list.
   */
  private void checkIndex(final int i) {
    if (i < 0 || i >= array.size())
      throw new JSONException("JSONArray[" + i + "] not found: the array has " + array.size() + " element(s)");
  }

  private JsonElement getElement(final int i) {
    checkIndex(i);
    return array.get(i);
  }

  /**
   * Returns the element at the given position, guaranteed to be neither out of range nor JSON null. GSON models an
   * explicit JSON null as a {@link JsonNull} entry, whose converters raise
   * {@code UnsupportedOperationException} instead of the documented {@link JSONException} (issue #5935).
   */
  private JsonElement getNotNullElement(final int i) {
    final JsonElement value = getElement(i);
    if (value.isJsonNull())
      throw new JSONException("JSONArray[" + i + "] is null");

    return value;
  }

  private static JSONException typeError(final int i, final String expectedType, final JsonElement value,
      final RuntimeException cause) {
    return new JSONException("JSONArray[" + i + "] is not a " + expectedType + " (" + JSONObject.describe(value) + ")", cause);
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
