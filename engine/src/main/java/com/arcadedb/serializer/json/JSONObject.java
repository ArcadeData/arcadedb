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

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.utility.DateUtils;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.Strictness;
import com.google.gson.internal.LazilyParsedNumber;
import com.google.gson.stream.JsonReader;

import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.util.*;

/**
 * JSON object.<br>
 * This API is compatible with org.json Java API, but uses Google GSON library under the hood. The main reason why we created this wrapper is
 * because the maintainer of the project org.json are not open to support ordered attributes as an option.
 * <p>
 * The class also implements Map to be managed by GraalVM as a native object.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class JSONObject implements Map<String, Object> {
  public static final JsonNull          NULL                   = JsonNull.INSTANCE;
  /**
   * Cap on how much of an offending value or property name is echoed in an error message, so a large payload cannot
   * be reflected back through an exception. See {@link #describe(JsonElement)} and {@link #truncate(String)}.
   */
  static final        int               MAX_ERROR_VALUE_LENGTH = 64;
  private final       JsonObject        object;
  private             String            dateFormatAsString     = null;
  private             DateTimeFormatter dateFormat             = null;
  private             String            dateTimeFormatAsString;
  private             DateTimeFormatter dateTimeFormat;

  public JSONObject() {
    this.object = new JsonObject();
  }

  public JSONObject(final JsonObject input) {
    this.object = input;
  }

  public JSONObject(final String input) {
    if (input != null) {
      try {
        final JsonReader reader = new JsonReader(new StringReader(input));
        reader.setStrictness(Strictness.LENIENT);
        object = JsonParser.parseReader(reader).getAsJsonObject();
      } catch (Exception e) {
        throw new JSONException("Invalid JSON object format: " + input, e);
      }
    } else
      object = new JsonObject();
  }

  public JSONObject(final Map<String, ?> map) {
    object = new JsonObject();
    if (map != null)
      for (Map.Entry<String, ?> entry : map.entrySet())
        put(entry.getKey(), entry.getValue());
  }

  public JSONObject copy() {
    return new JSONObject(object.deepCopy());
  }

  public JSONObject put(final String name, final String value) {
    object.addProperty(name, value);
    return this;
  }

  public JSONObject put(final String name, final Number value) {
    object.addProperty(name, isNonFinite(value) ? null : value);
    return this;
  }

  public JSONObject put(final String name, final Boolean value) {
    object.addProperty(name, value);
    return this;
  }

  public JSONObject put(final String name, final Character value) {
    object.addProperty(name, value);
    return this;
  }

  public JSONObject put(final String name, final Object value) {
    if (name == null)
      throw new IllegalArgumentException("Property name is null");

    switch (value) {
    case null -> object.add(name, NULL);
    case JsonNull jsonNull -> object.add(name, NULL);
    case JsonElement jsonElement -> object.add(name, jsonElement);
    case String string -> put(name, string);
    case Number number -> put(name, number); // HANDLE CONVERSION OF NaN/INF
    case Boolean bool -> put(name, bool);
    case Character character -> put(name, character);
    case JSONObject nObject -> object.add(name, nObject.getInternal());
    case JSONArray array -> put(name, array.getInternal());
    case Document doc -> object.add(name, doc.toJSON(false).getInternal());
    case String[] string1s -> object.add(name, new JSONArray(string1s).getInternal());
    case Object[] objects -> object.add(name, new JSONArray(objects).getInternal());
    case Iterable<?> iterable -> {
      // RETRY UP TO 10 TIMES IN CASE OF CONCURRENT UPDATE
      for (int i = 0; i < 10; i++) {
        final JSONArray array = new JSONArray();
        try {
          for (Object o : iterable)
            array.put(o);
          object.add(name, array.getInternal());
          break;
        } catch (ConcurrentModificationException e) {
          // RETRY
        }
      }
    }
    case Enum<?> enumValue -> object.addProperty(name, enumValue.name());
    case Date date -> {
      if (dateFormatAsString == null)
        // SAVE AS TIMESTAMP
        object.addProperty(name, date.getTime());
      else
        // SAVE AS STRING
        object.addProperty(name, dateFormat.format(date.toInstant().atZone(ZoneId.systemDefault())));
    }
    case LocalDate localDate -> {
      if (dateFormatAsString == null)
        // SAVE AS TIMESTAMP (resolve the offset for the target date's midnight, DST-correct)
        object.addProperty(name, localDate.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli());
      else
        // SAVE AS STRING
        object.addProperty(name, dateFormat.format(localDate.atStartOfDay()));
    }
    case TemporalAccessor temporalAccessor -> {
      if (dateFormatAsString == null)
        // SAVE AS TIMESTAMP
        object.addProperty(name,
            DateUtils.dateTimeToTimestamp(value, ChronoUnit.MILLIS));
      else if (temporalAccessor instanceof Instant instant)
        // SAVE AS STRING: an Instant has no date fields, so it must be anchored to UTC before the
        // schema-wide pattern can be applied (arcadedb.dateTimeImplementation=java.time.Instant).
        object.addProperty(name, dateTimeFormat.format(LocalDateTime.ofInstant(instant, ZoneOffset.UTC)));
      else
        // SAVE AS STRING
        object.addProperty(name, dateTimeFormat.format(temporalAccessor));
    }
    case Duration duration -> object.addProperty(name, duration.toSeconds() + (duration.toNanosPart() / 1_000_000_000.0));
    case Identifiable identifiable -> object.addProperty(name, identifiable.getIdentity().toString());
    case Map map -> object.add(name, new JSONObject(map).getInternal());
    case Class<?> clazz -> object.addProperty(name, clazz.getName());
    case Object o when o.getClass().isArray() -> object.add(name, primitiveArrayToElement(o));
    default ->
      // GENERIC CASE: TRANSFORM IT TO STRING
        object.addProperty(name, value.toString());
    }
    return this;
  }

  @Override
  public Object remove(final Object key) {
    return key == null ? null : remove(key.toString());
  }

  @Override
  public void putAll(Map<? extends String, ?> m) {
    if (m != null) {
      for (Map.Entry<? extends String, ?> entry : m.entrySet())
        put(entry.getKey(), entry.getValue());
    }
  }

  /**
   * Returns the string value of the property with the given name.
   *
   * @throws JSONException if the property is not found or is null.
   */
  public String getString(final String name) {
    final JsonElement value = getNotNullElement(name);
    try {
      return value.getAsString();
    } catch (UnsupportedOperationException | IllegalStateException e) {
      throw typeError(name, "string", value, e);
    }
  }

  /**
   * Returns the string value of the property with the given name, or the default value if the property is not found or is null.
   */
  public String getString(final String name, final String defaultValue) {
    if (isNull(name))
      return defaultValue;
    return getString(name);
  }

  /**
   * Returns the integer value of the property with the given name.
   *
   * @throws JSONException if the property is not found or is null.
   */
  public int getInt(final String name) {
    final JsonElement value = getNotNullElement(name);
    try {
      return value.getAsNumber().intValue();
    } catch (UnsupportedOperationException | IllegalStateException | NumberFormatException | ClassCastException e) {
      throw typeError(name, "int", value, e);
    }
  }

  /**
   * Returns the integer value of the property with the given name, or the default value if the property is not found or is null.
   */
  public int getInt(final String name, final int defaultValue) {
    if (isNull(name))
      return defaultValue;
    return getInt(name);
  }

  /**
   * Returns the long value of the property with the given name.
   *
   * @throws JSONException if the property is not found or is null.
   */
  public long getLong(final String name) {
    final JsonElement value = getNotNullElement(name);
    try {
      return value.getAsNumber().longValue();
    } catch (UnsupportedOperationException | IllegalStateException | NumberFormatException | ClassCastException e) {
      throw typeError(name, "long", value, e);
    }
  }

  /**
   * Returns the long value of the property with the given name, or the default value if the property is not found or is null.
   */
  public long getLong(final String name, final long defaultValue) {
    if (isNull(name))
      return defaultValue;
    return getLong(name);
  }

  /**
   * Returns the float value of the property with the given name.
   *
   * @throws JSONException if the property is not found or is null.
   */
  public float getFloat(final String name) {
    final JsonElement value = getNotNullElement(name);
    try {
      return value.getAsNumber().floatValue();
    } catch (UnsupportedOperationException | IllegalStateException | NumberFormatException | ClassCastException e) {
      throw typeError(name, "float", value, e);
    }
  }

  /**
   * Returns the float value of the property with the given name, or the default value if the property is not found or is null.
   */
  public float getFloat(final String name, final float defaultValue) {
    if (isNull(name))
      return defaultValue;
    return getFloat(name);
  }

  /**
   * Returns the double value of the property with the given name.
   *
   * @throws JSONException if the property is not found or is null.
   */
  public double getDouble(final String name) {
    final JsonElement value = getNotNullElement(name);
    try {
      return value.getAsNumber().doubleValue();
    } catch (UnsupportedOperationException | IllegalStateException | NumberFormatException | ClassCastException e) {
      throw typeError(name, "double", value, e);
    }
  }

  /**
   * Returns the double value of the property with the given name, or the default value if the property is not found or is null.
   */
  public double getDouble(final String name, final double defaultValue) {
    if (isNull(name))
      return defaultValue;
    return getDouble(name);
  }

  /**
   * Returns the boolean value of the property with the given name. A JSON boolean and the strings {@code "true"} /
   * {@code "false"} (case-insensitive) are accepted; any other value is a type mismatch.
   *
   * @throws JSONException if the property is not found, is null or is not a boolean.
   */
  public boolean getBoolean(final String name) {
    final JsonElement value = getNotNullElement(name);
    if (value.isJsonPrimitive()) {
      final JsonPrimitive primitive = value.getAsJsonPrimitive();
      if (primitive.isBoolean())
        return primitive.getAsBoolean();

      if (primitive.isString()) {
        // TOLERATE THE TEXTUAL FORM: CONFIGURATION FILES AND QUERY STRINGS CARRY "true"/"false" AS STRINGS
        final String text = primitive.getAsString();
        if ("true".equalsIgnoreCase(text))
          return true;
        if ("false".equalsIgnoreCase(text))
          return false;
      }
    }

    // GSON'S getAsBoolean() FALLS BACK TO Boolean.parseBoolean(), WHICH NEVER RAISES AND ANSWERS false FOR ANYTHING
    // THAT IS NOT "true": WITHOUT THIS EXPLICIT CHECK A TYPE MISMATCH WOULD SILENTLY DEGRADE TO false (issue #5935)
    throw typeError(name, "boolean", value, null);
  }

  /**
   * Returns the boolean value of the property with the given name, or the default value if the property is not found or is null.
   */
  public boolean getBoolean(final String name, final boolean defaultValue) {
    if (isNull(name))
      return defaultValue;
    return getBoolean(name);
  }

  /**
   * Returns the BigDecimal value of the property with the given name.
   *
   * @throws JSONException if the property is not found or is null.
   */
  public BigDecimal getBigDecimal(final String name) {
    final JsonElement value = getNotNullElement(name);
    try {
      return value.getAsBigDecimal();
    } catch (UnsupportedOperationException | IllegalStateException | NumberFormatException | ClassCastException e) {
      throw typeError(name, "BigDecimal", value, e);
    }
  }

  /**
   * Returns the BigDecimal value of the property with the given name, or the default value if the property is not found or is null.
   */
  public BigDecimal getBigDecimal(final String name, final BigDecimal defaultValue) {
    if (isNull(name))
      return defaultValue;
    return getBigDecimal(name);
  }

  /**
   * Returns the nested object of the property with the given name.
   *
   * @throws JSONException if the property is not found, is null or is not a JSON object.
   */
  public JSONObject getJSONObject(final String name) {
    final JsonElement value = getNotNullElement(name);
    if (!value.isJsonObject())
      throw typeError(name, "JSON object", value, null);
    return new JSONObject(value.getAsJsonObject());
  }

  public JSONObject getJSONObject(final String name, final JSONObject defaultValue) {
    if (isNull(name))
      return defaultValue;
    return getJSONObject(name);
  }

  /**
   * Returns the nested array of the property with the given name.
   *
   * @throws JSONException if the property is not found, is null or is not a JSON array.
   */
  public JSONArray getJSONArray(final String name) {
    final JsonElement value = getNotNullElement(name);
    if (!value.isJsonArray())
      throw typeError(name, "JSON array", value, null);
    return new JSONArray(value.getAsJsonArray());
  }

  public JSONArray getJSONArray(final String name, final JSONArray defaultValue) {
    if (isNull(name))
      return defaultValue;
    return getJSONArray(name);
  }

  public Object get(final String name) {
    return elementToObject(getElement(name));
  }

  public Object get(final String name, final Object defaultValue) {
    if (isNull(name))
      return defaultValue;
    return elementToObject(getElement(name));
  }

  public Object opt(final String name) {
    return name == null ? null : elementToObject(object.get(name));
  }

  public boolean has(final String name) {
    return object.has(name);
  }

  public Object remove(final String name) {
    final JsonElement oldElement = object.remove(name);
    if (oldElement != null)
      return elementToObject(oldElement);
    return null;
  }

  public Map<String, Object> toMap() {
    return toMap(false);
  }

  /**
   * Converts the object to a Java {@link Map}.
   *
   * @param optimizeNumericArrays when {@code true}, homogeneous numeric arrays found anywhere in
   *                              the tree are returned as primitive {@code long[]}/{@code double[]} instead of
   *                              {@code List<Number>}. This avoids per-element boxing for large vector payloads
   *                              (issue #3864 follow-up), see {@link JSONArray#toPrimitiveNumericArrayOrNull()}. Used by the HTTP command handler when
   *                              parsing {@code params}.
   */
  public Map<String, Object> toMap(final boolean optimizeNumericArrays) {
    final Map<String, JsonElement> map = object.asMap();
    final Map<String, Object> result = new LinkedHashMap<>(map.size());
    for (Map.Entry<String, JsonElement> entry : map.entrySet()) {
      Object value = elementToObject(entry.getValue());
      if (value instanceof JSONObject nObject)
        value = nObject.toMap(optimizeNumericArrays);
      else if (value instanceof JSONArray array) {
        if (optimizeNumericArrays) {
          final Object primitive = array.toPrimitiveNumericArrayOrNull();
          value = primitive != null ? primitive : array.toList(true);
        } else
          value = array.toList();
      }

      result.put(entry.getKey(), value);
    }

    return result;
  }

  public JSONArray names() {
    return new JSONArray(object.keySet());
  }

  public Set<String> keySet() {
    return object.keySet();
  }

  @Override
  public Collection<Object> values() {
    final List<Object> values = new ArrayList<>(object.size());
    for (String key : object.keySet())
      values.add(elementToObject(object.get(key)));
    return values;
  }

  @Override
  public Set<Entry<String, Object>> entrySet() {
    final Set<Entry<String, Object>> entrySet = new LinkedHashSet<>();
    for (String key : object.keySet()) {
      final JsonElement value = object.get(key);
      entrySet.add(new AbstractMap.SimpleEntry<>(key, elementToObject(value)));
    }
    return entrySet;
  }

  public int length() {
    return keySet().size();
  }

  public JsonElement getInternal() {
    return object;
  }

  public String toString(final int indent) {
    return JSONFactory.INSTANCE.getGsonPrettyPrint().toJson(object);
  }

  @Override
  public String toString() {
    return JSONFactory.INSTANCE.getGson().toJson(object);
  }

  @Override
  public int size() {
    return length();
  }

  public boolean isEmpty() {
    return object.size() == 0;
  }

  @Override
  public boolean containsKey(final Object key) {
    return key == null ? false : has(key.toString());
  }

  @Override
  public boolean containsValue(final Object value) {
    for (String key : object.keySet()) {
      Object val = elementToObject(object.get(key));
      if (Objects.equals(val, value))
        return true;
    }
    return false;
  }

  @Override
  public Object get(Object key) {
    return key != null ? opt(key.toString()) : null;
  }

  public void clear() {
    object.asMap().clear();
  }

  public boolean isNull(final String name) {
    return !object.has(name) || object.get(name).isJsonNull();
  }

  public void write(final FileWriter writer) throws IOException {
    writer.write(toString(0));
  }

  /**
   * Sets the format for dates. Null means using the timestamp, otherwise it follows the syntax of Java SimpleDateFormat.
   *
   * @return
   */
  public JSONObject setDateFormat(final String dateFormat) {
    this.dateFormatAsString = dateFormat;
    // Null is the documented timestamp mode (see the javadoc above and the `dateFormatAsString == null` branches in
    // put()), so it must not reach a formatter factory: both DateTimeFormatter.ofPattern(null) and
    // DateUtils.getFormatter(null) throw NullPointerException, which the IllegalArgumentException catch below never
    // covered. The setter documented a mode it could not actually be put into.
    if (dateFormat == null) {
      this.dateFormat = null;
      return this;
    }
    try {
      // DateUtils.getFormatter(), not DateTimeFormatter.ofPattern(): these formats come from the schema, and JSON is a
      // wire and storage format, so a textual field must not follow the JVM default locale (issue #7144)
      this.dateFormat = DateUtils.getFormatter(dateFormat);
    } catch (IllegalArgumentException e) {
      throw new JSONException("Invalid date format: " + dateFormat, e);
    }
    return this;
  }

  public JSONObject setDateTimeFormat(final String dateFormat) {
    this.dateTimeFormatAsString = dateFormat;
    if (dateFormat == null) {
      this.dateTimeFormat = null;
      return this;
    }
    try {
      this.dateTimeFormat = DateUtils.getFormatter(dateFormat);
    } catch (IllegalArgumentException e) {
      throw new JSONException("Invalid date format: " + dateFormat, e);
    }
    return this;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (!(o instanceof JSONObject))
      return false;
    final JSONObject that = (JSONObject) o;
    return object.equals(that.object);
  }

  @Override
  public int hashCode() {
    return Objects.hash(object);
  }

  protected static Object elementToObject(final JsonElement element) {
    if (element == null || element == NULL)
      return null;
    else if (element.isJsonPrimitive()) {
      // DETERMINE FROM THE PRIMITIVE
      final JsonPrimitive primitive = element.getAsJsonPrimitive();
      if (primitive.isNumber()) {
        final Number value = primitive.getAsNumber();
        if (!(value instanceof LazilyParsedNumber))
          return value;
        final String strValue = primitive.getAsString();

        // Efficient check to determine the appropriate type
        if (strValue.contains(".") || strValue.contains("e") || strValue.contains("E")) {
          // Contains decimal point or scientific notation: a double, unless it carries more digits than a double holds
          final double doubleVal = primitive.getAsDouble();
          if (mayLoseDigits(strValue, doubleVal) && !isExactDouble(strValue, doubleVal) && isSafeBigNumber(strValue))
            return new BigDecimal(strValue);
          return doubleVal;
        } else {
          // Check if it fits in an Integer or a Long. LazilyParsedNumber.longValue() would silently keep the low 64 bits of a
          // bigger number, so the value is parsed from its text (issue #9004)
          try {
            final long longVal = Long.parseLong(strValue);
            if (longVal >= Integer.MIN_VALUE && longVal <= Integer.MAX_VALUE)
              return (int) longVal;
            return longVal;

          } catch (NumberFormatException e) {
            // beyond the long range: keep every digit, unless the token is one only a hostile payload writes
            return isSafeBigNumber(strValue) ? new BigDecimal(strValue) : (Object) primitive.getAsDouble();
          }
        }
      } else if (primitive.isString())
        return primitive.getAsString();
      else if (primitive.isBoolean())
        return primitive.getAsBoolean();

    } else if (element.isJsonObject())
      return new JSONObject(element.getAsJsonObject());
    else if (element.isJsonArray())
      return new JSONArray(element.getAsJsonArray());

    throw new IllegalArgumentException("Element " + element + " not supported");
  }

  private static final int MAX_BIG_NUMBER_TOKEN_LENGTH = 1000;
  private static final int MAX_BIG_NUMBER_EXPONENT     = 1000;

  /**
   * Whether a token that no long or double holds may become a {@link BigDecimal}. A {@code 1e999999999} token is a few bytes that
   * a later {@code toBigInteger()}, {@code toPlainString()} or integer conversion would expand into gigabytes, and a token of
   * thousands of digits costs superlinear time to parse: beyond these limits the number stays what it always was, a double.
   */
  static boolean isSafeBigNumber(final String token) {
    if (token.length() > MAX_BIG_NUMBER_TOKEN_LENGTH)
      return false;
    for (int i = 0; i < token.length(); i++) {
      final char c = token.charAt(i);
      if (c == 'e' || c == 'E') {
        long exponent = 0;
        for (int j = i + 1; j < token.length(); j++) {
          final char d = token.charAt(j);
          if (d >= '0' && d <= '9') {
            exponent = exponent * 10 + (d - '0');
            // STOP BEFORE A LONG EXPONENT WRAPS AROUND TO A SMALL ONE (1e18446744073709551617) AND REACHES new BigDecimal(), WHICH REFUSES IT
            if (exponent > MAX_BIG_NUMBER_EXPONENT)
              return false;
          }
        }
        return true;
      }
    }
    return true;
  }

  /** A token a double may not hold exactly: more digits than a double keeps, or a value that overflowed or underflowed. */
  static boolean mayLoseDigits(final String token, final double value) {
    if (!Double.isFinite(value))
      return true;
    final boolean tiny = Math.abs(value) < Double.MIN_NORMAL;
    if (!tiny && token.length() <= 15)
      return false;
    // THE SIGN, THE POINT AND THE EXPONENT MAKE A TOKEN LONG WITHOUT MAKING IT HOLD MORE THAN 15 DIGITS, WHICH A double ALWAYS HOLDS
    int digits = 0;
    boolean nonZero = false;
    for (int i = 0, n = token.length(); i < n; i++) {
      final char c = token.charAt(i);
      if (c == 'e' || c == 'E')
        break;
      if (c >= '0' && c <= '9') {
        digits++;
        if (c != '0')
          nonZero = true;
      }
    }
    // A DIGIT-LESS ZERO IS EXACT; A NON-ZERO MANTISSA THAT BECAME (SUB)NORMAL-LESS HAS UNDERFLOWED
    return tiny ? nonZero : digits > 15;
  }

  /**
   * Whether a decimal token holds nothing the double parsed from it loses: it has the same significant digits (sign, decimal point,
   * exponent and the zeros around the digits ignored) as {@link Double#toString(double)}, the shortest rendering that parses back to
   * the double (guaranteed from JDK 19; an older JDK may render a longer string, which only sends more tokens to {@link BigDecimal}).
   * Compared in place, so the common long token of an embedding (a double written with 17 digits) costs no extra object
   * besides that string, and no {@link BigDecimal}.
   */
  static boolean isExactDouble(final String token, final double value) {
    if (!Double.isFinite(value))
      return false;
    final String shortest = Double.toString(value);
    final int tokenEnd = endOfDigits(token);
    final int shortestEnd = endOfDigits(shortest);
    int i = 0;
    int j = 0;
    boolean started = false;
    while (true) {
      i = nextDigit(token, i, tokenEnd, started);
      j = nextDigit(shortest, j, shortestEnd, started);
      if (i < 0 && j < 0)
        return true;
      // ONE SIDE ENDED: THE OTHER MAY ONLY HAVE TRAILING ZEROS LEFT
      if (i < 0)
        return onlyZerosFrom(shortest, j, shortestEnd);
      if (j < 0)
        return onlyZerosFrom(token, i, tokenEnd);
      if (token.charAt(i) != shortest.charAt(j))
        return false;
      started = true;
      i++;
      j++;
    }
  }

  private static int endOfDigits(final String number) {
    for (int i = 0; i < number.length(); i++) {
      final char c = number.charAt(i);
      if (c == 'e' || c == 'E')
        return i;
    }
    return number.length();
  }

  /** Index of the next digit at or after {@code from}, leading zeros skipped until the first significant digit, or -1. */
  private static int nextDigit(final String number, final int from, final int end, final boolean started) {
    for (int i = from; i < end; i++) {
      final char c = number.charAt(i);
      if (c >= '0' && c <= '9' && (started || c != '0'))
        return i;
    }
    return -1;
  }

  private static boolean onlyZerosFrom(final String number, final int from, final int end) {
    for (int i = from; i < end; i++) {
      final char c = number.charAt(i);
      if (c >= '1' && c <= '9')
        return false;
    }
    return true;
  }

  /**
   * JSON has no literal for NaN and the infinities: every writer in this class turns them into {@code null}, never into a number
   * (a {@code 0} would be indistinguishable from a measurement). The integral types and the big numbers are finite by construction, so
   * a huge {@link BigDecimal} is not mistaken for an infinity. Any other {@link Number} is asked for its double value, so a
   * lazily parsed token outside the double range is treated as non-finite as well.
   */
  static boolean isNonFinite(final Number number) {
    if (number instanceof Double || number instanceof Float)
      return !Double.isFinite(number.doubleValue());
    if (number == null || number instanceof Integer || number instanceof Long || number instanceof BigDecimal || number instanceof BigInteger
        || number instanceof Short || number instanceof Byte)
      return false;
    // any other Number (e.g. a lazily parsed "NaN" token): ask for its double value
    return !Double.isFinite(number.doubleValue());
  }

  // PRIMITIVE ARRAYS (float[], double[], int[], long[], short[], byte[], ...): serialized element-by-element via reflection instead of
  // falling through to the generic toString() (which would emit "[F@..."), wherever the array sits: a property, a map value or a list element.
  private static JsonElement primitiveArrayToElement(final Object array) {
    // TYPED LOOPS FOR THE COMMON CASES (EMBEDDINGS, BINARY): NO REFLECTION AND NO TYPE SWITCH PER ELEMENT
    final JsonArray result;
    switch (array) {
    case float[] floats -> {
      result = new JsonArray(floats.length);
      for (final float f : floats)
        result.add(Float.isFinite(f) ? new JsonPrimitive(f) : JsonNull.INSTANCE);
    }
    case double[] doubles -> {
      result = new JsonArray(doubles.length);
      for (final double d : doubles)
        result.add(Double.isFinite(d) ? new JsonPrimitive(d) : JsonNull.INSTANCE);
    }
    case int[] ints -> {
      result = new JsonArray(ints.length);
      for (final int i : ints)
        result.add(i);
    }
    case long[] longs -> {
      result = new JsonArray(longs.length);
      for (final long l : longs)
        result.add(l);
    }
    case byte[] bytes -> {
      result = new JsonArray(bytes.length);
      for (final byte b : bytes)
        result.add(b);
    }
    default -> {
      final int length = Array.getLength(array);
      result = new JsonArray(length);
      for (int i = 0; i < length; i++)
        result.add(objectToElement(Array.get(array, i)));
    }
    }
    return result;
  }

  protected static JsonElement objectToElement(final Object object) {
    return switch (object) {
      case null -> JsonNull.INSTANCE;
      case JsonElement jsonElement -> jsonElement;
      case String string -> new JsonPrimitive(string);
      case Number number -> isNonFinite(number) ? JsonNull.INSTANCE : new JsonPrimitive(number);
      case Boolean boolean1 -> new JsonPrimitive(boolean1);
      case Character character -> new JsonPrimitive(character);
      case JSONObject nObject -> nObject.getInternal();
      case JSONArray array -> array.getInternal();
      case Collection collection -> new JSONArray(collection).getInternal();
      case Object[] objects -> new JSONArray(objects).getInternal();
      case Map map -> new JSONObject(map).getInternal();
      case Document document -> document.toJSON(false).getInternal();
      case Identifiable identifiable -> new JsonPrimitive(identifiable.getIdentity().toString());
      case Enum<?> enumValue -> new JsonPrimitive(enumValue.name());
      case Object o when o.getClass().isArray() -> primitiveArrayToElement(o);
      case Date date -> new JsonPrimitive(date.getTime());
      case LocalDate localDate -> new JsonPrimitive(localDate.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli());
      case TemporalAccessor temporalAccessor -> {
        final Long timestamp = DateUtils.dateTimeToTimestamp(temporalAccessor, ChronoUnit.MILLIS);
        yield timestamp != null ? new JsonPrimitive(timestamp) : new JsonPrimitive(temporalAccessor.toString());
      }
      case Duration duration -> new JsonPrimitive(duration.toSeconds() + (duration.toNanosPart() / 1_000_000_000.0));
      case Class<?> clazz -> new JsonPrimitive(clazz.getName());
      default -> new JsonPrimitive(object.toString());
    };
  }

  private JsonElement getElement(final String name) {
    if (name == null)
      throw new JSONException("Null key");

    final JsonElement value = object.get(name);
    if (value == null)
      throw new JSONException("JSONObject[" + truncate(name) + "] not found");

    return value;
  }

  /**
   * Returns the element bound to the given name, guaranteed to be neither absent nor JSON null.
   * <p>
   * GSON models an explicit JSON null as a {@link JsonNull} entry in the underlying map, so the "not found" check in
   * {@link #getElement(String)} does not catch it: the getters documented to raise must screen it here, otherwise the
   * conversion leaks GSON's {@code UnsupportedOperationException} instead of the documented {@link JSONException}
   * (issue #5935).
   */
  private JsonElement getNotNullElement(final String name) {
    final JsonElement value = getElement(name);
    if (value.isJsonNull())
      throw new JSONException("JSONObject[" + truncate(name) + "] is null");

    return value;
  }

  /**
   * Wraps a failed conversion into the {@link JSONException} declared by the getters. GSON signals a type mismatch with
   * {@code UnsupportedOperationException} / {@code IllegalStateException}, and a non-numeric string reaches
   * {@code NumberFormatException} only when the lazily parsed number is materialized.
   */
  private static JSONException typeError(final String name, final String expectedType, final JsonElement value,
      final RuntimeException cause) {
    return new JSONException("JSONObject[" + truncate(name) + "] is not a " + expectedType + " (" + describe(value) + ")", cause);
  }

  /**
   * Renders an element for an error message. Containers are reported by kind and a long primitive is truncated, so a
   * multi-megabyte payload cannot be echoed back through an exception message.
   */
  static String describe(final JsonElement value) {
    if (value.isJsonObject())
      return "JSON object";
    if (value.isJsonArray())
      return "JSON array";

    return truncate(value.toString());
  }

  /**
   * Caps a client-supplied fragment quoted in an error message. Property names travel in the request payload just like
   * values do, so they are bounded on the same terms.
   */
  static String truncate(final String text) {
    if (text == null || text.length() <= MAX_ERROR_VALUE_LENGTH)
      return text;
    return text.substring(0, MAX_ERROR_VALUE_LENGTH) + "...";
  }

  /**
   * Checks recursively and replaces NaN and infinite values with null.
   */
  public void validate() {
    for (String key : keySet()) {
      Object value = get(key);
      if (value instanceof Number number) {
        if (isNonFinite(number))
          // FIX NAN NUMBERS
          put(key, (Number) null);
      } else if (value instanceof JSONObject nObject) {
        nObject.validate();
      } else if (value instanceof JSONArray array) {
        for (int i = 0; i < array.length(); i++) {
          final Object arrayValue = array.get(i);
          if (arrayValue instanceof Number number) {
            if (isNonFinite(number))
              // FIX NAN NUMBERS
              array.put(i, null);
          } else if (arrayValue instanceof JSONObject nObject) {
            nObject.validate();
          }
        }
      }
    }
  }

  public Object getExpression(final String expression) {
    if (expression == null || expression.isEmpty())
      return null;

    String[] tokens = expression.split("(?=\\[)|(?<=\\])|\\.");
    Object current = this;

    for (String token : tokens) {
      if (token.isEmpty())
        continue;

      if (token.startsWith("[")) {
        // Array or map access
        String key = token.substring(1, token.length() - 1);
        if (current instanceof JSONArray array) {
          try {
            int idx = Integer.parseInt(key);
            current = idx >= 0 && idx < array.length() ? array.get(idx) : null;
          } catch (NumberFormatException e) {
            return null;
          }
        } else if (current instanceof JSONObject obj) {
          current = obj.opt(key);
        } else {
          return null;
        }
      } else {
        // Dot notation
        if (current instanceof JSONObject obj) {
          if (!obj.has(token))
            return null;
          current = obj.opt(token);
        } else {
          return null;
        }
      }
      if (current == null)
        return null;
    }
    return current;
  }
}
