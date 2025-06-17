/*
 * Copyright 2023 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.arcadedb.serializer.json;

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.utility.DateUtils;
import com.google.gson.*;
import com.google.gson.internal.LazilyParsedNumber;
import com.google.gson.stream.JsonReader;

import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
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
    public static final JsonNull NULL = JsonNull.INSTANCE;
    private final JsonObject object;
    private String dateFormatAsString = null;
    private DateTimeFormatter dateFormat = null;
    private String dateTimeFormatAsString;
    private DateTimeFormatter dateTimeFormat;

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
        object.addProperty(name, value);
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

        // GENERIC CASE: TRANSFORM IT TO STRING
        if (value == null) {
            object.add(name, NULL);
        } else if (value instanceof JsonElement jsonElement) {
            object.add(name, jsonElement);
        } else if (value instanceof JsonNull) {
            object.add(name, NULL);
        } else if (value instanceof String string) {
            object.addProperty(name, string);
        } else if (value instanceof Number number) {
            object.addProperty(name, number);
        } else if (value instanceof Boolean bool) {
            object.addProperty(name, bool);
        } else if (value instanceof Character character) {
            object.addProperty(name, character);
        } else if (value instanceof JSONObject nObject) {
            object.add(name, nObject.getInternal());
        }else if (value instanceof JSONArray jsonArray) {
            object.add(name, jsonArray.getInternal());
        }else if (value instanceof Document doc) {
            object.add(name, doc.toJSON(false).getInternal());
        } else if (value instanceof String[] string1s) {
            object.add(name, new JSONArray(string1s).getInternal());
        } else if (value instanceof Iterable<?> iterable) {// RETRY UP TO 10 TIMES IN CASE OF CONCURRENT UPDATE
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
        } else if (value instanceof Enum<?> enumValue) {
            object.addProperty(name, enumValue.name());
        } else if (value instanceof Date date) {
            if (dateFormatAsString == null)
                // SAVE AS TIMESTAMP
                object.addProperty(name, date.getTime());
            else
                // SAVE AS STRING
                object.addProperty(name, dateFormat.format(date.toInstant().atZone(ZoneId.systemDefault())));
        } else if (value instanceof LocalDate localDate) {
            if (dateFormatAsString == null)
                // SAVE AS TIMESTAMP
                object.addProperty(name,
                        (localDate.atStartOfDay().toInstant(ZoneId.systemDefault().getRules().getOffset(Instant.now()))
                                .toEpochMilli()));
            else
                // SAVE AS STRING
                object.addProperty(name, dateFormat.format(localDate.atStartOfDay()));
        } else if (value instanceof TemporalAccessor temporalAccessor) {
            if (dateFormatAsString == null)
                // SAVE AS TIMESTAMP
                object.addProperty(name,
                        DateUtils.dateTimeToTimestamp(value, ChronoUnit.NANOS)); // ALWAYS USE NANOS TO AVOID PRECISION LOSS
            else
                // SAVE AS STRING
                object.addProperty(name, dateTimeFormat.format(temporalAccessor));
        } else if (value instanceof Duration duration) {
            object.addProperty(name,
                    Double.valueOf("%d.%d".formatted(duration.toSeconds(), duration.toNanosPart())));
        } else if (value instanceof Identifiable identifiable) {
            object.addProperty(name, identifiable.getIdentity().toString());
        } else if (value instanceof Map) {
            final JSONObject embedded = new JSONObject((Map<String, Object>) value);
            object.add(name, embedded.getInternal());
        } else if (value instanceof Class<?> clazz) {
            object.addProperty(name, clazz.getName());
        } else {
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

    public String getString(final String name) {
        return getElement(name).getAsString();
    }

    public int getInt(final String name) {
        return getElement(name).getAsNumber().intValue();
    }

    public long getLong(final String name) {
        return getElement(name).getAsNumber().longValue();
    }

    public float getFloat(final String name) {
        return getElement(name).getAsNumber().floatValue();
    }

    public double getDouble(final String name) {
        return getElement(name).getAsNumber().doubleValue();
    }

    public boolean getBoolean(final String name) {
        return getElement(name).getAsBoolean();
    }

    public BigDecimal getBigDecimal(final String name) {
        return getElement(name).getAsBigDecimal();
    }

    public JSONObject getJSONObject(final String name) {
        return new JSONObject(getElement(name).getAsJsonObject());
    }

    public JSONArray getJSONArray(final String name) {
        return new JSONArray(getElement(name).getAsJsonArray());
    }

    public Object get(final String name) {
        return elementToObject(getElement(name));
    }

    public String optString(final String name) {
        return optString(name, "");
    }

    public String optString(final String name, final String defaultValue) {
        final Object value = this.opt(name);
        return value == null || NULL.equals(value) ? defaultValue : value.toString();
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
        final Map<String, JsonElement> map = object.asMap();
        final Map<String, Object> result = new LinkedHashMap<>(map.size());
        for (Map.Entry<String, JsonElement> entry : map.entrySet()) {
            Object value = elementToObject(entry.getValue());
            if (value instanceof JSONObject nObject)
                value = nObject.toMap();
            else if (value instanceof JSONArray array)
                value = array.toList();

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
        try {
            this.dateFormat = DateTimeFormatter.ofPattern(dateFormat);
        } catch (IllegalArgumentException e) {
            throw new JSONException("Invalid date format: " + dateFormat, e);
        }
        return this;
    }

    public JSONObject setDateTimeFormat(final String dateFormat) {
        this.dateTimeFormatAsString = dateFormat;
        try {
            this.dateTimeFormat = DateTimeFormatter.ofPattern(dateFormat);
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
                    // Contains decimal point or scientific notation - definitely a double
                    return primitive.getAsDouble();
                } else {

                    // Check if it fits in an Integer
                    try {
                        final long longVal = primitive.getAsLong();
                        if (longVal >= Integer.MIN_VALUE && longVal <= Integer.MAX_VALUE)
                            return (int) longVal;
                        return longVal;

                    } catch (NumberFormatException e) {
                        // It could be a very large number, use double as fallback
                        return primitive.getAsDouble();
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

    protected static JsonElement objectToElement(final Object object) {
        if (object == null) {
            return JsonNull.INSTANCE;
        } else if (object instanceof JsonElement jsonElement) {
            return jsonElement;
        } else if (object instanceof String string) {
            return new JsonPrimitive(string);
        } else if (object instanceof Number number) {
            return new JsonPrimitive(number);
        } else if (object instanceof Boolean boolean1) {
            return new JsonPrimitive(boolean1);
        } else if (object instanceof Character character) {
            return new JsonPrimitive(character);
        } else if (object instanceof JSONObject nObject) {
            return nObject.getInternal();
        } else if (object instanceof JSONArray array) {
            return array.getInternal();
        } else if (object instanceof Collection collection) {
            return new JSONArray(collection).getInternal();
        } else if (object instanceof Map map) {
            return new JSONObject(map).getInternal();
        } else if (object instanceof Document document) {
            return document.toJSON().getInternal();
        } else if (object instanceof Identifiable identifiable) {
            return new JsonPrimitive(identifiable.getIdentity().toString());
        } else {
            throw new IllegalArgumentException("Object of type " + object.getClass() + " not supported");
        }

    }

    private JsonElement getElement(final String name) {
        if (name == null)
            throw new JSONException("Null key");

        final JsonElement value = object.get(name);
        if (value == null)
            throw new JSONException("JSONObject[" + name + "] not found");

        return value;
    }

    /**
     * Checks recursively and replace NaN values with zero.
     */
    public void validate() {
        for (String key : keySet()) {
            Object value = get(key);
            if (value instanceof Number number) {
                if (Double.isNaN(number.doubleValue()))
                    // FIX NAN NUMBERS
                    put(key, 0);
            } else if (value instanceof JSONObject nObject) {
                nObject.validate();
            } else if (value instanceof JSONArray array) {
                for (int i = 0; i < array.length(); i++) {
                    final Object arrayValue = array.get(i);
                    if (arrayValue instanceof Number number) {
                        if (Double.isNaN(number.doubleValue()))
                            // FIX NAN NUMBERS
                            array.put(i, 0);
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
