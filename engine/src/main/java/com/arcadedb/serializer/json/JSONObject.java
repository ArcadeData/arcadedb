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

import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

import java.io.*;
import java.math.*;
import java.util.*;

/**
 * JSON object.<br>
 * This API is compatible with org.json Java API, but uses Google GSON library under the hood. The main reason why we created this wrapper is
 * because the maintainer of the project org.json are not open to support ordered attributes as an option.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class JSONObject {
  public static final JsonNull   NULL = JsonNull.INSTANCE;
  private final       JsonObject object;

  public JSONObject() {
    this.object = new JsonObject();
  }

  public JSONObject(final JsonObject input) {
    this.object = input;
  }

  public JSONObject(final String input) {
    object = (JsonObject) JsonParser.parseString(input);
  }

  public JSONObject(final Map<String, ? extends Object> map) {
    object = new JsonObject();
    for (Map.Entry<String, ? extends Object> entry : map.entrySet())
      put(entry.getKey(), entry.getValue());
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
    if (value == null || value instanceof JsonNull)
      object.add(name, NULL);
    else if (value instanceof String)
      object.addProperty(name, (String) value);
    else if (value instanceof Number)
      object.addProperty(name, (Number) value);
    else if (value instanceof Boolean)
      object.addProperty(name, (Boolean) value);
    else if (value instanceof Character)
      object.addProperty(name, (Character) value);
    else if (value instanceof JSONObject)
      object.add(name, ((JSONObject) value).getInternal());
    else if (value instanceof String[])
      object.add(name, new JSONArray((String[]) value).getInternal());
    else if (value instanceof Iterable) {
      final JSONArray array = new JSONArray();
      for (Object o : (Iterable) value)
        array.put(o);
      object.add(name, array.getInternal());
    } else if (value instanceof Enum) {
      object.addProperty(name, ((Enum) value).name());
    } else
      throw new JSONException("Type '" + value.getClass() + "' not supported for JSONObject");
    return this;
  }

  public String getString(final String name) {
    return getElement(name).getAsString();
  }

  public Integer getInt(final String name) {
    return getElement(name).getAsNumber().intValue();
  }

  public Long getLong(final String name) {
    return getElement(name).getAsNumber().longValue();
  }

  public float getFloat(final String name) {
    return getElement(name).getAsNumber().floatValue();
  }

  public double getDouble(final String name) {
    return getElement(name).getAsNumber().doubleValue();
  }

  public Boolean getBoolean(final String name) {
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
    final Object object = this.opt(name);
    return NULL.equals(object) ? defaultValue : object.toString();
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
    for (Map.Entry<String, JsonElement> entry : map.entrySet())
      result.put(entry.getKey(), elementToObject(entry.getValue()));

    return result;
  }

  public JSONArray names() {
    return new JSONArray(object.keySet());
  }

  public Set<String> keySet() {
    return object.keySet();
  }

  public int length() {
    return keySet().size();
  }

  public JsonElement getInternal() {
    return object;
  }

  public String toString(final int indent) {
    return new GsonBuilder().serializeNulls().setPrettyPrinting().create().toJson(object);
  }

  @Override
  public String toString() {
    return new GsonBuilder().serializeNulls().create().toJson(object);
  }

  public boolean isEmpty() {
    return object.size() == 0;
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
      final JsonPrimitive primitive = element.getAsJsonPrimitive();
      if (primitive.isString())
        return primitive.getAsString();
      else if (primitive.isNumber()) {
        final String value = primitive.getAsNumber().toString();
        try {
          return Integer.parseInt(value);
        } catch (NumberFormatException e) {
          try {
            return Long.parseLong(value);
          } catch (NumberFormatException e2) {
            return Double.parseDouble(value);
          }
        }
      } else if (primitive.isBoolean())
        return primitive.getAsString();
    } else if (element.isJsonObject())
      return new JSONObject(element.getAsJsonObject());
    else if (element.isJsonArray())
      return new JSONArray(element.getAsJsonArray());

    throw new IllegalArgumentException("Element " + element + " not supported");
  }

  protected static JsonElement objectToElement(final Object object) {
    if (object == null)
      return JsonNull.INSTANCE;
    else if (object instanceof String)
      return new JsonPrimitive((String) object);
    else if (object instanceof Number)
      return new JsonPrimitive((Number) object);
    else if (object instanceof Boolean)
      return new JsonPrimitive((Boolean) object);
    else if (object instanceof Character)
      return new JsonPrimitive((Character) object);
    else if (object instanceof JSONObject)
      return ((JSONObject) object).getInternal();
    else if (object instanceof JSONArray)
      return ((JSONArray) object).getInternal();
    else if (object instanceof Map)
      return new JSONObject((Map) object).getInternal();

    throw new IllegalArgumentException("Object of type " + object.getClass() + " not supported");
  }

  private JsonElement getElement(final String name) {
    if (name == null)
      throw new JSONException("Null key");

    final JsonElement value = object.get(name);
    if (object == null)
      throw new JSONException("JSONObject[" + name + "] not found");

    return value;
  }
}