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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

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

  public JSONArray(final Collection<? extends Object> input) {
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
    final List<JsonElement> list = array.asList();
    final List<Object> result = new ArrayList<>(list.size());
    for (JsonElement e : array.asList()) {
      Object value = JSONObject.elementToObject(e);

      if (value instanceof JSONObject)
        value = ((JSONObject) value).toMap();
      else if (value instanceof JSONArray)
        value = ((JSONArray) value).toList();

      result.add(value);
    }

    return result;
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

  public JSONArray put(final Number object) {
    array.add(object);
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
    final Gson gson = new GsonBuilder().create();
    return gson.toJson(array);
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
