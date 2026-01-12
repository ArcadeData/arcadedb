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
package com.arcadedb.query.sql.executor;

import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.DateUtils;
import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;

import java.lang.reflect.*;
import java.text.*;
import java.time.*;
import java.util.*;

/**
 * Created by luigidellaquila on 21/07/16.
 */
@ExcludeFromJacocoGeneratedReport
public interface Result {

  /**
   * Returns the value for the property.
   *
   * @param name the property name
   *
   * @return the property value. If the property value is a persistent record, it only returns the RID. See also  {@link
   * #getElementProperty(String)}
   */
  <T> T getProperty(String name);

  /**
   * Returns the value for the property. If the property does not exist, then the `defaultValue` is returned.
   *
   * @param name         the property name
   * @param defaultValue default value to return in case the property is missing
   *
   * @return the property value. If the property value is a persistent record, it only returns the RID. See also  {@link
   * #getElementProperty(String)}
   */
  <T> T getProperty(String name, Object defaultValue);

  /**
   * returns an OElement property from the result
   *
   * @param name the property name
   *
   * @return the property value. Null if the property is not defined or if it's not an OElement
   */
  Record getElementProperty(String name);

  Set<String> getPropertyNames();

  Optional<RID> getIdentity();

  boolean isElement();

  Optional<Document> getElement();

  Document toElement();

  Optional<Record> getRecord();

  default boolean isRecord() {
    return !isProjection();
  }

  boolean isProjection();

  /**
   * return metadata related to current result given a key
   *
   * @param key the metadata key
   *
   * @return metadata related to current result given a key
   */
  Object getMetadata(String key);

  /**
   * return all the metadata keys available
   *
   * @return all the metadata keys available
   */
  Set<String> getMetadataKeys();

  default JSONObject toJSON() {
    if (isElement())
      return getElement().get().toJSON();

    final JSONObject result = new JSONObject();
    for (final String prop : getPropertyNames())
      result.put(prop, valueToJSON(getProperty(prop)));

    return result;
  }

  default Object valueToJSON(final Object val) {
    if (val != null) {
      if (val instanceof Result result) {
        return result.toJSON();
      } else if (val instanceof Record record) {
        return record.getIdentity() != null ? record.getIdentity().toString() : null;
      } else if (val instanceof Iterable<?> iterable) {
        final JSONArray array = new JSONArray();
        for (final Object o : iterable)
          array.put(valueToJSON(o));
        return array;
      } else if (val instanceof Iterator<?> iterator) {
        final JSONArray array = new JSONArray();
        while (iterator.hasNext())
          array.put(valueToJSON(iterator.next()));
        return array;
      } else if (val instanceof Map) {
        return new JSONObject((Map<String, Object>) val);
      } else if (val instanceof byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
      } else if (val.getClass().isArray()) {
        final JSONArray array = new JSONArray();
        final int length = Array.getLength(val);
        for (int i = 0; i < length; i++)
          array.put(Array.get(val, i));
      } else if (val instanceof Date) {
        final Database database = getDatabase();
        if (database != null)
          return DateUtils.format(val, database.getSchema().getDateTimeFormat());
        else
          return new SimpleDateFormat().format(val);

      } else if (val instanceof LocalDateTime) {
        final Database database = getDatabase();
        if (database != null)
          return DateUtils.format(val, database.getSchema().getDateTimeFormat());
      } else if (val instanceof Type type)
        return type.name();
    }

    return val;
  }

  Database getDatabase();

  default String encode(final String s) {
    String result = s.replace("\"", "\\\\\"");
    result = result.replace("\n", "\\\\n");
    result = result.replace("\t", "\\\\t");
    return result;
  }

  default boolean isEdge() {
    return getElement().map(x -> x instanceof Edge).orElse(false);
  }

  default boolean isVertex() {
    return getElement().map(x -> x instanceof Vertex).orElse(false);
  }

  default Optional<Vertex> getVertex() {
    if (isVertex()) {
      return Optional.ofNullable((Vertex) getElement().get());
    }
    return Optional.empty();
  }

  default Optional<Edge> getEdge() {
    if (isEdge()) {
      return Optional.ofNullable((Edge) getElement().get());
    }
    return Optional.empty();
  }

  boolean hasProperty(String varName);

  Map<String, Object> toMap();
}
