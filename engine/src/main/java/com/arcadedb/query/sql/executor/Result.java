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

import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.Type;

import java.text.*;
import java.util.*;

/**
 * Created by luigidellaquila on 21/07/16.
 */
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
  <T> T getProperty(String name, T defaultValue);

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

  default String toJSON() {
    if (isElement())
      return getElement().get().toJSON().toString();

    final StringBuilder result = new StringBuilder();
    result.append("{");
    boolean first = true;
    for (final String prop : getPropertyNames()) {
      if (!first) {
        result.append(", ");
      }
      result.append(toJson(prop));
      result.append(": ");
      result.append(toJson(getProperty(prop)));
      first = false;
    }
    result.append("}");
    return result.toString();
  }

  default String toJson(final Object val) {
    String jsonVal = null;
    if (val == null) {
      jsonVal = "null";
    } else if (val instanceof String) {
      jsonVal = "\"" + encode(val.toString()) + "\"";
    } else if (val instanceof Number || val instanceof Boolean) {
      jsonVal = val.toString();
    } else if (val instanceof Result) {
      jsonVal = ((Result) val).toJSON();
    } else if (val instanceof Record) {
      final RID id = ((Record) val).getIdentity();

      jsonVal = "\"" + id + "\"";
    } else if (val instanceof RID) {
      jsonVal = "\"" + val + "\"";
    } else if (val instanceof Iterable) {
      final StringBuilder builder = new StringBuilder();
      builder.append("[");
      boolean first = true;
      for (final Object o : (Iterable) val) {
        if (!first) {
          builder.append(", ");
        }
        builder.append(toJson(o));
        first = false;
      }
      builder.append("]");
      jsonVal = builder.toString();
    } else if (val instanceof Iterator) {
      final StringBuilder builder = new StringBuilder();
      builder.append("[");
      boolean first = true;
      final Iterator iterator = (Iterator) val;
      while (iterator.hasNext()) {
        if (!first) {
          builder.append(", ");
        }
        builder.append(toJson(iterator.next()));
        first = false;
      }
      builder.append("]");
      jsonVal = builder.toString();
    } else if (val instanceof Map) {
      final StringBuilder builder = new StringBuilder();
      builder.append("{");
      boolean first = true;
      final Map<String, Object> map = (Map) val;
      for (final Map.Entry entry : map.entrySet()) {
        if (!first) {
          builder.append(", ");
        }
        builder.append(toJson(entry.getKey()));
        builder.append(": ");
        builder.append(toJson(entry.getValue()));
        first = false;
      }
      builder.append("}");
      jsonVal = builder.toString();
    } else if (val instanceof byte[]) {
      jsonVal = "\"" + Base64.getEncoder().encodeToString((byte[]) val) + "\"";
    } else if (val instanceof Date) {
      new SimpleDateFormat().format(val);//TODO
//      jsonVal = "\"" + ODateHelper.getDateTimeFormatInstance().format(val) + "\"";
    } else if (val instanceof Type) {
      jsonVal = ((Type) val).name();
    } else {

      jsonVal = val.toString();
      //throw new UnsupportedOperationException("Cannot convert " + val + " - " + val.getClass() + " to JSON");
    }
    return jsonVal;
  }

  default String encode(final String s) {
    String result = s.replaceAll("\"", "\\\\\"");
    result = result.replaceAll("\n", "\\\\n");
    result = result.replaceAll("\t", "\\\\t");
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
