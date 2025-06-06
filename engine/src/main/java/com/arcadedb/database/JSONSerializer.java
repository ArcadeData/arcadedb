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
package com.arcadedb.database;

import com.arcadedb.graph.MutableVertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalVertexType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.DateUtils;

import java.time.temporal.*;
import java.util.*;
import java.util.logging.*;

public class JSONSerializer {
  private final Database database;

  public JSONSerializer(final Database database) {
    this.database = database;
  }

  public JSONObject map2json(final Map<String, Object> map, final DocumentType type, final boolean includeMetadata,
      final String... includeProperties) {
    final JSONObject json = new JSONObject();

    final Set<String> includePropertiesSet;
    if (includeProperties.length > 0)
      includePropertiesSet = new HashSet<>(Arrays.asList(includeProperties));
    else
      includePropertiesSet = null;

    final StringBuilder propertyTypes = includeMetadata ? new StringBuilder() : null;

    for (final Map.Entry<String, Object> entry : map.entrySet()) {
      final String propertyName = entry.getKey();

      if (includePropertiesSet != null && !includePropertiesSet.contains(propertyName))
        continue;

      Object value = entry.getValue();

      final Type propertyType;
      if (type != null && type.existsProperty(propertyName))
        propertyType = type.getProperty(propertyName).getType();
      else if (value != null)
        propertyType = Type.getTypeByClass(value.getClass());
      else
        propertyType = null;

      if (includeMetadata && propertyType != null) {
        if (propertyTypes.length() > 0)
          propertyTypes.append(",");
        propertyTypes.append(propertyName).append(":").append(propertyType.getId());
      }

      value = convertToJSONType(value, propertyType);

      if (value instanceof Number number && !Float.isFinite(number.floatValue())) {
        LogManager.instance()
            .log(this, Level.SEVERE, "Found non finite number in map with key '%s', ignore this entry in the conversion",
                propertyName);
        continue;
      }

      json.put(propertyName, value);
    }

    if (propertyTypes != null && !propertyTypes.isEmpty())
      json.put(Property.PROPERTY_TYPES_PROPERTY, propertyTypes);

    return json;
  }

  public Map<String, Object> json2map(final JSONObject json) {
    final Map<String, Object> map = new HashMap<>();
    for (final String k : json.keySet()) {
      final Object value = convertFromJSONType(json.get(k));
      map.put(k, value);
    }

    return map;
  }

  private Object convertToJSONType(Object value) {
    return convertToJSONType(value, null);
  }

  private Object convertToJSONType(Object value, final Type type) {
    if (value instanceof Document document) {
      value = document.toJSON(true);
    } else if (value instanceof Collection c) {
      final JSONArray array = new JSONArray();
      for (final Iterator it = c.iterator(); it.hasNext(); )
        array.put(convertToJSONType(it.next()));
      value = array;
    } else if (value instanceof Date date)
      value = date.getTime();
    else if (value instanceof Temporal)
      value = DateUtils.dateTimeToTimestamp(value, type != null ? DateUtils.getPrecisionFromType(type) : ChronoUnit.MILLIS);
    else if (value instanceof Map) {
      final Map<String, Object> m = (Map<String, Object>) value;
      final JSONObject map = new JSONObject();
      for (final Map.Entry<String, Object> entry : m.entrySet())
        map.put(entry.getKey(), convertToJSONType(entry.getValue()));
      value = map;
    }

    return value;
  }

  private Object convertFromJSONType(Object value) {
    if (value instanceof JSONObject json) {
      final String embeddedTypeName = json.getString(Property.TYPE_PROPERTY);

      final DocumentType type = database.getSchema().getType(embeddedTypeName);

      if (type instanceof LocalVertexType) {
        final MutableVertex v = database.newVertex(embeddedTypeName);
        v.fromJSON((JSONObject) value);
        value = v;
      } else if (type != null) {
        final MutableEmbeddedDocument embeddedDocument = ((DatabaseInternal) database).newEmbeddedDocument(null, embeddedTypeName);
        embeddedDocument.fromJSON((JSONObject) value);
        value = embeddedDocument;
      }
    } else if (value instanceof JSONArray array) {
      final List<Object> list = new ArrayList<>();
      for (int i = 0; i < array.length(); ++i)
        list.add(convertFromJSONType(array.get(i)));
      value = list;
    }

    return value;
  }
}
