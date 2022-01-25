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
import com.arcadedb.schema.VertexType;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;
import java.util.logging.Level;

public class JSONSerializer {
  private final Database database;

  public JSONSerializer(final Database database) {
    this.database = database;
  }

  public JSONObject map2json(final Map<String, Object> map) {
    final JSONObject json = new JSONObject();
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      final Object value = convertToJSONType(entry.getValue());

      if (value instanceof Number && !Float.isFinite(((Number) value).floatValue())) {
        LogManager.instance()
            .log(this, Level.SEVERE, "Found non finite number in map with key '%s', ignore this entry in the conversion", entry.getKey());
        continue;
      }

      json.put(entry.getKey(), value);
    }

    return json;
  }

  public Map<String, Object> json2map(final JSONObject json) {
    final Map<String, Object> map = new HashMap<>();
    for (String k : json.keySet()) {
      final Object value = convertFromJSONType(json.get(k));
      map.put(k, value);
    }

    return map;
  }

  private Object convertToJSONType(Object value) {
    if (value instanceof Document) {
      final JSONObject json = ((Document) value).toJSON();
      json.put("@type", ((Document) value).getTypeName());
      value = json;
    } else if (value instanceof Collection) {
      final Collection c = (Collection) value;
      final JSONArray array = new JSONArray();
      for (Iterator it = c.iterator(); it.hasNext(); )
        array.put(convertToJSONType(it.next()));
      value = array;
    } else if (value instanceof Date)
      value = ((Date) value).getTime();
    else if (value instanceof Map) {
      final Map<String, Object> m = (Map<String, Object>) value;
      final JSONObject map = new JSONObject();
      for (Map.Entry<String, Object> entry : m.entrySet())
        map.put(entry.getKey(), convertToJSONType(entry.getValue()));
      value = map;
    }

    return value;
  }

  private Object convertFromJSONType(Object value) {
    if (value instanceof JSONObject) {
      final JSONObject json = (JSONObject) value;
      final String embeddedTypeName = json.getString("@type");

      final DocumentType type = database.getSchema().getType(embeddedTypeName);

      if (type instanceof VertexType) {
        final MutableVertex v = database.newVertex(embeddedTypeName);
        v.fromJSON((JSONObject) value);
        value = v;
      } else if (type != null) {
        final MutableEmbeddedDocument embeddedDocument = ((DatabaseInternal) database).newEmbeddedDocument(null, embeddedTypeName);
        embeddedDocument.fromJSON((JSONObject) value);
        value = embeddedDocument;
      }
    } else if (value instanceof JSONArray) {
      final JSONArray array = (JSONArray) value;
      final List<Object> list = new ArrayList<>();
      for (int i = 0; i < array.length(); ++i)
        list.add(convertFromJSONType(array.get(i)));
      value = list;
    }

    return value;
  }
}
