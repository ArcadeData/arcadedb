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

import com.arcadedb.schema.Property;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.serializer.json.JSONObject;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.arcadedb.schema.Property.RID_PROPERTY;

/**
 * Detached document instances are generated from a document and can be accessed outside a transaction.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class DetachedDocument extends BaseDocument {
  protected Map<String, Object> map;

  protected DetachedDocument(final Document source) {
    super(null, source.getType(), source.getIdentity(), null);
    init(source);
  }

  private void init(final Document sourceDocument) {
    this.map = new LinkedHashMap<>();
    final Map<String, Object> sourceMap = sourceDocument.propertiesAsMap();
    for (final Map.Entry<String, Object> entry : sourceMap.entrySet()) {

      Object value = entry.getValue();

      if (value instanceof List list) {
        for (int i = 0; i < list.size(); i++) {
          final Object embValue = list.get(i);
          if (embValue instanceof EmbeddedDocument document)
            ((List) value).set(i, document.detach());
        }
      } else if (value instanceof Map) {
        final Map<String, Object> map = (Map<String, Object>) value;

        for (final Map.Entry<String, Object> subentry : map.entrySet()) {
          final Object embValue = subentry.getValue();
          if (embValue instanceof EmbeddedDocument document)
            map.put(subentry.getKey(), document.detach());
        }

      } else if (value instanceof EmbeddedDocument document)
        value = document.detach();

      this.map.put(entry.getKey(), value);
    }
  }

  @Override
  public synchronized MutableDocument modify() {
    throw new UnsupportedOperationException(
        "Detached document cannot be modified. Get a new regular object from the database by its id to modify it");
  }

  @Override
  public synchronized Map<String, Object> toMap(final boolean includeMetadata) {
    final Map<String, Object> result = new HashMap<>(map);
    if (includeMetadata) {
      result.put(Property.CAT_PROPERTY, "d");
      result.put(Property.TYPE_PROPERTY, type.getName());
      if (getIdentity() != null)
        result.put(RID_PROPERTY, getIdentity().toString());
    }
    return result;
  }

  /**
   * @return
   */
  @Override
  public Map<String, Object> propertiesAsMap() {
    return Map.of();
  }

  @Override
  public synchronized JSONObject toJSON(final boolean includeMetadata) {
    final JSONObject result = new JsonSerializer(database).map2json(map, type, includeMetadata);
    if (includeMetadata) {
      result.put(Property.CAT_PROPERTY, "d");
      result.put(Property.TYPE_PROPERTY, type.getName());
      if (getIdentity() != null)
        result.put(RID_PROPERTY, getIdentity().toString());
    }
    return result;
  }

  @Override
  public synchronized boolean has(final String propertyName) {
    return map.containsKey(propertyName);
  }

  public synchronized Object get(final String propertyName) {
    return map.get(propertyName);
  }

  @Override
  public void setBuffer(final Binary buffer) {
    throw new UnsupportedOperationException("setBuffer");
  }

  @Override
  public synchronized String toString() {
    final StringBuilder result = new StringBuilder(256);
    if (rid != null)
      result.append(rid);
    if (type != null) {
      result.append('@');
      result.append(type.getName());
    }
    result.append('[');
    if (map == null) {
      result.append('?');
    } else {
      int i = 0;
      for (final Map.Entry<String, Object> entry : map.entrySet()) {
        if (i > 0)
          result.append(',');

        result.append(entry.getKey());
        result.append('=');
        result.append(entry.getValue());
        i++;
      }
    }
    result.append(']');
    return result.toString();
  }

  @Override
  public synchronized Set<String> getPropertyNames() {
    return map.keySet();
  }

  @Override
  public void reload() {
    map = null;
    buffer = null;
    super.reload();
    init(this);
  }
}
