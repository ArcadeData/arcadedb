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
 */
package com.arcadedb.database;

import org.json.JSONObject;

import java.util.*;

public class DetachedDocument extends ImmutableDocument {
  private Map<String, Object> map;

  protected DetachedDocument(final BaseDocument source) {
    super(null, source.type, source.rid, null);
    init(source);
  }

  private void init(Document sourceDocument) {
    this.map = new LinkedHashMap<>();
    final Map<String, Object> sourceMap = sourceDocument.toMap();
    for (Map.Entry<String, Object> entry : sourceMap.entrySet()) {
      Object value = entry.getValue();

      if (value instanceof List) {
        for (int i = 0; i < ((List) value).size(); i++) {
          final Object embValue = ((List) value).get(i);
          if (embValue instanceof EmbeddedDocument)
            ((List) value).set(i, ((EmbeddedDocument) embValue).detach());
        }
      } else if (value instanceof EmbeddedDocument)
        value = ((EmbeddedDocument) value).detach();

      this.map.put(entry.getKey(), value);
    }
  }

  @Override
  public synchronized MutableDocument modify() {
    throw new UnsupportedOperationException("Detached document cannot be modified. Get a new regular object from the database by its id to modify it");
  }

  @Override
  public Map<String, Object> toMap() {
    return new HashMap<>(map);
  }

  @Override
  public JSONObject toJSON() {
    final JSONObject result = new JSONSerializer(database).map2json(map);
    result.put("@type", type.getName());
    if (getIdentity() != null)
      result.put("@rid", getIdentity().toString());
    return result;
  }

  @Override
  public boolean has(String propertyName) {
    return map.containsKey(propertyName);
  }

  public Object get(final String propertyName) {
    return map.get(propertyName);
  }

  @Override
  public void setBuffer(Binary buffer) {
    throw new UnsupportedOperationException("setBuffer");
  }

  @Override
  public String toString() {
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
      for (Map.Entry<String, Object> entry : map.entrySet()) {
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
  public Set<String> getPropertyNames() {
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
