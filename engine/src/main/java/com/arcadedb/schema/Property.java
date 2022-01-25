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
package com.arcadedb.schema;

import com.arcadedb.index.Index;

import java.util.*;

public class Property {
  private final   DocumentType        owner;
  private final   String              name;
  private final   Type                type;
  private final   int                 id;
  protected final Map<String, Object> custom = new HashMap<>();
  private         Object              defaultValue;

  public Property(final DocumentType owner, final String name, final Type type) {
    this.owner = owner;
    this.name = name;
    this.type = type;
    this.id = owner.getSchema().getDictionary().getIdByName(name, true);
  }

  /**
   * Creates an index on this property.
   *
   * @param type   Index type between LSM_TREE and FULL_TEXT
   * @param unique true if the index is unique
   *
   * @return The index instance
   */
  public Index createIndex(final EmbeddedSchema.INDEX_TYPE type, final boolean unique) {
    return owner.createTypeIndex(type, unique, name);
  }

  /**
   * Returns an index on this property or creates it if not exists.
   *
   * @param type   Index type between LSM_TREE and FULL_TEXT
   * @param unique true if the index is unique
   *
   * @return The index instance
   */
  public Index getOrCreateIndex(final EmbeddedSchema.INDEX_TYPE type, final boolean unique) {
    return owner.getOrCreateTypeIndex(type, unique, name);
  }

  public String getName() {
    return name;
  }

  public Type getType() {
    return type;
  }

  public int getId() {
    return id;
  }

  public Object getDefaultValue() {
    return defaultValue;
  }

  public void setDefaultValue(final Object defaultValue) {
    if (!Objects.equals(this.defaultValue, defaultValue)) {
      this.defaultValue = defaultValue;

      // REPLACE THE SET OF PROPERTIES WITH DEFAULT VALUES DEFINED
      final Set<String> propertiesWithDefaultDefined = new HashSet<>(owner.propertiesWithDefaultDefined);
      if (defaultValue == null)
        propertiesWithDefaultDefined.remove(name);
      else
        propertiesWithDefaultDefined.add(name);
      owner.propertiesWithDefaultDefined = Collections.unmodifiableSet(propertiesWithDefaultDefined);

      owner.getSchema().getEmbedded().saveConfiguration();
    }
  }

  public Set<String> getCustomKeys() {
    return Collections.unmodifiableSet(custom.keySet());
  }

  public Object getCustomValue(final String key) {
    return custom.get(key);
  }

  public Object setCustomValue(final String key, final Object value) {
    Object prev;
    if (value == null)
      prev = custom.remove(key);
    else
      prev = custom.put(key, value);

    if (!Objects.equals(prev, value))
      owner.getSchema().getEmbedded().saveConfiguration();

    return prev;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    final Property property = (Property) o;
    return id == property.id && Objects.equals(name, property.name) && Objects.equals(type, property.type);
  }

  @Override
  public int hashCode() {
    return id;
  }
}
