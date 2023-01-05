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
import com.arcadedb.serializer.json.JSONObject;

import java.util.*;

public class Property {
  private final   DocumentType        owner;
  private final   String              name;
  private final   Type                type;
  private final   int                 id;
  protected final Map<String, Object> custom    = new HashMap<>();
  private         Object              defaultValue;
  private         boolean             readonly  = false;
  private         boolean             mandatory = false;
  private         boolean             notNull   = false;
  private         String              max       = null;
  private         String              min       = null;
  private         String              regexp    = null;

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

  public Property setDefaultValue(final Object defaultValue) {
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
    return this;
  }

  public Property setReadonly(final boolean readonly) {
    final boolean changed = !Objects.equals(this.readonly, readonly);
    if (changed) {
      this.readonly = readonly;
      owner.getSchema().getEmbedded().saveConfiguration();
    }
    return this;
  }

  public boolean isReadonly() {
    return readonly;
  }

  public Property setMandatory(final boolean mandatory) {
    final boolean changed = !Objects.equals(this.mandatory, mandatory);
    if (changed) {
      this.mandatory = mandatory;
      owner.getSchema().getEmbedded().saveConfiguration();
    }
    return this;
  }

  public boolean isMandatory() {
    return mandatory;
  }

  /**
   * Sets the constraint `not null` for the current property. If true, the property cannot be null.
   */
  public Property setNotNull(final boolean notNull) {
    final boolean changed = !Objects.equals(this.notNull, notNull);
    if (changed) {
      this.notNull = notNull;
      owner.getSchema().getEmbedded().saveConfiguration();
    }
    return this;
  }

  /**
   * Returns true if the current property has set the constraint `not null`. If true, the property cannot be null.
   */
  public boolean isNotNull() {
    return notNull;
  }

  public Property setMax(final String max) {
    final boolean changed = !Objects.equals(this.max, max);
    if (changed) {
      this.max = max;
      owner.getSchema().getEmbedded().saveConfiguration();
    }
    return this;
  }

  public String getMax() {
    return max;
  }

  public Property setMin(final String min) {
    final boolean changed = !Objects.equals(this.min, min);
    if (changed) {
      this.min = min;
      owner.getSchema().getEmbedded().saveConfiguration();
    }
    return this;
  }

  public String getMin() {
    return min;
  }

  public Property setRegexp(final String regexp) {
    final boolean changed = !Objects.equals(this.regexp, regexp);
    if (changed) {
      this.regexp = regexp;
      owner.getSchema().getEmbedded().saveConfiguration();
    }
    return this;
  }

  public String getRegexp() {
    return regexp;
  }

  public Set<String> getCustomKeys() {
    return Collections.unmodifiableSet(custom.keySet());
  }

  public Object getCustomValue(final String key) {
    return custom.get(key);
  }

  public Object setCustomValue(final String key, final Object value) {
    final Object prev;
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

  public JSONObject toJSON() {
    final JSONObject json = new JSONObject();

    json.put("type", type.name);

    final Object defValue = getDefaultValue();
    if (defValue != null)
      json.put("default", defValue);

    if (readonly)
      json.put("readonly", readonly);
    if (mandatory)
      json.put("mandatory", mandatory);
    if (notNull)
      json.put("notNull", notNull);
    if (max != null)
      json.put("max", max);
    if (min != null)
      json.put("min", min);
    if (regexp != null)
      json.put("regexp", regexp);

    json.put("custom", new JSONObject(custom));

    return json;
  }
}
