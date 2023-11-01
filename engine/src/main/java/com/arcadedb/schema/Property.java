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

import com.arcadedb.database.Database;
import com.arcadedb.database.Record;
import com.arcadedb.exception.*;
import com.arcadedb.index.Index;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.ParseException;
import com.arcadedb.query.sql.parser.SqlParser;
import com.arcadedb.serializer.json.JSONObject;

import java.io.*;
import java.util.*;

public class Property {
  private final        DocumentType        owner;
  private final        String              name;
  private final        Type                type;
  private final        int                 id;
  protected final      Map<String, Object> custom          = new HashMap<>();
  private              Object              defaultValue    = DEFAULT_NOT_SET;
  private              boolean             readonly        = false;
  private              boolean             mandatory       = false;
  private              boolean             notNull         = false;
  private              String              max             = null;
  private              String              min             = null;
  private              String              regexp          = null;
  private              String              ofType          = null;
  private final static Object              DEFAULT_NOT_SET = "<DEFAULT_NOT_SET>";

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
    return owner.getSchema().buildTypeIndex(owner.getName(), new String[] { name }).withType(type).withUnique(unique).withIgnoreIfExists(true).create();
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
    if (defaultValue != null && defaultValue != DEFAULT_NOT_SET) {
      if (defaultValue instanceof String) {
        // TODO: OPTIMIZE THE CASE WHERE FUNCTIONS ARE DEFAULT
        final Database database = owner.getSchema().getEmbedded().getDatabase();
        final Expression expr;
        try {
          expr = new SqlParser(database, new ByteArrayInputStream(defaultValue.toString().getBytes())).ParseExpression();
          final Object result = expr.execute((Record) null, new BasicCommandContext().setDatabase(database));
          return Type.convert(database, result, type.javaDefaultType);
        } catch (ParseException e) {
          // IGNORE IT
        }
      }
    }

    return defaultValue;
  }

  public Property setDefaultValue(final Object defaultValue) {
    final Database database = owner.getSchema().getEmbedded().getDatabase();

    // TODO: OPTIMIZE THE CASE WHERE FUNCTIONS ARE DEFAULT
    final Object convertedValue = defaultValue instanceof String ? defaultValue : Type.convert(database, defaultValue, type.javaDefaultType);

    if (!Objects.equals(this.defaultValue, convertedValue)) {
      this.defaultValue = convertedValue;

      // REPLACE THE SET OF PROPERTIES WITH DEFAULT VALUES DEFINED
      final Set<String> propertiesWithDefaultDefined = new HashSet<>(owner.propertiesWithDefaultDefined);
      if (convertedValue == DEFAULT_NOT_SET)
        propertiesWithDefaultDefined.remove(name);
      else
        propertiesWithDefaultDefined.add(name);
      owner.propertiesWithDefaultDefined = Collections.unmodifiableSet(propertiesWithDefaultDefined);

      owner.getSchema().getEmbedded().saveConfiguration();
    }
    return this;
  }

  public String getOfType() {
    return ofType;
  }

  public Property setOfType(String ofType) {
    final boolean changed = !Objects.equals(this.ofType, ofType);
    if (changed) {
      if (type == Type.LIST || type == Type.MAP) {
        if (Type.getTypeByName(ofType) != null) {
          ofType = ofType.toUpperCase(Locale.ENGLISH);
        } else {
          if (!owner.schema.existsType(ofType))
            throw new SchemaException("Type '" + ofType + "' not defined");
        }
      } else if (type == Type.LINK || type == Type.EMBEDDED) {
        if (owner.schema.isSchemaLoaded() && !owner.schema.existsType(ofType))
          throw new SchemaException("Type '" + ofType + "' not defined");
      }

      this.ofType = ofType;
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
      switch (type) {
      case LINK:
      case BOOLEAN:
      case EMBEDDED:
        throw new IllegalArgumentException("Maximum value not applicable for type " + type);

      case STRING:
      case BINARY:
      case LIST:
      case MAP:
        if (Integer.parseInt(max) < 0)
          throw new IllegalArgumentException("Maximum value for type " + type + " is 0");
        break;
      }

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
      switch (type) {
      case LINK:
      case BOOLEAN:
      case EMBEDDED:
        throw new IllegalArgumentException("Minimum value not applicable for type " + type);

      case STRING:
      case BINARY:
      case LIST:
      case MAP:
        if (Integer.parseInt(min) < 0)
          throw new IllegalArgumentException("Minimum value for type " + type + " is 0");
        break;
      }

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

  public JSONObject toJSON() {
    final JSONObject json = new JSONObject();

    json.put("type", type.name);

    if (ofType != null)
      json.put("of", ofType);

    final Object defValue = defaultValue;
    if (defValue != DEFAULT_NOT_SET)
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
    return id == property.id && Objects.equals(name, property.name) && Objects.equals(type, property.type) && Objects.equals(ofType, property.ofType);
  }

  @Override
  public int hashCode() {
    return id;
  }
}
