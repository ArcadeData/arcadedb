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
import com.arcadedb.index.Index;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.ParseException;
import com.arcadedb.query.sql.parser.SqlParser;
import com.arcadedb.serializer.json.JSONObject;

import java.io.*;
import java.util.*;

public abstract class AbstractProperty implements Property {
  protected final        DocumentType        owner;
  protected final        String              name;
  protected final        Type                type;
  protected final        int                 id;
  protected              Map<String, Object> custom          = new HashMap<>();
  protected              Object              defaultValue    = DEFAULT_NOT_SET;
  protected              boolean             readonly        = false;
  protected              boolean             mandatory       = false;
  protected              boolean             notNull         = false;
  protected              String              max             = null;
  protected              String              min             = null;
  protected              String              regexp          = null;
  protected              String              ofType          = null;
  protected final static Object              DEFAULT_NOT_SET = "<DEFAULT_NOT_SET>";

  public AbstractProperty(final DocumentType owner, final String name, final Type type, final int id) {
    this.owner = owner;
    this.name = name;
    this.type = type;
    this.id = id;
  }

  /**
   * Creates an index on this property.
   *
   * @param type   Index type between LSM_TREE and FULL_TEXT
   * @param unique true if the index is unique
   *
   * @return The index instance
   */
  @Override
  public Index createIndex(final Schema.INDEX_TYPE type, final boolean unique) {
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
  @Override
  public Index getOrCreateIndex(final Schema.INDEX_TYPE type, final boolean unique) {
    return owner.getSchema().buildTypeIndex(owner.getName(), new String[] { name }).withType(type).withUnique(unique)
        .withIgnoreIfExists(true).create();
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Type getType() {
    return type;
  }

  @Override
  public int getId() {
    return id;
  }

  @Override
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

  @Override
  public String getOfType() {
    return ofType;
  }

  @Override
  public boolean isReadonly() {
    return readonly;
  }

  @Override
  public boolean isMandatory() {
    return mandatory;
  }

  /**
   * Returns true if the current property has set the constraint `not null`. If true, the property cannot be null.
   */
  @Override
  public boolean isNotNull() {
    return notNull;
  }

  @Override
  public String getMax() {
    return max;
  }

  @Override
  public String getMin() {
    return min;
  }

  @Override
  public String getRegexp() {
    return regexp;
  }

  @Override
  public Set<String> getCustomKeys() {
    return Collections.unmodifiableSet(custom.keySet());
  }

  @Override
  public Object getCustomValue(final String key) {
    return custom.get(key);
  }

  @Override
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

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    final AbstractProperty property = (AbstractProperty) o;
    return id == property.id && Objects.equals(name, property.name) && Objects.equals(type, property.type) && Objects.equals(ofType,
        property.ofType);
  }

  @Override
  public int hashCode() {
    return id;
  }
}
