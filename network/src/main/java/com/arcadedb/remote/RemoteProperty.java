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
package com.arcadedb.remote;

import com.arcadedb.schema.AbstractProperty;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Property used by {@link RemoteDatabase} class. The metadata are cached from the server until the schema is changed or
 * {@link RemoteSchema#reload()} is called.
 * <p>
 * This class is not thread safe. For multi-thread usage create one instance of RemoteDatabase per thread.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */

public class RemoteProperty extends AbstractProperty {

  RemoteProperty(final DocumentType owner, final Map<String, Object> record) {
    super(owner, (String) record.get("name"), typeOf((String) record.get("type")), (Integer) record.get("id"));
    reload(record);
  }

  /**
   * The type schema:types names. The server writes the constant's name ({@code ARRAY_OF_LONGS}), which
   * {@link Type#getTypeByName} does not know for the five array types, whose declared name is {@code Long[]} and the like:
   * such a property had no type on the client, so its values kept the list the JSON parser built instead of being read as the
   * primitive array they are (issue #12189).
   */
  static Type typeOf(final String name) {
    if (name == null)
      return null;
    final Type type = Type.getTypeByName(name);
    if (type != null)
      return type;
    try {
      return Type.valueOf(name.toUpperCase(Locale.ENGLISH));
    } catch (final IllegalArgumentException e) {
      return null;
    }
  }

  @Override
  public Property setDefaultValue(Object defaultValue) {
    throw new UnsupportedOperationException("setDefaultValue() is not supported in remote database. Use SQL ALTER PROPERTY <type>.<property> DEFAULT instead.");
  }

  @Override
  public Property rename(String newName) {
    throw new UnsupportedOperationException("rename() is not supported in remote database. Use SQL ALTER PROPERTY <type>.<property> NAME instead.");
  }

  @Override
  public Property setOfType(String ofType) {
    throw new UnsupportedOperationException("setOfType() is not supported in remote database. ALTER PROPERTY has no OF TYPE setting: drop the property and declare it again with SQL CREATE PROPERTY.");
  }

  @Override
  public Property setReadonly(boolean readonly) {
    throw new UnsupportedOperationException("setReadonly() is not supported in remote database. Use SQL ALTER PROPERTY <type>.<property> READONLY instead.");
  }

  @Override
  public Property setMandatory(boolean mandatory) {
    throw new UnsupportedOperationException("setMandatory() is not supported in remote database. Use SQL ALTER PROPERTY <type>.<property> MANDATORY instead.");
  }

  @Override
  public Property setNotNull(boolean notNull) {
    throw new UnsupportedOperationException("setNotNull() is not supported in remote database. Use SQL ALTER PROPERTY <type>.<property> NOTNULL instead.");
  }

  @Override
  public Property setHidden(boolean hidden) {
    throw new UnsupportedOperationException("setHidden() is not supported in remote database. Use SQL ALTER PROPERTY <type>.<property> HIDDEN instead.");
  }

  @Override
  public Property setExternal(boolean external) {
    throw new UnsupportedOperationException("setExternal() is not supported in remote database. Use SQL ALTER PROPERTY <type>.<property> EXTERNAL instead.");
  }

  @Override
  public Property setCompression(String compression) {
    throw new UnsupportedOperationException("setCompression() is not supported in remote database. Use SQL ALTER PROPERTY <type>.<property> COMPRESSION instead.");
  }

  @Override
  public Property setMax(String max) {
    throw new UnsupportedOperationException("setMax() is not supported in remote database. Use SQL ALTER PROPERTY <type>.<property> MAX instead.");
  }

  @Override
  public Property setMin(String min) {
    throw new UnsupportedOperationException("setMin() is not supported in remote database. Use SQL ALTER PROPERTY <type>.<property> MIN instead.");
  }

  @Override
  public Property setRegexp(String regexp) {
    throw new UnsupportedOperationException("setRegexp() is not supported in remote database. Use SQL ALTER PROPERTY <type>.<property> REGEXP instead.");
  }

  @Override
  public Object setCustomValue(String key, Object value) {
    throw new UnsupportedOperationException("setCustomValue() is not supported in remote database. Use SQL ALTER PROPERTY <type>.<property> CUSTOM <key> = <value> instead.");
  }

  /**
   * Whether this is the property {@code entry} of a {@code schema:types} record describes. The id and the type of a
   * property are fixed when it is built, so an entry that carries another type describes a property dropped and declared
   * again under the same name, and this instance must not stand for it (issue #12188).
   */
  boolean describes(final Map<String, Object> entry) {
    return entry.get("id") instanceof Integer entryId && entryId == id
        && type == Type.getTypeByName((String) entry.get("type"));
  }

  void reload(final Map<String, Object> entry) {
    // The server lists an attribute only while it is set (FetchFromSchemaTypesStep), so one that is absent is the default,
    // not "unchanged": taken as unchanged, an ALTER PROPERTY that cleared a constraint left it in force on this client for
    // the life of the connection (issue #12188).
    ofType = (String) entry.get("ofType");
    mandatory = Boolean.TRUE.equals(entry.get("mandatory"));
    readonly = Boolean.TRUE.equals(entry.get("readOnly"));
    notNull = Boolean.TRUE.equals(entry.get("notNull"));
    min = (String) entry.get("min");
    max = (String) entry.get("max");
    hidden = Boolean.TRUE.equals(entry.get("hidden"));
    external = Boolean.TRUE.equals(entry.get("external"));
    compression = (String) entry.get("compression");
    if (entry.containsKey("default"))
      // No compiled expression: the server sends the definition and the remote side only reports it back, having no
      // embedded database to evaluate an SQL expression against.
      setDefaultValueDefinition(entry.get("default"));
    else if (getDefaultValueDefinition() != null)
      setDefaultValueDefinition(DEFAULT_NOT_SET);
    regexp = Regexp.of((String) entry.get("regexp"));
    if (entry.containsKey("custom") && entry.get("custom") instanceof Map<?, ?> received) {
      // built aside and published whole, like every change of the map
      final Map<String, Object> built = new HashMap<>(received.size());
      for (final Map.Entry<?, ?> e : received.entrySet())
        built.put(String.valueOf(e.getKey()), e.getValue());
      custom = Collections.unmodifiableMap(built);
    }
  }
}
