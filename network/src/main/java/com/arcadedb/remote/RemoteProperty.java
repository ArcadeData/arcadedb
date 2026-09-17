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
    super(owner, (String) record.get("name"), Type.getTypeByName((String) record.get("type")), (Integer) record.get("id"));
    reload(record);
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

  void reload(final Map<String, Object> entry) {
    if (entry.containsKey("ofType"))
      ofType = (String) entry.get("ofType");
    if (entry.containsKey("mandatory"))
      mandatory = (Boolean) entry.get("mandatory");
    if (entry.containsKey("readOnly"))
      readonly = (Boolean) entry.get("readOnly");
    if (entry.containsKey("notNull"))
      notNull = (Boolean) entry.get("notNull");
    if (entry.containsKey("min"))
      min = (String) entry.get("min");
    if (entry.containsKey("max"))
      max = (String) entry.get("max");
    if (entry.containsKey("hidden"))
      hidden = (Boolean) entry.get("hidden");
    if (entry.containsKey("external"))
      external = (Boolean) entry.get("external");
    if (entry.containsKey("compression"))
      compression = (String) entry.get("compression");
    if (entry.containsKey("default"))
      // No compiled expression: the server sends the definition and the remote side only reports it back, having no
      // embedded database to evaluate an SQL expression against.
      setDefaultValueDefinition(entry.get("default"));
    if (entry.containsKey("regexp"))
      regexp = (String) entry.get("regexp");
    if (entry.containsKey("custom"))
      custom = (Map<String, Object>) entry.get("custom");
  }
}
