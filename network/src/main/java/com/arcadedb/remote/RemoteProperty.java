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

import java.util.*;

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
    throw new UnsupportedOperationException();
  }

  @Override
  public Property setOfType(String ofType) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Property setReadonly(boolean readonly) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Property setMandatory(boolean mandatory) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Property setNotNull(boolean notNull) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Property setHidden(boolean hidden) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Property setMax(String max) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Property setMin(String min) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Property setRegexp(String regexp) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object setCustomValue(String key, Object value) {
    throw new UnsupportedOperationException();
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
    if (entry.containsKey("default"))
      defaultValue = entry.get("default");
    if (entry.containsKey("regexp"))
      regexp = (String) entry.get("regexp");
    if (entry.containsKey("custom"))
      custom = (Map<String, Object>) entry.get("custom");
  }
}
