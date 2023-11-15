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

public class EmbeddedProperty extends AbstractProperty {

  public EmbeddedProperty(final EmbeddedDocumentType owner, final String name, final Type type) {
    super(owner, name, type, owner.getSchema().getDictionary().getIdByName(name, true));
  }

  @Override
  public Property setDefaultValue(final Object defaultValue) {
    final Database database = owner.getSchema().getEmbedded().getDatabase();

    // TODO: OPTIMIZE THE CASE WHERE FUNCTIONS ARE DEFAULT
    final Object convertedValue = defaultValue instanceof String ?
        defaultValue :
        Type.convert(database, defaultValue, type.javaDefaultType);

    if (!Objects.equals(this.defaultValue, convertedValue)) {
      this.defaultValue = convertedValue;

      // REPLACE THE SET OF PROPERTIES WITH DEFAULT VALUES DEFINED
      final Set<String> propertiesWithDefaultDefined = new HashSet<>(((EmbeddedDocumentType) owner).propertiesWithDefaultDefined);
      if (convertedValue == DEFAULT_NOT_SET)
        propertiesWithDefaultDefined.remove(name);
      else
        propertiesWithDefaultDefined.add(name);
      ((EmbeddedDocumentType) owner).propertiesWithDefaultDefined = Collections.unmodifiableSet(propertiesWithDefaultDefined);

      owner.getSchema().getEmbedded().saveConfiguration();
    }
    return this;
  }

  @Override
  public Property setOfType(String ofType) {
    final boolean changed = !Objects.equals(this.ofType, ofType);
    if (changed) {
      final EmbeddedSchema schema = (EmbeddedSchema) owner.getSchema();
      if (type == Type.LIST || type == Type.MAP) {
        if (Type.getTypeByName(ofType) != null) {
          ofType = ofType.toUpperCase(Locale.ENGLISH);
        } else {
          if (!schema.existsType(ofType))
            throw new SchemaException("Type '" + ofType + "' not defined");
        }
      } else if (type == Type.LINK || type == Type.EMBEDDED) {
        if (schema.isSchemaLoaded() && !schema.existsType(ofType))
          throw new SchemaException("Type '" + ofType + "' not defined");
      }

      this.ofType = ofType;
      owner.getSchema().getEmbedded().saveConfiguration();
    }
    return this;
  }

  @Override
  public Property setReadonly(final boolean readonly) {
    final boolean changed = !Objects.equals(this.readonly, readonly);
    if (changed) {
      this.readonly = readonly;
      owner.getSchema().getEmbedded().saveConfiguration();
    }
    return this;
  }

  @Override
  public Property setMandatory(final boolean mandatory) {
    final boolean changed = !Objects.equals(this.mandatory, mandatory);
    if (changed) {
      this.mandatory = mandatory;
      owner.getSchema().getEmbedded().saveConfiguration();
    }
    return this;
  }

  /**
   * Sets the constraint `not null` for the current property. If true, the property cannot be null.
   */
  @Override
  public Property setNotNull(final boolean notNull) {
    final boolean changed = !Objects.equals(this.notNull, notNull);
    if (changed) {
      this.notNull = notNull;
      owner.getSchema().getEmbedded().saveConfiguration();
    }
    return this;
  }

  @Override
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

  @Override
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

  @Override
  public Property setRegexp(final String regexp) {
    final boolean changed = !Objects.equals(this.regexp, regexp);
    if (changed) {
      this.regexp = regexp;
      owner.getSchema().getEmbedded().saveConfiguration();
    }
    return this;
  }

  @Override
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
}
