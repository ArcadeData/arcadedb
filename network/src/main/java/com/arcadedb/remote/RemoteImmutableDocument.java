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

import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.ImmutableDocument;
import com.arcadedb.database.JSONSerializer;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;

import java.util.*;
import java.util.logging.*;

import static com.arcadedb.schema.Property.CAT_PROPERTY;
import static com.arcadedb.schema.Property.RID_PROPERTY;
import static com.arcadedb.schema.Property.TYPE_PROPERTY;

public class RemoteImmutableDocument extends ImmutableDocument {
  protected final RemoteDatabase      remoteDatabase;
  protected final Map<String, Object> map;

  protected RemoteImmutableDocument(final RemoteDatabase remoteDatabase, final Map<String, Object> attributes) {
    super(null, remoteDatabase.getSchema().getType((String) attributes.get(Property.TYPE_PROPERTY)), null, null);
    this.remoteDatabase = remoteDatabase;

    this.map = new HashMap<>(attributes.size());

    final Map<String, Type> propTypes = parsePropertyTypes((String) attributes.get(Property.PROPERTY_TYPES_PROPERTY));

    for (Map.Entry<String, Object> entry : attributes.entrySet()) {
      final String fieldName = entry.getKey();
      if (!Property.METADATA_PROPERTIES.contains(fieldName)) {
        Object value = entry.getValue();

        final Property property = type.getPolymorphicPropertyIfExists(fieldName);
        final Type propType = property != null ? property.getType() : propTypes.get(fieldName);

        Class javaImplementation = value != null ? value.getClass() : null;
        if (propType == Type.DATE)
          javaImplementation = remoteDatabase.getSerializer().getDateImplementation();
        else if (propType == Type.DATETIME)
          javaImplementation = remoteDatabase.getSerializer().getDateTimeImplementation();
        else if (propType != null)
          javaImplementation = propType.getDefaultJavaType();

        value = Type.convert(null, value, javaImplementation, property);

        map.put(fieldName, value);
      }
    }

    final String ridAsString = (String) attributes.get(RID_PROPERTY);
    if (ridAsString != null)
      this.rid = new RID(remoteDatabase, ridAsString);
    else
      this.rid = null;
  }

  @Override
  public String getTypeName() {
    return type.getName();
  }

  @Override
  public synchronized Set<String> getPropertyNames() {
    return Collections.unmodifiableSet(map.keySet());
  }

  @Override
  public synchronized boolean has(final String propertyName) {
    return map.containsKey(propertyName);
  }

  public synchronized Object get(final String propertyName) {

    return map.get(propertyName);
  }

  @Override
  public synchronized MutableDocument modify() {
    return new RemoteMutableDocument(this);
  }

  @Override
  public synchronized Map<String, Object> toMap(final boolean includeMetadata) {
    final HashMap<String, Object> result = new HashMap<>(map);
    if (includeMetadata) {
      result.put(CAT_PROPERTY, "d");
      result.put(TYPE_PROPERTY, getTypeName());
      if (getIdentity() != null)
        result.put(RID_PROPERTY, getIdentity().toString());
    }
    return result;
  }

  @Override
  public synchronized JSONObject toJSON(final boolean includeMetadata) {
    final JSONObject result = new JSONSerializer(database).map2json(map, type, includeMetadata);
    if (includeMetadata) {
      result.put(CAT_PROPERTY, "d");
      result.put(TYPE_PROPERTY, getTypeName());
      if (getIdentity() != null)
        result.put(RID_PROPERTY, getIdentity().toString());
    }
    return result;
  }

  @Override
  public DocumentType getType() {
    return type;
  }

  @Override
  public Database getDatabase() {
    throw new UnsupportedOperationException("Embedded Database API not supported in remote database");
  }

  @Override
  public Binary getBuffer() {
    throw new UnsupportedOperationException("Raw buffer API not supported in remote database");
  }

  @Override
  public void reload() {
    throw new UnsupportedOperationException("Unable to reload an immutable document");
  }

  @Override
  protected boolean checkForLazyLoading() {
    return false;
  }

  private Map<String, Type> parsePropertyTypes(final String propTypesAsString) {
    Map<String, Type> propTypes = null;
    if (propTypesAsString != null) {
      for (String entry : propTypesAsString.split(",")) {
        try {
          final String[] entryPair = entry.split(":");
          if (entryPair.length == 2) {
            final Type propType = Type.getById((byte) Integer.parseInt(entryPair[1]));
            propTypes.put(entryPair[0], propType);
          } else
            LogManager.instance().log(this, Level.SEVERE, "Error parsing property types " + entryPair);
        } catch (Exception e) {
          LogManager.instance().log(this, Level.SEVERE, "Error parsing property types", e);
        }
      }
    }
    return propTypes != null ? propTypes : Collections.emptyMap();
  }
}
