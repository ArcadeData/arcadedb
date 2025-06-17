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
import com.arcadedb.schema.DocumentType;
import com.arcadedb.serializer.json.JSONObject;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.arcadedb.schema.Property.CAT_PROPERTY;
import static com.arcadedb.schema.Property.RID_PROPERTY;
import static com.arcadedb.schema.Property.TYPE_PROPERTY;

public class RemoteImmutableDocument extends ImmutableDocument {
  protected final RemoteDatabase      remoteDatabase;
  protected final Map<String, Object> map;

  protected RemoteImmutableDocument(final RemoteDatabase remoteDatabase, final Map<String, Object> attributes) {
    super(null, remoteDatabase.getSchema().getType((String) attributes.get("@type")), null, null);
    this.remoteDatabase = remoteDatabase;
    this.map = new HashMap<>(attributes);

    final String ridAsString = (String) map.remove(RID_PROPERTY);
    if (ridAsString != null)
      this.rid = new RID(remoteDatabase, ridAsString);
    else
      this.rid = null;

    map.remove("@type");
    map.remove("@out");
    map.remove("@in");
    map.remove("@cat");
  }

  protected RemoteImmutableDocument(final RemoteDatabase remoteDatabase, final Map<String, Object> attributes,
      final String typeName, final RID rid) {
    super(null, remoteDatabase.getSchema().getType(typeName), rid, null);
    this.remoteDatabase = remoteDatabase;
    this.map = new HashMap<>(attributes);
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
    final JSONObject result = new JSONSerializer(database).map2json(map, null);
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
}
