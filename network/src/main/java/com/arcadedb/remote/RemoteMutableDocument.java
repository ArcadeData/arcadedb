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
import com.arcadedb.database.Document;
import com.arcadedb.database.JSONSerializer;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.json.JSONObject;

import java.util.*;

public class RemoteMutableDocument extends MutableDocument {
  protected final RemoteDatabase remoteDatabase;

  protected RemoteMutableDocument(final RemoteDatabase database, final String typeName) {
    super(null, database.getSchema().getType(typeName), null);
    this.remoteDatabase = database;
  }

  protected RemoteMutableDocument(final RemoteImmutableDocument source) {
    super(null, source.getType(), source.getIdentity());
    this.remoteDatabase = source.remoteDatabase;
    this.map.putAll(source.map);
    map.remove("@cat");
    map.remove("@type");
  }

  @Override
  public MutableDocument save() {
    rid = remoteDatabase.saveRecord(this);
    dirty = false;
    return this;
  }

  @Override
  public MutableDocument save(final String bucketName) {
    rid = remoteDatabase.saveRecord(this, bucketName);
    dirty = false;
    return this;
  }

  @Override
  public void delete() {
    remoteDatabase.deleteRecord(this);
  }

  @Override
  public void reload() {
    final ResultSet resultSet = remoteDatabase.query("sql", "select from " + rid);
    if (resultSet.hasNext()) {
      final Document document = resultSet.next().toElement();

      map.clear();
      map.putAll(document.propertiesAsMap());
      dirty = false;
    } else
      throw new RecordNotFoundException("Record " + rid + " not found", rid);
  }

  @Override
  public JSONObject toJSON(final boolean includeMetadata) {
    final JSONObject result = new JSONSerializer(database).map2json(map, null);
    if (includeMetadata) {
      result.put("@cat", "d");
      result.put("@type", getTypeName());
      if (getIdentity() != null)
        result.put("@rid", getIdentity().toString());
    }
    return result;
  }

  @Override
  public Map<String, Object> toMap(final boolean includeMetadata) {
    final Map<String, Object> result = new HashMap<>(map);
    if (includeMetadata) {
      result.put("@cat", "d");
      result.put("@type", getTypeName());
      if (getIdentity() != null)
        result.put("@rid", getIdentity().toString());
    }
    return result;
  }

  @Override
  public DocumentType getType() {
    throw new UnsupportedOperationException("Schema API are not supported in remote database");
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
  public void setBuffer(final Binary buffer) {
    throw new UnsupportedOperationException("Raw buffer API not supported in remote database");
  }

  @Override
  protected void checkForLazyLoadingProperties() {
    // NO ACTIONS
  }

  @Override
  protected Object convertValueToSchemaType(final String name, final Object value, final DocumentType type) {
    return value;
  }
}
