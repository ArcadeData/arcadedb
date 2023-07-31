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
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.serializer.json.JSONObject;

public class RemoteMutableEdge extends MutableEdge {
  protected final RemoteDatabase remoteDatabase;
  protected final String         typeName;

  protected RemoteMutableEdge(final RemoteImmutableEdge source) {
    super(null, null, source.getIdentity());
    this.remoteDatabase = source.remoteDatabase;
    this.typeName = source.typeName;
    this.map.putAll(source.map);
    this.out = source.getOut();
    this.in = source.getIn();
  }

  @Override
  public String getTypeName() {
    return typeName;
  }

  @Override
  public synchronized MutableEdge save() {
    dirty = true;
    if (rid != null)
      remoteDatabase.command("sql", "update " + rid + " content " + toJSON());
    else
      remoteDatabase.command("sql", "insert into " + typeName + " content " + toJSON());
    dirty = false;
    return this;
  }

  @Override
  public synchronized MutableEdge save(final String bucketName) {
    dirty = true;
    if (rid != null)
      throw new IllegalStateException("Cannot update a record in a custom bucket");
    remoteDatabase.command("sql", "insert into " + typeName + " bucket " + bucketName + " content " + toJSON());
    dirty = false;
    return this;
  }

  @Override
  public void delete() {
    remoteDatabase.deleteRecord(this);
  }

  @Override
  public synchronized void reload() {
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
  public synchronized JSONObject toJSON(final boolean includeMetadata) {
    final JSONObject result = new JSONSerializer(database).map2json(map);
    if (includeMetadata) {
      result.put("@cat", "e");
      result.put("@type", typeName);
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
  public synchronized void setBuffer(final Binary buffer) {
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

  @Override
  protected void checkForUpgradeLightWeight() {
    // NO ACTIONS
  }
}
