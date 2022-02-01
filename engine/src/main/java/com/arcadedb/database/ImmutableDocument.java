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
package com.arcadedb.database;

import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.schema.DocumentType;
import org.json.JSONObject;

import java.util.*;

/**
 * Immutable document implementation. To modify the record, you need to get the mutable representation by calling {@link #modify()}. This implementation keeps the
 * information in a byte[] to reduce the amount of objects to be managed by the Garbage Collector. For recurrent access to the record property you could evaluate
 * to return the mutable version of it that is backed by an internal map where the record properties are cached in RAM.
 *
 * @author Luca Garulli
 */
public class ImmutableDocument extends BaseDocument {

  protected ImmutableDocument(final Database graph, final DocumentType type, final RID rid, final Binary buffer) {
    super(graph, type, rid, buffer);
  }

  @Override
  public synchronized boolean has(final String propertyName) {
    if (propertyName == null)
      return false;

    checkForLazyLoading();
    final Map<String, Object> map = database.getSerializer()
        .deserializeProperties(database, buffer, new EmbeddedModifierProperty(this, propertyName), propertyName);
    return map.containsKey(propertyName);
  }

  @Override
  public synchronized Object get(final String propertyName) {
    if (propertyName == null)
      return null;

    checkForLazyLoading();
    final Map<String, Object> map = database.getSerializer()
        .deserializeProperties(database, buffer, new EmbeddedModifierProperty(this, propertyName), propertyName);
    return map.get(propertyName);
  }

  @Override
  public synchronized MutableDocument modify() {
    final Record recordInCache = database.getTransaction().getRecordFromCache(rid);
    if (recordInCache instanceof MutableDocument)
      return (MutableDocument) recordInCache;

    checkForLazyLoading();
    buffer.rewind();
    return new MutableDocument(database, type, rid, buffer.copy());
  }

  @Override
  public synchronized JSONObject toJSON() {
    checkForLazyLoading();
    final Map<String, Object> map = database.getSerializer().deserializeProperties(database, buffer, new EmbeddedModifierObject(this));

    final JSONObject result = new JSONSerializer(database).map2json(map);
    result.put("@type", type.getName());
    if (getIdentity() != null)
      result.put("@rid", getIdentity().toString());
    return result;
  }

  @Override
  public synchronized Map<String, Object> toMap() {
    checkForLazyLoading();
    return database.getSerializer().deserializeProperties(database, buffer, new EmbeddedModifierObject(this));
  }

  @Override
  public synchronized String toString() {
    final StringBuilder output = new StringBuilder(256);
    if (rid != null)
      output.append(rid);
    output.append('[');
    if (buffer == null)
      output.append('?');
    else {
      final int currPosition = buffer.position();

      buffer.position(propertiesStartingPosition);
      final Map<String, Object> map = this.database.getSerializer().deserializeProperties(database, buffer, new EmbeddedModifierObject(this));

      buffer.position(currPosition);

      int i = 0;
      for (Map.Entry<String, Object> entry : map.entrySet()) {
        if (i > 0)
          output.append(',');

        output.append(entry.getKey());
        output.append('=');
        output.append(entry.getValue());
        i++;
      }
    }
    output.append(']');
    return output.toString();
  }

  @Override
  public synchronized Set<String> getPropertyNames() {
    checkForLazyLoading();
    return database.getSerializer().getPropertyNames(database, buffer);
  }

  protected boolean checkForLazyLoading() {
    if (buffer == null) {
      if (rid == null)
        throw new DatabaseOperationException("Document cannot be loaded because RID is null");

      buffer = database.getSchema().getBucketById(rid.getBucketId()).getRecord(rid);
      buffer.position(propertiesStartingPosition);
      return true;
    }

    buffer.position(propertiesStartingPosition);
    return false;
  }
}
