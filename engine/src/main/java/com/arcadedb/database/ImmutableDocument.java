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
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.serializer.json.JSONObject;

import java.io.*;
import java.lang.reflect.*;
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
    return database.getSerializer().hasProperty(database, buffer, propertyName);
  }

  @Override
  public synchronized Object get(final String propertyName) {
    if (propertyName == null)
      return null;

    checkForLazyLoading();
    return database.getSerializer()
        .deserializeProperty(database, buffer, new EmbeddedModifierProperty(this, propertyName), propertyName, type);
  }

  @Override
  public synchronized MutableDocument modify() {
    final Record recordInCache = database.getTransaction().getRecordFromCache(rid);
    if (recordInCache != null) {
      if (recordInCache instanceof MutableDocument)
        return (MutableDocument) recordInCache;
      else if (!database.getTransaction().hasPageForRecord(rid.getPageId())) {
        // THE RECORD IS NOT IN TX, SO IT MUST HAVE BEEN LOADED WITHOUT A TX OR PASSED FROM ANOTHER TX
        // IT MUST BE RELOADED TO GET THE LATEST CHANGES. FORCE RELOAD
        try {
          // RELOAD THE PAGE FIRST TO AVOID LOOP WITH TRIGGERS (ENCRYPTION)
          database.getTransaction()
              .getPageToModify(rid.getPageId(), database.getSchema().getBucketById(rid.getBucketId()).getPageSize(), false);
          reload();
        } catch (final IOException e) {
          throw new DatabaseOperationException("Error on reloading document " + rid, e);
        }
      }
    }

    checkForLazyLoading();
    buffer.rewind();
    return new MutableDocument(database, type, rid, buffer.copyOfContent());
  }

  @Override
  public synchronized JSONObject toJSON(final boolean includeMetadata) {
    checkForLazyLoading();
    final Map<String, Object> map = database.getSerializer()
        .deserializeProperties(database, buffer, new EmbeddedModifierObject(this), type);
    final JSONObject result = new JSONSerializer(database).map2json(map);
    if (includeMetadata) {
      result.put("@cat", "d");
      result.put("@type", type.getName());
      if (getIdentity() != null)
        result.put("@rid", getIdentity().toString());
    }
    return result;
  }

  @Override
  public Map<String, Object> propertiesAsMap() {
    if (database == null || buffer == null)
      return Collections.emptyMap();
    buffer.position(propertiesStartingPosition);
    return database.getSerializer().deserializeProperties(database, buffer, new EmbeddedModifierObject(this), type);
  }

  @Override
  public synchronized Map<String, Object> toMap() {
    return toMap(true);
  }

  @Override
  public synchronized Map<String, Object> toMap(final boolean includeMetadata) {
    checkForLazyLoading();
    final Map<String, Object> result = database.getSerializer()
        .deserializeProperties(database, buffer, new EmbeddedModifierObject(this), type);
    if (includeMetadata) {
      result.put("@cat", "d");
      result.put("@type", type.getName());
      if (getIdentity() != null)
        result.put("@rid", getIdentity().toString());
    }
    return result;
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
      final Map<String, Object> map = this.database.getSerializer()
          .deserializeProperties(database, buffer, new EmbeddedModifierObject(this), type);

      buffer.position(currPosition);

      int i = 0;
      for (final Map.Entry<String, Object> entry : map.entrySet()) {
        if (i > 0)
          output.append(',');

        output.append(entry.getKey());
        output.append('=');

        final Object v = entry.getValue();
        if (v != null && v.getClass().isArray()) {
          output.append('[');
          output.append(Array.getLength(v));
          output.append(']');
        } else
          output.append(v);
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

      final Record loaded = database.invokeAfterReadEvents(this);
      if (loaded == null) {
        buffer = null;
        return false;
      } else if (loaded != this) {
        // CREATE A BUFFER FROM THE MODIFIED RECORD. THIS IS NEEDED FOR ENCRYPTION THAT UPDATE THE RECORD WITH A MUTABLE
        buffer = database.getSerializer().serialize(database, loaded);
      }

      return true;
    }

    buffer.position(propertiesStartingPosition);
    return false;
  }
}
