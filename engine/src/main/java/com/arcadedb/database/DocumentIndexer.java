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

import com.arcadedb.engine.Bucket;
import com.arcadedb.graph.Edge;
import com.arcadedb.index.Index;
import com.arcadedb.schema.DocumentType;

import java.util.List;

public class DocumentIndexer {
  private final EmbeddedDatabase database;

  protected DocumentIndexer(final EmbeddedDatabase database) {
    this.database = database;
  }

  public List<Index> getInvolvedIndexes(final Document modifiedRecord) {
    if (modifiedRecord == null)
      throw new IllegalArgumentException("Modified record is null");

    final RID rid = modifiedRecord.getIdentity();
    if (rid == null)
      // RECORD IS NOT PERSISTENT
      return null;

    final int bucketId = rid.getBucketId();

    return modifiedRecord.getType().getPolymorphicBucketIndexByBucketId(bucketId);
  }

  public void createDocument(final Document record, final DocumentType type, final Bucket bucket) {
    final RID rid = record.getIdentity();
    if (rid == null)
      throw new IllegalArgumentException("Cannot index a non persistent record");

    // INDEX THE RECORD
    final List<Index> metadata = type.getPolymorphicBucketIndexByBucketId(bucket.getId());
    for (Index entry : metadata)
      addToIndex(entry, rid, record);
  }

  public void addToIndex(final Index entry, final RID rid, final Document record) {
    final Index index = entry;
    final List<String> keyNames = entry.getPropertyNames();

    final Object[] keyValues = new Object[keyNames.size()];
    for (int i = 0; i < keyValues.length; ++i)
      keyValues[i] = getPropertyValue(record, keyNames.get(i));

    index.put(keyValues, new RID[] { rid });
  }

  public void updateDocument(final Document originalRecord, final Document modifiedRecord, final List<Index> indexes) {
    if (indexes == null || indexes.isEmpty())
      return;

    if (originalRecord == null)
      throw new IllegalArgumentException("Original record is null");
    if (modifiedRecord == null)
      throw new IllegalArgumentException("Modified record is null");

    final RID rid = modifiedRecord.getIdentity();
    if (rid == null)
      // RECORD IS NOT PERSISTENT
      return;

    for (Index index : indexes) {
      final List<String> keyNames = index.getPropertyNames();
      final Object[] oldKeyValues = new Object[keyNames.size()];
      final Object[] newKeyValues = new Object[keyNames.size()];

      boolean keyValuesAreModified = false;
      for (int i = 0; i < keyNames.size(); ++i) {
        oldKeyValues[i] = getPropertyValue(originalRecord, keyNames.get(i));
        newKeyValues[i] = getPropertyValue(modifiedRecord, keyNames.get(i));

        if ((newKeyValues[i] == null && oldKeyValues[i] != null) || (newKeyValues[i] != null && !newKeyValues[i].equals(oldKeyValues[i]))) {
          keyValuesAreModified = true;
          break;
        }
      }

      if (!keyValuesAreModified)
        // SAME VALUES, SKIP INDEX UPDATE
        continue;

      // REMOVE THE OLD ENTRY KEYS/VALUE AND INSERT THE NEW ONE
      index.remove(oldKeyValues, rid);
      index.put(newKeyValues, new RID[] { rid });
    }
  }

  public void deleteDocument(final Document record) {
    if (record.getIdentity() == null)
      // RECORD IS NOT PERSISTENT
      return;

    final int bucketId = record.getIdentity().getBucketId();

    final DocumentType type = database.getSchema().getTypeByBucketId(bucketId);
    if (type == null)
      throw new IllegalStateException("Type not found for bucket " + bucketId);

    final List<Index> metadata = type.getPolymorphicBucketIndexByBucketId(bucketId);
    if (metadata != null && !metadata.isEmpty()) {
      if (record instanceof RecordInternal)
        // FORCE RESET OF ANY PROPERTY TEMPORARY SET
        ((RecordInternal) record).unsetDirty();

      for (Index index : metadata) {
        final List<String> keyNames = index.getPropertyNames();
        final Object[] keyValues = new Object[keyNames.size()];
        for (int i = 0; i < keyNames.size(); ++i) {
          keyValues[i] = getPropertyValue(record, keyNames.get(i));
        }

        index.remove(keyValues, record.getIdentity());
      }
    }
  }

  private Object getPropertyValue(final Document record, final String propertyName) {
    if (record instanceof Edge) {
      // EDGE: CHECK FOR SPECIAL CASES @OUT AND @IN
      if ("@out".equals(propertyName))
        return ((Edge) record).getOut();
      else if ("@in".equals(propertyName))
        return ((Edge) record).getIn();
    }
    return record.get(propertyName);
  }
}
