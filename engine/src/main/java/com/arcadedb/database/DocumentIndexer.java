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

import com.arcadedb.database.bucketselectionstrategy.BucketSelectionStrategy;
import com.arcadedb.database.bucketselectionstrategy.PartitionedBucketSelectionStrategy;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.graph.Edge;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.schema.DocumentType;

import java.util.*;

public class DocumentIndexer {
  private final LocalDatabase database;

  protected DocumentIndexer(final LocalDatabase database) {
    this.database = database;
  }

  public List<IndexInternal> getInvolvedIndexes(final Document modifiedRecord) {
    if (modifiedRecord == null)
      throw new IllegalArgumentException("Modified record is null");

    final RID rid = modifiedRecord.getIdentity();
    if (rid == null)
      // RECORD IS NOT PERSISTENT
      return null;

    final int bucketId = rid.getBucketId();

    return modifiedRecord.getType().getPolymorphicBucketIndexByBucketId(bucketId, null);
  }

  public void createDocument(final Document record, final DocumentType type, final LocalBucket bucket) {
    final RID rid = record.getIdentity();
    if (rid == null)
      throw new IllegalArgumentException("Cannot index a non persistent record");

    // INDEX THE RECORD
    final List<IndexInternal> metadata = type.getPolymorphicBucketIndexByBucketId(bucket.getFileId(), null);
    for (final Index entry : metadata)
      addToIndex(entry, rid, record);
  }

  public void addToIndex(final Index entry, final RID rid, final Document record) {
    final List<String> keyNames = entry.getPropertyNames();

    final Object[] keyValues = new Object[keyNames.size()];
    for (int i = 0; i < keyValues.length; ++i)
      keyValues[i] = getPropertyValue(record, keyNames.get(i));

    entry.put(keyValues, new RID[] { rid });
  }

  public void updateDocument(final Document originalRecord, final Document modifiedRecord, final List<IndexInternal> indexes) {
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

    for (final Index index : indexes) {
      final List<String> keyNames = index.getPropertyNames();
      final Object[] oldKeyValues = new Object[keyNames.size()];
      final Object[] newKeyValues = new Object[keyNames.size()];

      boolean keyValuesAreModified = false;
      for (int i = 0; i < keyNames.size(); ++i) {
        oldKeyValues[i] = getPropertyValue(originalRecord, keyNames.get(i));
        newKeyValues[i] = getPropertyValue(modifiedRecord, keyNames.get(i));

        if (!keyValuesAreModified &&//
            ((newKeyValues[i] == null && oldKeyValues[i] != null) || (newKeyValues[i] != null && !newKeyValues[i].equals(
                oldKeyValues[i])))) {
          keyValuesAreModified = true;
        }
      }

      if (!keyValuesAreModified)
        // SAME VALUES, SKIP INDEX UPDATE
        continue;

      final BucketSelectionStrategy bucketSelectionStrategy = modifiedRecord.getType().getBucketSelectionStrategy();
      if (bucketSelectionStrategy instanceof PartitionedBucketSelectionStrategy) {
        if (!List.of(((PartitionedBucketSelectionStrategy) bucketSelectionStrategy).getProperties())
            .equals(index.getPropertyNames()))
          throw new IndexException("Cannot modify primary key when the bucket selection is partitioned");
      }

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

    final List<IndexInternal> metadata = type.getPolymorphicBucketIndexByBucketId(bucketId, null);
    if (metadata != null && !metadata.isEmpty()) {
      if (record instanceof RecordInternal)
        // FORCE RESET OF ANY PROPERTY TEMPORARY SET
        ((RecordInternal) record).unsetDirty();

      final List<IndexInternal> allIndexes = new ArrayList(metadata);
      for (final IndexInternal index : metadata) {
        final IndexInternal assIndex = index.getAssociatedIndex();
        if (assIndex != null)
          allIndexes.add(assIndex);
      }

      for (final IndexInternal index : allIndexes) {
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
