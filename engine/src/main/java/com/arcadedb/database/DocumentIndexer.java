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

import java.util.ArrayList;
import java.util.List;

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

    // Check if any property has "by item" modifier
    boolean hasListIndexing = false;
    int listPropertyIndex = -1;
    String[] propertyNamesArray = new String[keyNames.size()];

    for (int i = 0; i < keyNames.size(); ++i) {
      final String keyName = keyNames.get(i);

      if (keyName.endsWith(" by item")) {
        hasListIndexing = true;
        listPropertyIndex = i;
        // Extract actual property name (remove " by item" suffix)
        propertyNamesArray[i] = keyName.substring(0, keyName.length() - 8);
      } else {
        propertyNamesArray[i] = keyName;
      }
    }

    if (!hasListIndexing) {
      // Standard indexing - single entry per document
      final Object[] keyValues = new Object[keyNames.size()];
      for (int i = 0; i < keyValues.length; ++i)
        keyValues[i] = getPropertyValue(record, propertyNamesArray[i]);
      entry.put(keyValues, new RID[] { rid });
    } else {
      // List indexing - one entry per list element
      addListItemsToIndex(entry, rid, record, propertyNamesArray, listPropertyIndex);
    }
  }

  private void addListItemsToIndex(final Index entry, final RID rid, final Document record,
                                    final String[] propertyNames, final int listPropertyIndex) {
    // Get the list property value
    final Object listValue = getPropertyValue(record, propertyNames[listPropertyIndex]);

    if (listValue == null) {
      // Null list - no index entries
      return;
    }

    if (!(listValue instanceof List)) {
      throw new IndexException("Property '" + propertyNames[listPropertyIndex] +
          "' is indexed with BY ITEM but is not a LIST type");
    }

    final List<?> list = (List<?>) listValue;

    if (list.isEmpty()) {
      // Empty list - no index entries
      return;
    }

    // Create one index entry for EACH list element
    for (final Object listItem : list) {
      final Object[] keyValues = new Object[propertyNames.length];

      for (int i = 0; i < keyValues.length; ++i) {
        if (i == listPropertyIndex) {
          // Use the individual list item as the key value
          keyValues[i] = listItem;
        } else {
          // Use normal property value for other properties
          keyValues[i] = getPropertyValue(record, propertyNames[i]);
        }
      }

      entry.put(keyValues, new RID[] { rid });
    }
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

      // Check if any property has "by item" modifier
      boolean hasListIndexing = false;
      int listPropertyIndex = -1;
      String[] propertyNamesArray = new String[keyNames.size()];

      for (int i = 0; i < keyNames.size(); ++i) {
        final String keyName = keyNames.get(i);

        if (keyName.endsWith(" by item")) {
          hasListIndexing = true;
          listPropertyIndex = i;
          propertyNamesArray[i] = keyName.substring(0, keyName.length() - 8);
        } else {
          propertyNamesArray[i] = keyName;
        }
      }

      if (!hasListIndexing) {
        // Standard update logic (existing code)
        final Object[] oldKeyValues = new Object[keyNames.size()];
        final Object[] newKeyValues = new Object[keyNames.size()];

        boolean keyValuesAreModified = false;
        for (int i = 0; i < keyNames.size(); ++i) {
          oldKeyValues[i] = getPropertyValue(originalRecord, propertyNamesArray[i]);
          newKeyValues[i] = getPropertyValue(modifiedRecord, propertyNamesArray[i]);

          if (!keyValuesAreModified &&
              ((newKeyValues[i] == null && oldKeyValues[i] != null) ||
                  (newKeyValues[i] != null && !newKeyValues[i].equals(oldKeyValues[i])))
          ) {
            keyValuesAreModified = true;
          }
        }

        if (!keyValuesAreModified)
          // SAME VALUES, SKIP INDEX UPDATE
          continue;

        final BucketSelectionStrategy bucketSelectionStrategy = modifiedRecord.getType().getBucketSelectionStrategy();
        if (bucketSelectionStrategy instanceof PartitionedBucketSelectionStrategy strategy) {
          if (!List.of(strategy.getProperties())
              .equals(index.getPropertyNames()))
            throw new IndexException("Cannot modify primary key when the bucket selection is partitioned");
        }

        // REMOVE THE OLD ENTRY KEYS/VALUE AND INSERT THE NEW ONE
        index.remove(oldKeyValues, rid);
        index.put(newKeyValues, new RID[] { rid });
      } else {
        // List update logic - compute delta
        updateListItemsInIndex(index, rid, originalRecord, modifiedRecord, propertyNamesArray, listPropertyIndex);
      }
    }
  }

  private void updateListItemsInIndex(final Index index, final RID rid,
                                      final Document originalRecord, final Document modifiedRecord,
                                      final String[] propertyNames, final int listPropertyIndex) {
    final Object oldListValue = getPropertyValue(originalRecord, propertyNames[listPropertyIndex]);
    final Object newListValue = getPropertyValue(modifiedRecord, propertyNames[listPropertyIndex]);

    final List<?> oldList = (oldListValue instanceof List) ? (List<?>) oldListValue : List.of();
    final List<?> newList = (newListValue instanceof List) ? (List<?>) newListValue : List.of();

    // Remove entries for items that are no longer in the list
    for (final Object oldItem : oldList) {
      if (!newList.contains(oldItem)) {
        final Object[] keyValues = new Object[propertyNames.length];
        for (int i = 0; i < keyValues.length; ++i) {
          if (i == listPropertyIndex) {
            keyValues[i] = oldItem;
          } else {
            keyValues[i] = getPropertyValue(originalRecord, propertyNames[i]);
          }
        }
        index.remove(keyValues, rid);
      }
    }

    // Add entries for new items
    for (final Object newItem : newList) {
      if (!oldList.contains(newItem)) {
        final Object[] keyValues = new Object[propertyNames.length];
        for (int i = 0; i < keyValues.length; ++i) {
          if (i == listPropertyIndex) {
            keyValues[i] = newItem;
          } else {
            keyValues[i] = getPropertyValue(modifiedRecord, propertyNames[i]);
          }
        }
        index.put(keyValues, new RID[] { rid });
      }
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
      if (record instanceof RecordInternal internal)
        // FORCE RESET OF ANY PROPERTY TEMPORARY SET
        internal.unsetDirty();

      final List<IndexInternal> allIndexes = new ArrayList(metadata);
      for (final IndexInternal index : metadata) {
        final IndexInternal assIndex = index.getAssociatedIndex();
        if (assIndex != null)
          allIndexes.add(assIndex);
      }

      for (final IndexInternal index : allIndexes) {
        final List<String> keyNames = index.getPropertyNames();

        // Check if any property has "by item" modifier
        boolean hasListIndexing = false;
        int listPropertyIndex = -1;
        String[] propertyNamesArray = new String[keyNames.size()];

        for (int i = 0; i < keyNames.size(); ++i) {
          final String keyName = keyNames.get(i);

          if (keyName.endsWith(" by item")) {
            hasListIndexing = true;
            listPropertyIndex = i;
            propertyNamesArray[i] = keyName.substring(0, keyName.length() - 8);
          } else {
            propertyNamesArray[i] = keyName;
          }
        }

        if (!hasListIndexing) {
          // Standard deletion
          final Object[] keyValues = new Object[keyNames.size()];
          for (int i = 0; i < keyNames.size(); ++i) {
            keyValues[i] = getPropertyValue(record, propertyNamesArray[i]);
          }
          index.remove(keyValues, record.getIdentity());
        } else {
          // Delete all list item entries
          deleteListItemsFromIndex(index, record, propertyNamesArray, listPropertyIndex);
        }
      }
    }
  }

  private void deleteListItemsFromIndex(final Index index, final Document record,
                                        final String[] propertyNames, final int listPropertyIndex) {
    final Object listValue = getPropertyValue(record, propertyNames[listPropertyIndex]);

    if (listValue == null || !(listValue instanceof List)) {
      return;
    }

    final List<?> list = (List<?>) listValue;

    // Remove index entry for EACH list item
    for (final Object listItem : list) {
      final Object[] keyValues = new Object[propertyNames.length];

      for (int i = 0; i < keyValues.length; ++i) {
        if (i == listPropertyIndex) {
          keyValues[i] = listItem;
        } else {
          keyValues[i] = getPropertyValue(record, propertyNames[i]);
        }
      }

      index.remove(keyValues, record.getIdentity());
    }
  }

  private Object getPropertyValue(final Document record, final String propertyName) {
    if (record instanceof Edge edge) {
      // EDGE: CHECK FOR SPECIAL CASES @OUT AND @IN
      if ("@out".equals(propertyName))
        return edge.getOut();
      else if ("@in".equals(propertyName))
        return edge.getIn();
    }
    return record.get(propertyName);
  }
}
