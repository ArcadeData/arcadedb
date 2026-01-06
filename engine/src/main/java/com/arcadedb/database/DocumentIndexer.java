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
import java.util.Map;

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
    final String propertyName = propertyNames[listPropertyIndex];

    // Check if this is a nested property path (e.g., "tags.id")
    final String[] pathParts = propertyName.split("\\.");
    final String listPropertyName = pathParts[0];

    // Get the list property value (just the root property, e.g., "tags")
    final Object listValue = record.get(listPropertyName);

    if (listValue == null) {
      // Null list - no index entries
      return;
    }

    if (!(listValue instanceof List<?> list)) {
      throw new IndexException("Property '" + listPropertyName +
          "' is indexed with BY ITEM but is not a LIST type");
    }

    if (list.isEmpty()) {
      // Empty list - no index entries
      return;
    }

    // Create one index entry for EACH list element
    for (final Object listItem : list) {
      final Object[] keyValues = new Object[propertyNames.length];

      for (int i = 0; i < keyValues.length; ++i) {
        if (i == listPropertyIndex) {
          // For nested paths (e.g., "tags.id"), extract the nested property from the list item
          if (pathParts.length > 1) {
            // Build the nested path without the first part (e.g., "id" from "tags.id")
            final StringBuilder nestedPath = new StringBuilder();
            for (int j = 1; j < pathParts.length; j++) {
              if (j > 1)
                nestedPath.append(".");
              nestedPath.append(pathParts[j]);
            }

            // Extract the nested property value from the list item
            Object nestedValue = listItem;
            for (int j = 1; j < pathParts.length; j++) {
              if (nestedValue == null) {
                break;
              }
              if (nestedValue instanceof Document doc) {
                nestedValue = doc.get(pathParts[j]);
              } else if (nestedValue instanceof Map map) {
                nestedValue = map.get(pathParts[j]);
              } else {
                nestedValue = null;
                break;
              }
            }
            keyValues[i] = nestedValue;
          } else {
            // Simple property - use the list item directly
            keyValues[i] = listItem;
          }
        } else {
          // Use normal property value for other properties
          keyValues[i] = getPropertyValue(record, propertyNames[i]);
        }
      }

      // Only index if we have a valid key value
      if (keyValues[listPropertyIndex] != null) {
        entry.put(keyValues, new RID[] { rid });
      }
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
    final String propertyName = propertyNames[listPropertyIndex];
    final String[] pathParts = propertyName.split("\\.");
    final String listPropertyName = pathParts[0];

    // Get the list values from both records
    final Object oldListValue = originalRecord.get(listPropertyName);
    final Object newListValue = modifiedRecord.get(listPropertyName);

    final List<?> oldList = (oldListValue instanceof List) ? (List<?>) oldListValue : List.of();
    final List<?> newList = (newListValue instanceof List) ? (List<?>) newListValue : List.of();

    // For nested paths, we need to compare the extracted values, not the objects themselves
    final boolean isNested = pathParts.length > 1;

    if (isNested) {
      // Build a helper to extract nested values
      java.util.function.Function<Object, Object> extractValue = (item) -> {
        Object value = item;
        for (int j = 1; j < pathParts.length; j++) {
          if (value == null)
            break;
          if (value instanceof Document doc) {
            value = doc.get(pathParts[j]);
          } else if (value instanceof Map<?, ?> map) {
            value = map.get(pathParts[j]);
          } else {
            value = null;
            break;
          }
        }
        return value;
      };

      // Collect old and new values
      final List<Object> oldValues = new ArrayList<>();
      for (Object item : oldList) {
        final Object val = extractValue.apply(item);
        if (val != null)
          oldValues.add(val);
      }

      final List<Object> newValues = new ArrayList<>();
      for (Object item : newList) {
        final Object val = extractValue.apply(item);
        if (val != null)
          newValues.add(val);
      }

      // Remove entries for values that are no longer in the list
      for (final Object oldValue : oldValues) {
        if (!newValues.contains(oldValue)) {
          final Object[] keyValues = new Object[propertyNames.length];
          for (int i = 0; i < keyValues.length; ++i) {
            if (i == listPropertyIndex) {
              keyValues[i] = oldValue;
            } else {
              keyValues[i] = getPropertyValue(originalRecord, propertyNames[i]);
            }
          }
          index.remove(keyValues, rid);
        }
      }

      // Add entries for new values
      for (final Object newValue : newValues) {
        if (!oldValues.contains(newValue)) {
          final Object[] keyValues = new Object[propertyNames.length];
          for (int i = 0; i < keyValues.length; ++i) {
            if (i == listPropertyIndex) {
              keyValues[i] = newValue;
            } else {
              keyValues[i] = getPropertyValue(modifiedRecord, propertyNames[i]);
            }
          }
          index.put(keyValues, new RID[] { rid });
        }
      }
    } else {
      // Simple list items - use direct comparison
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

      // Reset dirty flag AFTER properties have been read for index deletion
      if (record instanceof RecordInternal internal)
        internal.unsetDirty();
    }
  }

  private void deleteListItemsFromIndex(final Index index, final Document record,
      final String[] propertyNames, final int listPropertyIndex) {
    final String propertyName = propertyNames[listPropertyIndex];
    final String[] pathParts = propertyName.split("\\.");
    final String listPropertyName = pathParts[0];

    final Object listValue = record.get(listPropertyName);

    if (listValue == null || !(listValue instanceof List)) {
      return;
    }

    final List<?> list = (List<?>) listValue;

    // Remove index entry for EACH list item
    for (final Object listItem : list) {
      final Object[] keyValues = new Object[propertyNames.length];

      for (int i = 0; i < keyValues.length; ++i) {
        if (i == listPropertyIndex) {
          // For nested paths, extract the nested property value
          if (pathParts.length > 1) {
            Object nestedValue = listItem;
            for (int j = 1; j < pathParts.length; j++) {
              if (nestedValue == null)
                break;
              if (nestedValue instanceof Document doc) {
                nestedValue = doc.get(pathParts[j]);
              } else if (nestedValue instanceof Map map) {
                nestedValue = map.get(pathParts[j]);
              } else {
                nestedValue = null;
                break;
              }
            }
            keyValues[i] = nestedValue;
          } else {
            keyValues[i] = listItem;
          }
        } else {
          keyValues[i] = getPropertyValue(record, propertyNames[i]);
        }
      }

      // Only remove if we have a valid key value
      if (keyValues[listPropertyIndex] != null) {
        index.remove(keyValues, record.getIdentity());
      }
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

    // First, try to get the property with the exact name (handles properties with dots in their names)
    if (record.has(propertyName)) {
      return record.get(propertyName);
    }

    // If property doesn't exist and contains a dot, try as nested path
    if (propertyName.contains(".")) {
      return getNestedPropertyValue(record, propertyName);
    }

    return null;
  }

  /**
   * Retrieves a nested property value using dot notation path.
   * For example, "tags.id" would get the property 'id' from the value of property 'tags'.
   * This is only called when the property with the full name doesn't exist.
   *
   * @param record       The document to extract the property from
   * @param propertyPath The dot-separated property path (e.g., "tags.id")
   *
   * @return The nested property value, or null if any part of the path is null
   */
  private Object getNestedPropertyValue(final Document record, final String propertyPath) {
    final String[] pathParts = propertyPath.split("\\.", 2); // Split into at most 2 parts
    Object current = record.get(pathParts[0]);

    if (current == null || pathParts.length == 1) {
      return current;
    }

    // Continue with the rest of the path
    return getNestedValue(current, pathParts[1]);
  }

  /**
   * Helper method to recursively get nested values from objects.
   */
  private Object getNestedValue(Object current, String path) {
    if (current == null) {
      return null;
    }

    final String[] pathParts = path.split("\\.", 2);
    final String currentPart = pathParts[0];

    if (current instanceof Document doc) {
      current = doc.get(currentPart);
    } else if (current instanceof Map map) {
      current = map.get(currentPart);
    } else {
      // Cannot traverse further - not a document or map
      return null;
    }

    // If there are more parts, recurse
    if (pathParts.length > 1 && current != null) {
      return getNestedValue(current, pathParts[1]);
    }

    return current;
  }
}
