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
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class DocumentIndexer {
  private final LocalDatabase database;

  /**
   * How a single index property must be expanded at write time: a plain property produces exactly one
   * index entry, while collection modifiers expand into one entry per element.
   */
  private enum KeyExpansion {
    NONE, LIST_ITEM, MAP_KEY, MAP_VALUE
  }

  private static KeyExpansion detectExpansion(final String keyName) {
    if (keyName.endsWith(" by item"))
      return KeyExpansion.LIST_ITEM;
    if (keyName.endsWith(" by key"))
      return KeyExpansion.MAP_KEY;
    if (keyName.endsWith(" by value"))
      return KeyExpansion.MAP_VALUE;
    return KeyExpansion.NONE;
  }

  private static String stripModifier(final String keyName, final KeyExpansion expansion) {
    return switch (expansion) {
      case LIST_ITEM -> keyName.substring(0, keyName.length() - 8);
      case MAP_KEY -> keyName.substring(0, keyName.length() - 7);
      case MAP_VALUE -> keyName.substring(0, keyName.length() - 9);
      case NONE -> keyName;
    };
  }

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

    // Detect a collection modifier ("by item" for LIST, "by key"/"by value" for MAP)
    KeyExpansion expansion = KeyExpansion.NONE;
    int expansionIndex = -1;
    final String[] propertyNamesArray = new String[keyNames.size()];

    for (int i = 0; i < keyNames.size(); ++i) {
      final String keyName = keyNames.get(i);
      final KeyExpansion e = detectExpansion(keyName);
      if (e != KeyExpansion.NONE) {
        expansion = e;
        expansionIndex = i;
        propertyNamesArray[i] = stripModifier(keyName, e);
      } else {
        propertyNamesArray[i] = keyName;
      }
    }

    if (expansion == KeyExpansion.NONE) {
      // Standard indexing - single entry per document
      final Object[] keyValues = new Object[keyNames.size()];
      for (int i = 0; i < keyValues.length; ++i)
        keyValues[i] = getPropertyValue(record, propertyNamesArray[i]);
      entry.put(keyValues, new RID[] { rid });
    } else if (expansion == KeyExpansion.LIST_ITEM) {
      // List indexing - one entry per list element
      addListItemsToIndex(entry, rid, record, propertyNamesArray, expansionIndex);
    } else {
      // Map indexing - one entry per key or per value
      addMapEntriesToIndex(entry, rid, record, propertyNamesArray, expansionIndex, expansion);
    }
  }

  private void addMapEntriesToIndex(final Index entry, final RID rid, final Document record,
      final String[] propertyNames, final int mapPropertyIndex, final KeyExpansion expansion) {
    for (final Object element : mapElements(record, propertyNames[mapPropertyIndex], expansion)) {
      final Object[] keyValues = new Object[propertyNames.length];
      for (int i = 0; i < keyValues.length; ++i)
        keyValues[i] = i == mapPropertyIndex ? element : getPropertyValue(record, propertyNames[i]);
      entry.put(keyValues, new RID[] { rid });
    }
  }

  /**
   * Returns the distinct keys or values of a MAP property to be indexed. Duplicate values (a value
   * shared by several keys) are collapsed so a record is not indexed more than once for the same key.
   */
  private Collection<Object> mapElements(final Document record, final String propertyName, final KeyExpansion expansion) {
    final Object mapValue = record.get(propertyName);
    if (mapValue == null)
      return List.of();

    if (!(mapValue instanceof Map<?, ?> map))
      throw new IndexException("Property '" + propertyName + "' is indexed with BY "
          + (expansion == KeyExpansion.MAP_KEY ? "KEY" : "VALUE") + " but is not a MAP type");

    final Collection<?> source = expansion == KeyExpansion.MAP_KEY ? map.keySet() : map.values();
    final LinkedHashSet<Object> elements = new LinkedHashSet<>(source.size());
    for (final Object element : source)
      if (element != null)
        elements.add(element);
    return elements;
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

  /**
   * Defensive copy for a snapshot value: scalar values are immutable and stored as-is; containers are
   * shallow-copied and embedded documents detached, so a later in-place mutation of the live value cannot
   * retroactively change the snapshot and hide an index delta.
   * <p>
   * The copy is intentionally ONE level deep: a nested container element (e.g. a Map inside a List) is stored
   * by reference, because indexed values are scalars or flat lists/maps - a nested container cannot be an
   * index key. Deep-copying would tax every snapshot for a shape the diff never compares.
   */
  private static Object copyForSnapshot(final Object value) {
    if (value instanceof List<?> list) {
      final List<Object> copy = new ArrayList<>(list.size());
      for (final Object element : list)
        copy.add(element instanceof EmbeddedDocument embedded ? embedded.detach() : element);
      return copy;
    }
    if (value instanceof Map<?, ?> map) {
      final Map<Object, Object> copy = new LinkedHashMap<>(map.size());
      for (final Map.Entry<?, ?> entry : map.entrySet())
        copy.put(entry.getKey(), entry.getValue() instanceof EmbeddedDocument embedded ? embedded.detach() : entry.getValue());
      return copy;
    }
    if (value instanceof EmbeddedDocument embedded)
      return embedded.detach();
    if (value instanceof Set<?> set) {
      // A Set is not an expected indexed shape (the serializer produces scalar/List/Map), but copy it anyway
      // rather than store it aliased: an in-place mutation of an aliased snapshot value would hide an index
      // delta. LinkedHashSet preserves Set.equals semantics for the diff.
      final Set<Object> copy = new LinkedHashSet<>(set.size());
      for (final Object element : set)
        copy.add(element instanceof EmbeddedDocument embedded ? embedded.detach() : element);
      return copy;
    }
    if (value instanceof Object[] array)
      // Same reasoning for arrays: unexpected as an indexed value, but never stored aliased. (Array equals is
      // identity-based, so the diff already treats any deserialized array as changed; the copy doesn't worsen that.)
      return Arrays.copyOf(array, array.length);
    // Remaining values (String, numbers, dates, RIDs) are immutable: store by reference.
    return value;
  }

  /**
   * @return a refreshed indexed-state snapshot of {@code modifiedRecord} when at least one index entry was
   *     actually removed/added, or {@code null} when every involved index saw identical key values. The caller
   *     stores the snapshot as the diff source for the NEXT update of the same record inside the same
   *     transaction (issue #4935); on {@code null} the previous diff source (committed buffer or an earlier
   *     snapshot) still describes the indexed state, so updates that only touch non-indexed properties pay no
   *     snapshot cost at all. The snapshot is built here, reusing the key values this method already extracted
   *     for the diff, so the common scalar-index case pays no second property extraction. It stores ONLY the
   *     indexed property values ({@link Document#detach()} would copy the whole document), keyed by the same
   *     names this method reads: the stripped key name (nested paths eagerly resolved) and, for LIST BY ITEM,
   *     the root property read directly by the list delta logic. Container values are shallow-copied (embedded
   *     documents detached) so the snapshot cannot alias state the caller later mutates in place.
   */
  public Document updateDocument(final Document originalRecord, final Document modifiedRecord, final List<IndexInternal> indexes) {
    if (indexes == null || indexes.isEmpty())
      return null;

    if (originalRecord == null)
      throw new IllegalArgumentException("Original record is null");
    if (modifiedRecord == null)
      throw new IllegalArgumentException("Modified record is null");

    final RID rid = modifiedRecord.getIdentity();
    if (rid == null)
      // RECORD IS NOT PERSISTENT
      return null;

    final int totalIndexes = indexes.size();
    // Retained per index for the snapshot build below, so the values extracted for the diff are reused.
    final String[][] indexPropertyNames = new String[totalIndexes][];
    final Object[][] indexNewKeyValues = new Object[totalIndexes][];
    final KeyExpansion[] indexExpansions = new KeyExpansion[totalIndexes];
    final int[] indexExpansionPositions = new int[totalIndexes];

    boolean anyIndexModified = false;
    for (int indexPos = 0; indexPos < totalIndexes; ++indexPos) {
      final Index index = indexes.get(indexPos);
      final List<String> keyNames = index.getPropertyNames();

      // Detect a collection modifier ("by item" for LIST, "by key"/"by value" for MAP)
      KeyExpansion expansion = KeyExpansion.NONE;
      int expansionIndex = -1;
      final String[] propertyNamesArray = new String[keyNames.size()];

      for (int i = 0; i < keyNames.size(); ++i) {
        final String keyName = keyNames.get(i);
        final KeyExpansion e = detectExpansion(keyName);
        if (e != KeyExpansion.NONE) {
          expansion = e;
          expansionIndex = i;
          propertyNamesArray[i] = stripModifier(keyName, e);
        } else {
          propertyNamesArray[i] = keyName;
        }
      }

      indexPropertyNames[indexPos] = propertyNamesArray;
      indexExpansions[indexPos] = expansion;
      indexExpansionPositions[indexPos] = expansionIndex;

      if (expansion == KeyExpansion.NONE) {
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

        indexNewKeyValues[indexPos] = newKeyValues;

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
        anyIndexModified = true;
      } else if (expansion == KeyExpansion.LIST_ITEM) {
        // List update logic - compute delta
        anyIndexModified |= updateListItemsInIndex(index, rid, originalRecord, modifiedRecord, propertyNamesArray, expansionIndex);
      } else {
        // Map update logic - compute delta on keys or values
        anyIndexModified |= updateMapEntriesInIndex(index, rid, originalRecord, modifiedRecord, propertyNamesArray,
            expansionIndex, expansion);
      }
    }

    if (!anyIndexModified)
      return null;

    // Build the refreshed snapshot from the values retained above. The snapshot must cover ALL involved
    // indexes (the next update diffs every index against it), including the ones that did not change: their
    // current values equal the previous state, so re-reading them from the modified record is correct.
    final Map<String, Object> values = new LinkedHashMap<>();
    for (int indexPos = 0; indexPos < totalIndexes; ++indexPos) {
      final String[] propertyNames = indexPropertyNames[indexPos];
      final KeyExpansion expansion = indexExpansions[indexPos];
      final int expansionIndex = indexExpansionPositions[indexPos];
      final Object[] newKeyValues = indexNewKeyValues[indexPos];

      for (int i = 0; i < propertyNames.length; ++i) {
        if (expansion != KeyExpansion.NONE && i == expansionIndex) {
          if (expansion == KeyExpansion.LIST_ITEM) {
            // The list delta logic reads the ROOT property directly (nested paths descend per element).
            final int dot = propertyNames[i].indexOf('.');
            final String root = dot > 0 ? propertyNames[i].substring(0, dot) : propertyNames[i];
            if (!values.containsKey(root))
              values.put(root, copyForSnapshot(modifiedRecord.get(root)));
          } else if (!values.containsKey(propertyNames[i]))
            // The map delta logic reads the stripped map property directly.
            values.put(propertyNames[i], copyForSnapshot(modifiedRecord.get(propertyNames[i])));
        } else if (!values.containsKey(propertyNames[i]))
          values.put(propertyNames[i], copyForSnapshot(
              newKeyValues != null ? newKeyValues[i] : getPropertyValue(modifiedRecord, propertyNames[i])));
      }
    }
    return new DetachedDocument(modifiedRecord, values);
  }

  /** @return {@code true} if at least one index entry was removed or added. */
  private boolean updateMapEntriesInIndex(final Index index, final RID rid, final Document originalRecord,
      final Document modifiedRecord, final String[] propertyNames, final int mapPropertyIndex, final KeyExpansion expansion) {
    final Collection<Object> oldElements = mapElements(originalRecord, propertyNames[mapPropertyIndex], expansion);
    final Collection<Object> newElements = mapElements(modifiedRecord, propertyNames[mapPropertyIndex], expansion);

    boolean changed = false;

    // Remove entries for elements no longer present
    for (final Object oldElement : oldElements) {
      if (!newElements.contains(oldElement)) {
        final Object[] keyValues = new Object[propertyNames.length];
        for (int i = 0; i < keyValues.length; ++i)
          keyValues[i] = i == mapPropertyIndex ? oldElement : getPropertyValue(originalRecord, propertyNames[i]);
        index.remove(keyValues, rid);
        changed = true;
      }
    }

    // Add entries for new elements
    for (final Object newElement : newElements) {
      if (!oldElements.contains(newElement)) {
        final Object[] keyValues = new Object[propertyNames.length];
        for (int i = 0; i < keyValues.length; ++i)
          keyValues[i] = i == mapPropertyIndex ? newElement : getPropertyValue(modifiedRecord, propertyNames[i]);
        index.put(keyValues, new RID[] { rid });
        changed = true;
      }
    }

    return changed;
  }

  /** @return {@code true} if at least one index entry was removed or added. */
  private boolean updateListItemsInIndex(final Index index, final RID rid,
      final Document originalRecord, final Document modifiedRecord,
      final String[] propertyNames, final int listPropertyIndex) {
    boolean changed = false;
    final String propertyName = propertyNames[listPropertyIndex];
    final String[] pathParts = propertyName.split("\\.");
    final String listPropertyName = pathParts[0];

    // Get the list values from both records
    final Object oldListValue = originalRecord.get(listPropertyName);
    final Object newListValue = modifiedRecord.get(listPropertyName);

    final List<?> oldList = oldListValue instanceof List<?> list ? list : List.of();
    final List<?> newList = newListValue instanceof List<?> list ? list : List.of();

    // For nested paths, we need to compare the extracted values, not the objects themselves
    final boolean isNested = pathParts.length > 1;

    if (isNested) {
      // Build a helper to extract nested values
      Function<Object, Object> extractValue = item -> {
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
          changed = true;
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
          changed = true;
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
          changed = true;
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
          changed = true;
        }
      }
    }

    return changed;
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

        // Detect a collection modifier ("by item" for LIST, "by key"/"by value" for MAP)
        KeyExpansion expansion = KeyExpansion.NONE;
        int expansionIndex = -1;
        final String[] propertyNamesArray = new String[keyNames.size()];

        for (int i = 0; i < keyNames.size(); ++i) {
          final String keyName = keyNames.get(i);
          final KeyExpansion e = detectExpansion(keyName);
          if (e != KeyExpansion.NONE) {
            expansion = e;
            expansionIndex = i;
            propertyNamesArray[i] = stripModifier(keyName, e);
          } else {
            propertyNamesArray[i] = keyName;
          }
        }

        if (expansion == KeyExpansion.NONE) {
          // Standard deletion
          final Object[] keyValues = new Object[keyNames.size()];
          for (int i = 0; i < keyNames.size(); ++i) {
            keyValues[i] = getPropertyValue(record, propertyNamesArray[i]);
          }
          index.remove(keyValues, record.getIdentity());
        } else if (expansion == KeyExpansion.LIST_ITEM) {
          // Delete all list item entries
          deleteListItemsFromIndex(index, record, propertyNamesArray, expansionIndex);
        } else {
          // Delete all map key/value entries
          deleteMapEntriesFromIndex(index, record, propertyNamesArray, expansionIndex, expansion);
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

  private void deleteMapEntriesFromIndex(final Index index, final Document record,
      final String[] propertyNames, final int mapPropertyIndex, final KeyExpansion expansion) {
    for (final Object element : mapElements(record, propertyNames[mapPropertyIndex], expansion)) {
      final Object[] keyValues = new Object[propertyNames.length];
      for (int i = 0; i < keyValues.length; ++i)
        keyValues[i] = i == mapPropertyIndex ? element : getPropertyValue(record, propertyNames[i]);
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
