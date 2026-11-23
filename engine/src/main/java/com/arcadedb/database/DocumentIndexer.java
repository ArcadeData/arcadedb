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
import com.arcadedb.exception.ValidationException;
import com.arcadedb.graph.Edge;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.IndexKeyEquality;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.logging.Level;

public class DocumentIndexer {
  /** Past this many old x new tuple comparisons {@link #applyTupleDelta} hashes the tuples instead of probing them linearly. */
  private static final int LINEAR_TUPLE_PROBE_LIMIT = 256;

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

    return modifiedRecord.getType().getPolymorphicBucketIndexMaintainedInTransaction(bucketId);
  }

  /**
   * Whether a write stages entries for {@code index} in its transaction: false for a COLUMNAR index, whose projection is filled
   * after commit (see {@link Schema.INDEX_TYPE#isMaintainedInTransaction()}).
   */
  private static boolean isMaintainedInTransaction(final Index index) {
    final Schema.INDEX_TYPE type = index.getType();
    return type == null || type.isMaintainedInTransaction();
  }

  /**
   * {@code indexes} without the ones no write maintains: the same list (null included) when there is none, which is always the case
   * for a list {@link #getInvolvedIndexes} answered, so that path allocates nothing.
   */
  private static List<IndexInternal> maintainedInTransaction(final List<IndexInternal> indexes) {
    if (indexes == null)
      return null;
    for (int i = 0; i < indexes.size(); i++)
      if (!isMaintainedInTransaction(indexes.get(i))) {
        final List<IndexInternal> maintained = new ArrayList<>(indexes.size() - 1);
        for (final IndexInternal index : indexes)
          if (isMaintainedInTransaction(index))
            maintained.add(index);
        return maintained;
      }
    return indexes;
  }

  public void createDocument(final Document record, final DocumentType type, final LocalBucket bucket) {
    final RID rid = record.getIdentity();
    if (rid == null)
      throw new IllegalArgumentException("Cannot index a non persistent record");

    // INDEX THE RECORD
    final List<IndexInternal> metadata = type.getPolymorphicBucketIndexMaintainedInTransaction(bucket.getFileId());
    for (final Index entry : metadata)
      addToIndex(entry, rid, record);
  }

  public void addToIndex(final Index entry, final RID rid, final Document record) {
    if (!isMaintainedInTransaction(entry))
      return;
    forEachIndexKey(entry, record, keyValues -> entry.put(keyValues, new RID[] { rid }));
  }

  /**
   * Takes out of ONE index the entries {@link #addToIndex} puts for {@code record}: the keys it derives are the same, so what
   * a build added for a record is exactly what this removes (issue #10490).
   */
  public void removeFromIndex(final Index entry, final RID rid, final Document record) {
    if (!isMaintainedInTransaction(entry))
      return;
    forEachIndexKey(entry, record, keyValues -> entry.remove(keyValues, rid));
  }

  /**
   * Expands a document into the same logical keys used by normal index maintenance without mutating the index.
   */
  public void forEachIndexKey(final Index entry, final Document record, final Consumer<Object[]> keyConsumer) {
    final List<String> keyNames = entry.getPropertyNames();

    // Detect a collection modifier ("by item" for LIST, "by key"/"by value" for MAP) on each property.
    final String[] propertyNamesArray = new String[keyNames.size()];
    final KeyExpansion[] expansions = detectExpansions(keyNames, propertyNamesArray);
    final boolean expanded = hasExpansion(expansions);
    if (isFullText(entry) && expanded)
      // Full-text index over BY ITEM / BY KEY / BY VALUE properties (issues #5181, #10209): a full-text index is an inverted
      // per-field index, not a compound key, so each expanded property is indexed as the UNION of its own elements (the lists
      // may be different and of different lengths), ONE key per document. No element-wise zip and no shared-root requirement.
      // UPGRADE: a single-property BM25 index built before #10209 put every item on its own, so the postings of a record not
      // written since carry per-item term frequencies and lengths. They are found and removed exactly as before, and the corpus
      // counters repair themselves, but only a full REBUILD INDEX (not statsOnly) rewrites those per-posting stats.
      keyConsumer.accept(buildFullTextExpandedKey(record, propertyNamesArray, expansions));
    else if (!expanded) {
      // Standard indexing - single entry per document
      final Object[] keyValues = new Object[keyNames.size()];
      for (int i = 0; i < keyValues.length; ++i)
        keyValues[i] = getPropertyValue(record, propertyNamesArray[i]);
      keyConsumer.accept(keyValues);
    } else
      // BY ITEM / BY KEY / BY VALUE: one entry per element, the cross product when more than one collection is expanded
      forEachExpandedKey(record, propertyNamesArray, expansions, keyConsumer);
  }

  /**
   * Detects the collection modifier of every index property, filling {@code propertyNamesArray} with the modifier-stripped
   * property names, and returns the per-property {@link KeyExpansion}.
   */
  private static KeyExpansion[] detectExpansions(final List<String> keyNames, final String[] propertyNamesArray) {
    final KeyExpansion[] expansions = new KeyExpansion[keyNames.size()];
    for (int i = 0; i < keyNames.size(); ++i) {
      final String keyName = keyNames.get(i);
      final KeyExpansion e = detectExpansion(keyName);
      expansions[i] = e;
      propertyNamesArray[i] = e != KeyExpansion.NONE ? stripModifier(keyName, e) : keyName;
    }
    return expansions;
  }

  private static boolean hasExpansion(final KeyExpansion[] expansions) {
    for (final KeyExpansion e : expansions)
      if (e != KeyExpansion.NONE)
        return true;
    return false;
  }

  private static boolean isMapExpansion(final KeyExpansion expansion) {
    return expansion == KeyExpansion.MAP_KEY || expansion == KeyExpansion.MAP_VALUE;
  }

  /**
   * Expands a document into the key tuples of an index with at least one {@code BY ITEM} / {@code BY KEY} / {@code BY VALUE}
   * property. The {@code BY ITEM} properties all descend from one list and are zipped element by element (issue #5181), every
   * {@code BY KEY} / {@code BY VALUE} property contributes the distinct keys or values of its own map, and the entries are the
   * cross product of those dimensions, every other property contributing its scalar value (issue #12444). A cross product, not a
   * zip, because what the planner answers with such an index is a conjunction of independent membership tests
   * ({@code m1 CONTAINSKEY 'a' AND m2 CONTAINSVALUE 'b'}, even on one map): the record must be found under every combination.
   * A collection with no element (null, empty, or only null elements) leaves the record without entries, as a single
   * expanded property always did.
   * <p>
   * One path for every shape, so the insert, the update diff and the delete cannot disagree on the keys: they all come from
   * here. Before #12444 only the LAST modified property was expanded and the others were stored as whole collections.
   */
  private void forEachExpandedKey(final Document record, final String[] propertyNames, final KeyExpansion[] expansions,
      final Consumer<Object[]> keyConsumer) {
    int mapCount = 0;
    boolean hasList = false;
    for (final KeyExpansion e : expansions)
      if (e == KeyExpansion.LIST_ITEM)
        hasList = true;
      else if (isMapExpansion(e))
        ++mapCount;

    final int[] mapPositions = new int[mapCount];
    final Object[][] mapElements = new Object[mapCount][];
    for (int i = 0, m = 0; i < expansions.length; ++i)
      if (isMapExpansion(expansions[i])) {
        final Collection<Object> elements = mapElements(record, propertyNames[i], expansions[i]);
        if (elements.isEmpty())
          // nothing to pair the other dimensions with
          return;
        mapPositions[m] = i;
        mapElements[m++] = elements.toArray();
      }

    if (hasList)
      for (final Object[] tuple : buildListItemTuples(record, propertyNames, expansions))
        crossMapElements(tuple, mapPositions, mapElements, 0, keyConsumer);
    else {
      final Object[] tuple = new Object[propertyNames.length];
      for (int i = 0; i < propertyNames.length; ++i)
        if (expansions[i] == KeyExpansion.NONE)
          tuple[i] = getPropertyValue(record, propertyNames[i]);
      crossMapElements(tuple, mapPositions, mapElements, 0, keyConsumer);
    }
  }

  /**
   * Hands {@code keyConsumer} one tuple per combination of the map elements from {@code depth} on, filling the map positions of
   * {@code tuple}. The tuple is handed as it is when there is no map to cross (it is a fresh one), and copied otherwise, since the
   * consumer may keep it (a transaction stages the key array).
   */
  private static void crossMapElements(final Object[] tuple, final int[] mapPositions, final Object[][] mapElements, final int depth,
      final Consumer<Object[]> keyConsumer) {
    if (depth == mapPositions.length) {
      keyConsumer.accept(depth == 0 ? tuple : tuple.clone());
      return;
    }
    final int position = mapPositions[depth];
    for (final Object element : mapElements[depth]) {
      tuple[position] = element;
      crossMapElements(tuple, mapPositions, mapElements, depth + 1, keyConsumer);
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

  private static boolean isFullText(final Index entry) {
    return entry.getType() == Schema.INDEX_TYPE.FULL_TEXT;
  }

  /**
   * Updates a FULL_TEXT index over {@code BY ITEM} / {@code BY KEY} / {@code BY VALUE} properties (issues #5181, #10209): rebuilds
   * the per-field key from the original and modified record and, when they differ, removes the old tokens and adds the new ones
   * in one shot (the full-text index tokenizes each field position, so a single remove/put covers every element of every
   * collection).
   * <p>
   * The record's key is never diffed element by element: what the index stores is the ANALYZED tokens, and several elements can
   * analyze to the same token ({@code beta} and {@code BETA}). Removing the element {@code BETA} alone would remove the posting
   * {@code beta} the surviving element {@code beta} still needs, and the record would vanish from the index (issue #10209).
   * <p>
   * An empty key (null or empty collections) is still removed and put: no posting comes of it, but a BM25 index counts the
   * document in its corpus counters on put, exactly as it counts a document whose scalar value is null, so the record count the
   * drift check compares against and the count the writers keep stay the same population.
   *
   * @return {@code true} if the index was changed.
   */
  private boolean updateFullTextExpandedInIndex(final Index index, final RID rid, final Document originalRecord,
      final Document modifiedRecord, final String[] propertyNames, final KeyExpansion[] expansions) {
    final Object[] oldKey = buildFullTextExpandedKey(originalRecord, propertyNames, expansions);
    final Object[] newKey = buildFullTextExpandedKey(modifiedRecord, propertyNames, expansions);
    if (sameKeyTuple(oldKey, newKey))
      return false;
    index.remove(oldKey, rid);
    index.put(newKey, new RID[] { rid });
    return true;
  }

  /**
   * Builds the single full-text key for a document: each {@code BY ITEM} position holds the list (union) of the values extracted
   * from that property's own root list, each {@code BY KEY} / {@code BY VALUE} position the distinct keys or values of its map,
   * every other position its scalar value.
   */
  private Object[] buildFullTextExpandedKey(final Document record, final String[] propertyNames, final KeyExpansion[] expansions) {
    final Object[] keyValues = new Object[propertyNames.length];
    for (int i = 0; i < propertyNames.length; ++i)
      keyValues[i] = switch (expansions[i]) {
        case LIST_ITEM -> collectListItemValues(record, propertyNames[i]);
        // mapElements() answers a collection of its own, never the map's live view: no copy needed
        case MAP_KEY, MAP_VALUE -> mapElements(record, propertyNames[i], expansions[i]);
        case NONE -> getPropertyValue(record, propertyNames[i]);
      };
    return keyValues;
  }

  /**
   * Returns the non-null values of a single {@code BY ITEM} property, walking its own root list once and extracting the nested
   * path from each element (e.g. {@code synonyms.name} -> the {@code name} of every element of {@code synonyms}).
   */
  private List<Object> collectListItemValues(final Document record, final String propertyName) {
    final String[] pathParts = propertyName.split("\\.");
    final Object listValue = record.get(pathParts[0]);
    if (listValue == null)
      return List.of();
    if (!(listValue instanceof List<?> list))
      throw new IndexException("Property '" + pathParts[0] + "' is indexed with BY ITEM but is not a LIST type");
    if (list.isEmpty())
      return List.of();

    final List<Object> values = new ArrayList<>(list.size());
    for (final Object element : list) {
      final Object value = extractNestedFromElement(element, pathParts);
      if (value != null)
        values.add(value);
    }
    return values;
  }

  /**
   * Builds one key tuple per element of the shared root list of the {@code BY ITEM} properties (one or more, issue #5181). Each
   * list property contributes the nested value extracted from the current element, each plain property its scalar record value;
   * the map positions are left for {@link #crossMapElements} to fill. An element is skipped only when every list-derived value is
   * null (a single list property does not index a null element).
   */
  private List<Object[]> buildListItemTuples(final Document record, final String[] propertyNames,
      final KeyExpansion[] expansions) {
    final String root = requireSharedListRoot(propertyNames, expansions);

    final Object listValue = record.get(root);
    if (listValue == null)
      return List.of();
    if (!(listValue instanceof List<?> list))
      throw new IndexException("Property '" + root + "' is indexed with BY ITEM but is not a LIST type");
    if (list.isEmpty())
      return List.of();

    // Precompute the path parts of each list property (relative to the shared root element).
    final String[][] pathParts = new String[propertyNames.length][];
    for (int i = 0; i < propertyNames.length; ++i)
      if (expansions[i] == KeyExpansion.LIST_ITEM)
        pathParts[i] = propertyNames[i].split("\\.");

    final List<Object[]> tuples = new ArrayList<>(list.size());
    for (final Object element : list) {
      final Object[] keyValues = new Object[propertyNames.length];
      boolean allListNull = true;
      for (int i = 0; i < propertyNames.length; ++i) {
        if (expansions[i] == KeyExpansion.LIST_ITEM) {
          final Object value = extractNestedFromElement(element, pathParts[i]);
          keyValues[i] = value;
          if (value != null)
            allListNull = false;
        } else if (expansions[i] == KeyExpansion.NONE)
          keyValues[i] = getPropertyValue(record, propertyNames[i]);
      }
      if (!allListNull)
        tuples.add(keyValues);
    }
    return tuples;
  }

  /**
   * Returns the common root list property shared by every {@code BY ITEM} property, or throws when they do not all descend from
   * the same list (a cross-product of independent lists is ambiguous and not supported).
   */
  private static String requireSharedListRoot(final String[] propertyNames, final KeyExpansion[] expansions) {
    String root = null;
    for (int i = 0; i < propertyNames.length; ++i) {
      if (expansions[i] != KeyExpansion.LIST_ITEM)
        continue;
      final int dot = propertyNames[i].indexOf('.');
      final String r = dot > 0 ? propertyNames[i].substring(0, dot) : propertyNames[i];
      if (root == null)
        root = r;
      else if (!root.equals(r))
        throw new IndexException(
            "Multiple BY ITEM properties in the same index must reference the same list; found '" + root + "' and '" + r + "'");
    }
    return root;
  }

  /**
   * Extracts a nested value from a single list element following {@code pathParts[1..]} (index 0 is the root list property).
   * Returns the element itself for a simple list ({@code pathParts.length == 1}).
   */
  private static Object extractNestedFromElement(final Object element, final String[] pathParts) {
    Object value = element;
    for (int j = 1; j < pathParts.length; ++j) {
      if (value == null)
        break;
      if (value instanceof Document doc)
        value = doc.get(pathParts[j]);
      else if (value instanceof Map<?, ?> map)
        value = map.get(pathParts[j]);
      else
        return null;
    }
    return value;
  }

  /**
   * Updates an index with collection modifiers ({@code BY ITEM}, {@code BY KEY}, {@code BY VALUE}) by diffing the complete key
   * tuples the write path produces for the original and the modified record. Diffing whole tuples rather than the collection's
   * element membership is what keeps the non-expanded sibling properties of the same composite key honest: a change to a scalar
   * sibling alone changes every tuple, so every stale entry is removed and re-added under the new key (issue #6934). The tuples
   * come from {@link #forEachExpandedKey}, the builder of the insert, so update and insert cannot disagree on the key shape.
   *
   * @return {@code true} if at least one index entry was removed or added.
   */
  private boolean updateExpandedKeysInIndex(final Index index, final RID rid, final Document originalRecord,
      final Document modifiedRecord, final String[] propertyNames, final KeyExpansion[] expansions) {
    final List<Object[]> oldTuples = new ArrayList<>();
    forEachExpandedKey(originalRecord, propertyNames, expansions, oldTuples::add);
    final List<Object[]> newTuples = new ArrayList<>();
    forEachExpandedKey(modifiedRecord, propertyNames, expansions, newTuples::add);
    return applyTupleDelta(index, rid, oldTuples, newTuples);
  }

  /**
   * Removes the tuples no longer produced by the record and adds the ones that are new, leaving unchanged tuples untouched.
   *
   * @return {@code true} if at least one index entry was removed or added.
   */
  private static boolean applyTupleDelta(final Index index, final RID rid, final List<Object[]> oldTuples,
      final List<Object[]> newTuples) {
    // a linear probe is cheapest for the usual handful of entries; a cross product of collections (issue #12444) or a long list
    // would make it quadratic, so past that the membership test is hashed with the same equality
    final boolean hashed = (long) oldTuples.size() * newTuples.size() > LINEAR_TUPLE_PROBE_LIMIT;
    final Set<TupleKey> oldKeys = hashed ? tupleKeys(oldTuples) : null;
    final Set<TupleKey> newKeys = hashed ? tupleKeys(newTuples) : null;

    boolean changed = false;
    for (final Object[] oldTuple : oldTuples) {
      if (hashed ? !newKeys.contains(new TupleKey(oldTuple)) : !containsTuple(newTuples, oldTuple)) {
        index.remove(oldTuple, rid);
        changed = true;
      }
    }
    for (final Object[] newTuple : newTuples) {
      if (hashed ? !oldKeys.contains(new TupleKey(newTuple)) : !containsTuple(oldTuples, newTuple)) {
        index.put(newTuple, new RID[] { rid });
        changed = true;
      }
    }
    return changed;
  }

  /** A key tuple as a hash-set member, with the {@link IndexKeyEquality} equality every diff of this class uses. */
  private record TupleKey(Object[] values) {
    @Override
    public boolean equals(final Object o) {
      return o instanceof TupleKey other && sameKeyTuple(values, other.values);
    }

    @Override
    public int hashCode() {
      return IndexKeyEquality.hashTuple(values);
    }
  }

  private static Set<TupleKey> tupleKeys(final List<Object[]> tuples) {
    final Set<TupleKey> keys = new HashSet<>(tuples.size() * 4 / 3 + 1);
    for (final Object[] tuple : tuples)
      keys.add(new TupleKey(tuple));
    return keys;
  }

  private static boolean containsTuple(final List<Object[]> tuples, final Object[] tuple) {
    for (final Object[] t : tuples)
      if (sameKeyTuple(t, tuple))
        return true;
    return false;
  }

  /**
   * The one equality every "did this key change?" decision in this class goes through: two tuples are the same key when
   * every element is {@link #sameKeyValue(Object, Object) the same key value}. Kept as the single policy so a new update
   * path cannot pick a third notion of equality (issue #7109), and shared with the transaction-side overlay through
   * {@link IndexKeyEquality} (issue #7881).
   */
  private static boolean sameKeyTuple(final Object[] a, final Object[] b) {
    return IndexKeyEquality.sameTuple(a, b);
  }

  /**
   * Content-aware equality for a single key value: array-typed keys (the {@code float[]} of a vector index, a
   * {@code BINARY} property) do not override {@code Object.equals()}, so two independently deserialized-but-identical
   * arrays would compare unequal and trigger a spurious remove()+put() on every record update (issues #5318 and #7109).
   * <p>
   * Delegated to {@link IndexKeyEquality}, which is where the transaction-side overlay reads the same policy from -
   * the two used to have their own copies of it and disagreed on exactly these keys (issue #7881).
   */
  private static boolean sameKeyValue(final Object a, final Object b) {
    return IndexKeyEquality.sameValue(a, b);
  }

  /**
   * Records, as the state a later update of the record inside this transaction diffs against, what the indexes hold for
   * {@code pending} once {@code built} has been built over its pending state (issue #10490).
   * <p>
   * An update diffs against {@link TransactionContext#getLastIndexedSnapshot} when there is one and against the committed
   * buffer otherwise, and neither knows that an index created after the record was written has been brought up to its
   * pending state: the next update would remove and add from the committed (or the creation) values, and the entry queued for
   * the intermediate value would stay in the index for good. The snapshot built here takes the values of the properties
   * {@code built} indexes from {@code pending}, and those of every other index from the diff source it had, an earlier snapshot
   * or the buffer, since the reconciliation did not touch them. Nothing is recorded for a record the transaction did not
   * write: its buffer is its pending state.
   */
  public void snapshotAfterBuild(final TransactionContext tx, final Document pending, final IndexInternal built) {
    final RID rid = pending.getIdentity();
    if (rid == null || tx.getWrittenRecord(rid) == null || !isMaintainedInTransaction(built))
      return;

    final List<IndexInternal> indexes = getInvolvedIndexes(pending);
    if (indexes == null || indexes.isEmpty())
      return;

    Document baseline = tx.getLastIndexedSnapshot(rid);
    if (baseline == null)
      try {
        baseline = database.getOriginalDocument(pending);
      } catch (final IllegalStateException e) {
        // No buffer to read: the other indexes are given the pending values, the best state left to describe them. Benign: with
        // no earlier snapshot, no index was changed by the writes that came before, so the pending values of their properties
        // are the ones they already hold
        LogManager.instance().log(this, Level.FINE, "No original buffer for record %s: the indexes other than '%s' are described by "
            + "its pending values in the snapshot of the next update", rid, built.getName());
        baseline = pending;
      }

    final Map<String, Object> values = new LinkedHashMap<>();
    for (final IndexInternal index : indexes) {
      final Document source = index == built ? pending : baseline;
      final List<String> keyNames = index.getPropertyNames();
      final String[] propertyNames = new String[keyNames.size()];
      final KeyExpansion[] expansions = detectExpansions(keyNames, propertyNames);

      for (int i = 0; i < propertyNames.length; ++i) {
        // the same keys updateDocument() stores: the root of a list read per item, the map as a whole, the value otherwise
        final String key;
        final Object value;
        if (expansions[i] == KeyExpansion.LIST_ITEM) {
          final int dot = propertyNames[i].indexOf('.');
          key = dot > 0 ? propertyNames[i].substring(0, dot) : propertyNames[i];
          value = source.get(key);
        } else if (expansions[i] == KeyExpansion.MAP_KEY || expansions[i] == KeyExpansion.MAP_VALUE) {
          key = propertyNames[i];
          value = source.get(key);
        } else {
          key = propertyNames[i];
          value = getPropertyValue(source, key);
        }
        // One value per property name: the built index wins over the others for a property they share. They cannot disagree in
        // practice, an index that existed was brought to the pending state by the update that changed it (and left a snapshot)
        if (!values.containsKey(key) || index == built)
          values.put(key, copyForSnapshot(value));
      }
    }
    tx.setLastIndexedSnapshot(rid, new DetachedDocument(pending, values));
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
  public Document updateDocument(final Document originalRecord, final Document modifiedRecord, final List<IndexInternal> involved) {
    if (involved == null || involved.isEmpty())
      return null;
    final List<IndexInternal> indexes = maintainedInTransaction(involved);
    if (indexes.isEmpty())
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
    final KeyExpansion[][] indexExpansions = new KeyExpansion[totalIndexes][];

    boolean anyIndexModified = false;
    for (int indexPos = 0; indexPos < totalIndexes; ++indexPos) {
      final Index index = indexes.get(indexPos);
      final List<String> keyNames = index.getPropertyNames();

      // Detect a collection modifier ("by item" for LIST, "by key"/"by value" for MAP) on each property.
      final String[] propertyNamesArray = new String[keyNames.size()];
      final KeyExpansion[] expansions = detectExpansions(keyNames, propertyNamesArray);
      final boolean expanded = hasExpansion(expansions);

      indexPropertyNames[indexPos] = propertyNamesArray;
      indexExpansions[indexPos] = expansions;

      if (isFullText(index) && expanded)
        // Full-text index over expanded properties (issues #5181, #10209): replace the per-field element unions as a whole
        anyIndexModified |= updateFullTextExpandedInIndex(index, rid, originalRecord, modifiedRecord, propertyNamesArray, expansions);
      else if (!expanded) {
        // Standard update logic (existing code)
        final Object[] oldKeyValues = new Object[keyNames.size()];
        final Object[] newKeyValues = new Object[keyNames.size()];

        boolean keyValuesAreModified = false;
        for (int i = 0; i < keyNames.size(); ++i) {
          oldKeyValues[i] = getPropertyValue(originalRecord, propertyNamesArray[i]);
          newKeyValues[i] = getPropertyValue(modifiedRecord, propertyNamesArray[i]);

          if (!keyValuesAreModified && !sameKeyValue(newKeyValues[i], oldKeyValues[i]))
            keyValuesAreModified = true;
        }

        indexNewKeyValues[indexPos] = newKeyValues;

        if (!keyValuesAreModified)
          // SAME VALUES, SKIP INDEX UPDATE
          continue;

        final BucketSelectionStrategy bucketSelectionStrategy = modifiedRecord.getType().getBucketSelectionStrategy();
        if (bucketSelectionStrategy instanceof PartitionedBucketSelectionStrategy strategy) {
          if (!List.of(strategy.getProperties())
              .equals(index.getPropertyNames()))
            // a refusal of the write, not a fault of the index: answered as the client's request, like a declared constraint (issue #12037)
            throw new ValidationException("Cannot modify primary key when the bucket selection is partitioned");
        }

        // REMOVE THE OLD ENTRY KEYS/VALUE AND INSERT THE NEW ONE
        index.remove(oldKeyValues, rid);
        index.put(newKeyValues, new RID[] { rid });
        anyIndexModified = true;
      } else
        // BY ITEM / BY KEY / BY VALUE modifiers: diff the complete key tuples, so a change to a scalar sibling of the collection
        // property is reflected too (issue #6934), and so is a change to any of several expanded collections (issue #12444)
        anyIndexModified |= updateExpandedKeysInIndex(index, rid, originalRecord, modifiedRecord, propertyNamesArray, expansions);
    }

    if (!anyIndexModified)
      return null;

    // Build the refreshed snapshot from the values retained above. The snapshot must cover ALL involved
    // indexes (the next update diffs every index against it), including the ones that did not change: their
    // current values equal the previous state, so re-reading them from the modified record is correct.
    final Map<String, Object> values = new LinkedHashMap<>();
    for (int indexPos = 0; indexPos < totalIndexes; ++indexPos) {
      final String[] propertyNames = indexPropertyNames[indexPos];
      final KeyExpansion[] expansions = indexExpansions[indexPos];
      final Object[] newKeyValues = indexNewKeyValues[indexPos];

      for (int i = 0; i < propertyNames.length; ++i) {
        if (expansions[i] == KeyExpansion.LIST_ITEM) {
          // The list delta logic reads the ROOT property directly (nested paths descend per element).
          final int dot = propertyNames[i].indexOf('.');
          final String root = dot > 0 ? propertyNames[i].substring(0, dot) : propertyNames[i];
          if (!values.containsKey(root))
            values.put(root, copyForSnapshot(modifiedRecord.get(root)));
        } else if (expansions[i] == KeyExpansion.MAP_KEY || expansions[i] == KeyExpansion.MAP_VALUE) {
          // The map delta logic reads the stripped map property directly.
          if (!values.containsKey(propertyNames[i]))
            values.put(propertyNames[i], copyForSnapshot(modifiedRecord.get(propertyNames[i])));
        } else if (!values.containsKey(propertyNames[i]))
          values.put(propertyNames[i], copyForSnapshot(
              newKeyValues != null ? newKeyValues[i] : getPropertyValue(modifiedRecord, propertyNames[i])));
      }
    }
    return new DetachedDocument(modifiedRecord, values);
  }

  /**
   * Queues on {@code tx} the index changes that bring the indexes to the values {@code document} holds now, which is what an update
   * of a record does when it is saved in a transaction. They are diffed against the state an earlier update of the record in the same
   * transaction left (its indexed snapshot, issue #4935) and against the committed buffer when there is none: the buffer stays
   * frozen until the commit, because the serialization is deferred, so diffing against it would leak an entry for every intermediate
   * value. The refreshed snapshot is recorded only when an index changed: otherwise the diff source it had still describes the indexed
   * state, and an update that touches no indexed property costs no snapshot.
   *
   * @param indexes the indexes {@link #getInvolvedIndexes} answered for {@code document}, not empty
   */
  public void updateDocumentInTransaction(final TransactionContext tx, final Document document, final List<IndexInternal> indexes) {
    final RID rid = document.getIdentity();
    final Document previous = tx.getLastIndexedSnapshot(rid);
    final Document refreshedSnapshot = updateDocument(previous != null ? previous : database.getOriginalDocument(document), document,
        indexes);
    if (refreshedSnapshot != null)
      tx.setLastIndexedSnapshot(rid, refreshedSnapshot);
  }

  /**
   * Puts back, on a record the transaction has queued for its commit, the indexed values an index has just refused (issue #13130).
   * <p>
   * A save that an index refuses leaves the indexes as they were (the entries it queued are taken back by the caller), but the values it
   * assigned are on the instance, and the commit writes that very instance and indexes whatever was assigned to it after its last accepted
   * save (#11751): it would meet the refused value again, fail with it, and take every valid write of the transaction down. A record that
   * was not queued before the refused save is taken out of the queue instead, so this is only for the one that was.
   * <p>
   * The values restored are the ones the indexes hold, the source {@link #updateDocumentInTransaction} diffs against: the snapshot of the
   * last indexed state of the record in this transaction, and the committed buffer when there is none. Only the properties an index keys
   * are touched, and only when they differ from it: the other properties assigned by the refused call stay on the record, as does
   * anything assigned to a queued record without a save, which the commit writes as it is.
   *
   * @param indexes the indexes {@link #getInvolvedIndexes} answered for {@code document}
   *
   * @return true when every indexed property of the record is what the indexes hold again, and the commit has nothing to re-index: false
   *     when a value could not be put back (the state the indexes hold is not kept as a whole property, as for the nested path of an
   *     embedded document or map, or there is nothing to read it from), and the caller has to refuse the commit instead of storing a value
   *     no index describes
   */
  public boolean restoreIndexedValues(final TransactionContext tx, final MutableDocument document, final List<IndexInternal> indexes) {
    final List<IndexInternal> maintained = maintainedInTransaction(indexes);
    if (maintained == null || maintained.isEmpty())
      return true;

    Document baseline = tx.getLastIndexedSnapshot(document.getIdentity());
    if (baseline == null)
      try {
        baseline = database.getOriginalDocument(document);
      } catch (final IllegalStateException e) {
        // No buffer to read: nothing says what the indexes hold
        return false;
      }

    for (final IndexInternal index : maintained) {
      final List<String> keyNames = index.getPropertyNames();
      final String[] propertyNames = new String[keyNames.size()];
      final KeyExpansion[] expansions = detectExpansions(keyNames, propertyNames);

      for (int i = 0; i < propertyNames.length; ++i) {
        // the keys the snapshot of the indexed state is stored under, see updateDocument(): the root of a list read per item, the map as
        // a whole, the value (a nested path resolved) otherwise
        final KeyExpansion expansion = expansions[i];
        final String key;
        if (expansion == KeyExpansion.LIST_ITEM) {
          final int dot = propertyNames[i].indexOf('.');
          key = dot > 0 ? propertyNames[i].substring(0, dot) : propertyNames[i];
        } else
          key = propertyNames[i];

        final boolean asProperty = expansion != KeyExpansion.NONE;
        final Object indexed = asProperty ? baseline.get(key) : getPropertyValue(baseline, key);
        if (sameKeyValue(asProperty ? document.get(key) : getPropertyValue(document, key), indexed))
          continue;

        // a nested path (the value is part of a property, which the snapshot does not keep), record metadata, and a value that holds
        // embedded documents (detached in the snapshot) cannot be given back as a property
        if (key.isEmpty() || key.charAt(0) == '@' || (!asProperty && key.indexOf('.') >= 0 && !document.has(key)) || holdsDocument(indexed))
          return false;

        if (indexed == null)
          document.remove(key);
        else
          document.set(key, copyForRestore(indexed));
      }
    }
    return true;
  }

  private static boolean holdsDocument(final Object value) {
    if (value instanceof Document)
      return true;
    if (value instanceof Collection<?> collection) {
      for (final Object element : collection)
        if (element instanceof Document)
          return true;
    } else if (value instanceof Map<?, ?> map)
      for (final Object element : map.values())
        if (element instanceof Document)
          return true;
    return false;
  }

  /**
   * The value to assign to the record from what the snapshot holds: the snapshot keeps its arrays and, for the ones of a primitive type
   * (the {@code float[]} of a vector), by reference, so the record is given its own, or a later change to one would change the other.
   */
  private static Object copyForRestore(final Object value) {
    final Class<?> type = value.getClass();
    if (type.isArray() && type.getComponentType().isPrimitive()) {
      final int length = Array.getLength(value);
      final Object copy = Array.newInstance(type.getComponentType(), length);
      System.arraycopy(value, 0, copy, 0, length);
      return copy;
    }
    return copyForSnapshot(value);
  }

  /**
   * Indexes what was assigned to a record after the last {@code save()} that queued it for the commit (issue #11751). That save brought
   * the indexes to the values the record held then, and the commit writes the record as it is when it runs, so a later assignment
   * reaches the record and would never reach the indexes. Queues the difference, which is empty when the assignment put back what the
   * save had left. Only while the transaction is {@code BEGUN}, the one state an index queues an entry instead of writing it.
   */
  public void updateDocumentAssignedAfterSave(final TransactionContext tx, final Document document) {
    final List<IndexInternal> indexes = getInvolvedIndexes(document);
    if (indexes != null && !indexes.isEmpty())
      updateDocumentInTransaction(tx, document, indexes);
  }

  public void deleteDocument(final Document record) {
    if (record.getIdentity() == null)
      // RECORD IS NOT PERSISTENT
      return;

    final int bucketId = record.getIdentity().getBucketId();

    final DocumentType type = database.getSchema().getTypeByBucketId(bucketId);
    if (type == null)
      // A BUCKET NO TYPE OWNS ANY MORE: THE INDEXES OF THE TYPE IT LEFT DROPPED THEIR SUB-INDEX OF IT, SO THERE IS NOTHING TO REMOVE
      // (ISSUE #11335)
      return;

    final List<IndexInternal> metadata = type.getPolymorphicBucketIndexMaintainedInTransaction(bucketId);
    if (metadata != null && !metadata.isEmpty()) {
      final List<IndexInternal> allIndexes = new ArrayList(metadata);
      for (final IndexInternal index : metadata) {
        final IndexInternal assIndex = index.getAssociatedIndex();
        if (assIndex != null)
          allIndexes.add(assIndex);
      }

      for (final IndexInternal index : allIndexes) {
        final List<String> keyNames = index.getPropertyNames();

        // Detect a collection modifier ("by item" for LIST, "by key"/"by value" for MAP) on each property.
        final String[] propertyNamesArray = new String[keyNames.size()];
        final KeyExpansion[] expansions = detectExpansions(keyNames, propertyNamesArray);
        final boolean expanded = hasExpansion(expansions);

        if (isFullText(index) && expanded)
          // Remove the per-field element unions of a full-text index over expanded properties (issues #5181, #10209), the one
          // key the record was put with
          index.remove(buildFullTextExpandedKey(record, propertyNamesArray, expansions), record.getIdentity());
        else if (!expanded) {
          // Standard deletion
          final Object[] keyValues = new Object[keyNames.size()];
          for (int i = 0; i < keyNames.size(); ++i)
            keyValues[i] = getPropertyValue(record, propertyNamesArray[i]);
          index.remove(keyValues, record.getIdentity());
        } else
          // Delete every entry of an index over BY ITEM / BY KEY / BY VALUE properties: the keys the insert put
          forEachExpandedKey(record, propertyNamesArray, expansions, keyValues -> index.remove(keyValues, record.getIdentity()));
      }

      // Reset dirty flag AFTER properties have been read for index deletion
      if (record instanceof RecordInternal internal)
        internal.unsetDirty();
    }
  }

  /**
   * Reads the value an index keys for {@code propertyName}: the exact property name first (names with dots are valid), then the dotted
   * path, and the special {@code @out}/{@code @in} of an edge. Static so the columnar extractor reads a record exactly as an index does.
   */
  public static Object getPropertyValue(final Document record, final String propertyName) {
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
  private static Object getNestedPropertyValue(final Document record, final String propertyPath) {
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
  private static Object getNestedValue(Object current, String path) {
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
