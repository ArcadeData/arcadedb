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
package com.arcadedb.index.lsm;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.index.ConsistentKeyLookup;
import com.arcadedb.index.EmptyIndexCursor;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexCursorEntry;
import com.arcadedb.index.TempIndexCursor;
import com.arcadedb.utility.RidHashSet;

import java.util.ArrayList;
import java.util.List;

/**
 * The entries an older build wrote into an {@link LSMTreeIndex} under the old spelling of a key (issues #12160 and #12233, see
 * {@link LSMTreeIndexAbstract#legacyKeys}): a seek, a removal and the uniqueness check at commit look under both spellings. Only an
 * index that {@link LSMTreeIndex#mayHoldLegacyKeys() may hold such entries} gets here: one this build created or rebuilt holds
 * canonical keys only, and pays nothing.
 * <p>
 * An old entry counts only when its record holds the value: the old text of a list or a number is also the key of a STRING spelled
 * the same way, which is another value since #11615 and #9132.
 */
final class LSMTreeIndexLegacyKeys {
  private static final IndexCursor          EMPTY_CURSOR             = new EmptyIndexCursor();
  // Test seam: runs in the seeking thread between the read of the old spelling and the read of the canonical key of a seek
  private static final ThreadLocal<Runnable> BETWEEN_READS_TEST_HOOK = new ThreadLocal<>();

  private LSMTreeIndexLegacyKeys() {
  }

  /** Sets, or with null removes, the test hook of the calling thread: see {@link #BETWEEN_READS_TEST_HOOK}. */
  static void setBetweenReadsTestHook(final Runnable hook) {
    if (hook == null)
      BETWEEN_READS_TEST_HOOK.remove();
    else
      BETWEEN_READS_TEST_HOOK.set(hook);
  }

  /**
   * The entries of a seek on an index that may hold old entries, or null when the key has no old spelling and spells none, so the
   * entries of {@code convertedKeys} are the answer as they are.
   */
  static IndexCursor get(final LSMTreeIndex index, final Object[] keys, final Object[] convertedKeys, final int limit) {
    final Object[] legacyKeys = index.mutable.legacyKeys(keys, convertedKeys);
    if (legacyKeys != null)
      return withLegacyEntries(index, convertedKeys, legacyKeys, limit);
    if (index.mutable.maySpellALegacyKey(keys, convertedKeys))
      return withoutLegacyHolders(index, convertedKeys, limit);
    return null;
  }

  /**
   * The entries of {@code convertedKeys}, with the entries an older build wrote for the same value under {@code legacyKeys}.
   * <p>
   * The two keys are read one after the other, and a commit between the reads can move a record's entry from the old key to the
   * canonical one: it would be in neither read. So both are read between two samples of the page manager's publication sequence
   * that are equal and even, as {@link ConsistentKeyLookup} reads its keys, and otherwise read again; after
   * {@link ConsistentKeyLookup#MAX_ATTEMPTS} attempts, once more with the publications held off.
   * <p>
   * The old key is read first: when it holds no entry in a state no commit was publishing, every record holding the value is under the
   * canonical key, as no build writes the old key any more, and the cursor of the canonical key is the answer, read lazily as any seek.
   */
  private static IndexCursor withLegacyEntries(final LSMTreeIndex index, final Object[] convertedKeys, final Object[] legacyKeys,
      final int limit) {
    final List<IndexCursorEntry> entries = ConsistentKeyLookup.readConsistently(index.getDatabase(), index.getName(),
        () -> readWithLegacyEntries(index, convertedKeys, legacyKeys, limit));
    return toCursor(index, convertedKeys, entries, limit);
  }

  private static IndexCursor toCursor(final LSMTreeIndex index, final Object[] convertedKeys, final List<IndexCursorEntry> entries,
      final int limit) {
    if (entries == null)
      return index.getConverted(convertedKeys, limit);
    return entries.isEmpty() ? EMPTY_CURSOR : new TempIndexCursor(entries);
  }

  /** The entries of both keys, or null when the old key holds no entry. */
  private static List<IndexCursorEntry> readWithLegacyEntries(final LSMTreeIndex index, final Object[] convertedKeys,
      final Object[] legacyKeys, final int limit) {
    List<RID> legacyHolders = null;
    try (final IndexCursor cursor = index.getConverted(legacyKeys, -1)) {
      while (cursor.hasNext()) {
        if (legacyHolders == null)
          legacyHolders = new ArrayList<>(2);
        legacyHolders.add(cursor.next().getIdentity());
      }
    }
    final Runnable betweenReads = BETWEEN_READS_TEST_HOOK.get();
    if (betweenReads != null)
      betweenReads.run();
    if (legacyHolders == null)
      return null;

    final List<IndexCursorEntry> entries = new ArrayList<>();
    final RidHashSet seen = new RidHashSet(16);
    try (final IndexCursor cursor = index.getConverted(convertedKeys, limit)) {
      while (cursor.hasNext()) {
        final Identifiable next = cursor.next();
        if (seen.add(next.getIdentity()))
          entries.add(new IndexCursorEntry(convertedKeys, next, 1));
      }
    }
    for (int i = 0; i < legacyHolders.size() && (limit < 0 || entries.size() < limit); i++) {
      final RID holder = legacyHolders.get(i);
      if (!seen.contains(holder) && holdsKey(index, holder, convertedKeys)) {
        seen.add(holder);
        entries.add(new IndexCursorEntry(convertedKeys, holder, 1));
      }
    }
    return entries;
  }

  /**
   * The entries of a STRING key that spells the old key of a list or a number ({@link LSMTreeIndexAbstract#maySpellALegacyKey}),
   * without the records that are under it only through an entry an older build wrote for their list or number: since #11615 and
   * #9132 the STRING and the value are two keys. The limit is applied to what is left.
   */
  private static IndexCursor withoutLegacyHolders(final LSMTreeIndex index, final Object[] convertedKeys, final int limit) {
    final List<IndexCursorEntry> entries = new ArrayList<>();
    try (final IndexCursor cursor = index.getConverted(convertedKeys, -1)) {
      while (cursor.hasNext() && (limit < 0 || entries.size() < limit)) {
        final Identifiable next = cursor.next();
        if (holdsKey(index, next.getIdentity(), convertedKeys))
          entries.add(new IndexCursorEntry(convertedKeys, next, 1));
      }
    }
    return entries.isEmpty() ? EMPTY_CURSOR : new TempIndexCursor(entries);
  }

  /** Removes, with the entries of a key, the entries an older build wrote for the same value: only those, not a STRING's. */
  static void removeLegacyEntries(final LSMTreeIndex index, final Object[] keys, final Object[] convertedKeys) {
    final Object[] legacyKeys = index.mutable.legacyKeys(keys, convertedKeys);
    if (legacyKeys == null)
      return;
    final List<RID> holders = new ArrayList<>();
    try (final IndexCursor cursor = index.getConverted(legacyKeys, -1)) {
      while (cursor.hasNext())
        holders.add(cursor.next().getIdentity());
    }
    for (int i = 0; i < holders.size(); i++)
      if (holdsKey(index, holders.get(i), convertedKeys))
        index.removeConverted(legacyKeys, holders.get(i));
  }

  /**
   * Removes, with the entry of a record, the entry an older build wrote for the record's value. Looked up first, so that a value no
   * old entry stands for leaves no removal in the index.
   */
  static void removeLegacyEntry(final LSMTreeIndex index, final Object[] keys, final Object[] convertedKeys, final Identifiable rid) {
    final Object[] legacyKeys = index.mutable.legacyKeys(keys, convertedKeys);
    if (legacyKeys == null)
      return;
    final RID holder = rid.getIdentity();
    try (final IndexCursor cursor = index.getConverted(legacyKeys, -1)) {
      while (cursor.hasNext())
        if (holder.equals(cursor.next().getIdentity())) {
          index.removeConverted(legacyKeys, rid);
          return;
        }
    }
  }

  /**
   * Whether the record at {@code rid} holds {@code convertedKeys} in the index, as its put() converted them: false for a record that
   * is gone, as one deleted since its entry was read.
   */
  static boolean holdsKey(final LSMTreeIndex index, final RID rid, final Object[] convertedKeys) {
    final Document document = loadDocument(index.getDatabase(), rid);
    if (document == null)
      return false;
    final boolean[] holds = new boolean[1];
    index.getDatabase().getIndexer().forEachIndexKey(index, document, keys -> {
      if (!holds[0])
        try {
          holds[0] = sameKey(index, index.convertLookupKeys(keys), convertedKeys);
        } catch (final IllegalArgumentException e) {
          // a value the key type cannot read holds no key here
        }
    });
    return holds[0];
  }

  /**
   * The keys an older build wrote for the value the record at {@code rid} holds under {@code convertedKeys}, or null when it wrote
   * them the same way or the record holds no such value. The record is loaded only for a key that can be the canonical key of a
   * list or a number.
   */
  static Object[] legacyKeysOf(final LSMTreeIndex index, final Object[] convertedKeys, final RID rid) {
    if (!index.mutable.mayBeCanonicalKeyOfLegacyValue(convertedKeys))
      return null;
    final Document document = loadDocument(index.getDatabase(), rid);
    if (document == null)
      return null;
    final Object[][] legacyKeys = new Object[1][];
    index.getDatabase().getIndexer().forEachIndexKey(index, document, keys -> {
      if (legacyKeys[0] == null)
        try {
          if (sameKey(index, index.convertLookupKeys(keys), convertedKeys))
            legacyKeys[0] = index.mutable.legacyKeys(keys, convertedKeys);
        } catch (final IllegalArgumentException e) {
          // a value the key type cannot read holds no key here
        }
    });
    return legacyKeys[0];
  }

  /** The record at {@code rid} as a document, or null when it is gone. */
  private static Document loadDocument(final DatabaseInternal database, final RID rid) {
    try {
      final Record record = database.lookupByRID(rid, true);
      return record != null ? record.asDocument(true) : null;
    } catch (final RecordNotFoundException e) {
      return null;
    }
  }

  /**
   * Whether two converted keys are one key under the index's comparator, which reads 1.0 and 1.00 as one DECIMAL where
   * {@code equals} does not: a null component equals only a null one.
   */
  private static boolean sameKey(final LSMTreeIndex index, final Object[] keys1, final Object[] keys2) {
    if (keys1 == null || keys2 == null || keys1.length != keys2.length)
      return false;
    final LSMTreeIndexMutable mutable = index.mutable;
    final byte[] keyTypes = mutable.binaryKeyTypes;
    for (int i = 0; i < keys1.length; i++) {
      if (keys1[i] == null || keys2[i] == null) {
        // one of the two is null here, so they are one key only when the other is null too
        if (keys1[i] != null || keys2[i] != null)
          return false;
      } else
        try {
          if (mutable.comparator.compare(keys1[i], keyTypes[i], keys2[i], keyTypes[i]) != 0)
            return false;
        } catch (final IllegalArgumentException | UnsupportedOperationException | ClassCastException e) {
          // what BinaryComparator throws for values it cannot set side by side (a pair of types it refuses, a binary of an unknown
          // class): they are not one key
          return false;
        }
    }
    return true;
  }
}
