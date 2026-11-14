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
package com.arcadedb.engine;

import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.serializer.BinaryComparator;
import com.arcadedb.serializer.BinarySerializer;
import com.arcadedb.utility.LongHashSet;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

/**
 * Compares the entries of the LSM-Tree indexes of one bucket with the records of that bucket (issue #11734): an entry whose
 * record does not exist, an entry whose record does not hold its key, and a key a record holds that has no entry. The structural
 * check ({@code LSMTreeIndex.checkIntegrity()}) reads the key order of the pages only, so none of those is visible to it, and an
 * indexed query answers them wrong rows (a dangling entry names whatever record reuses the RID later) or misses rows.
 * <p>
 * A healthy index costs one scan of the bucket, shared by all its indexes, and one walk of each index: both sides fold their
 * (key, RID) pairs into a count and two order-independent sums of a 64-bit fingerprint of the pair. The key is fingerprinted by
 * its serialized form under the index's key types, after the conversion an insert applies, so the record side hashes exactly what
 * the index stores. Only an index whose three numbers differ takes the second, precise pass, which reads the record of every entry
 * and names what it finds. A finding is therefore always a concrete entry or record: a mismatch the precise pass cannot pin
 * down (a write that landed between the scan and the walk) reports nothing.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class IndexContentChecker {
  private static final long FNV_OFFSET = 0xcbf29ce484222325L;
  private static final long FNV_PRIME  = 0x100000001b3L;

  private final DatabaseInternal database;
  private final BinarySerializer serializer;
  private final int              maxExamples;
  private final Binary           buffer = new Binary(128);

  /** What the precise pass found in one index. */
  static final class Report {
    long dangling;
    long stale;
    long missing;
    final List<String> examples = new ArrayList<>();

    boolean isInconsistent() {
      return dangling > 0 || stale > 0 || missing > 0;
    }
  }

  private static final class Summary {
    long count;
    long sum;
    long mixedSum;

    void add(final long fingerprint) {
      ++count;
      sum += fingerprint;
      mixedSum += mix(fingerprint);
    }

    boolean matches(final Summary other) {
      return count == other.count && sum == other.sum && mixedSum == other.mixedSum;
    }
  }

  IndexContentChecker(final DatabaseInternal database, final int maxExamples) {
    this.database = database;
    this.serializer = database.getSerializer();
    this.maxExamples = maxExamples;
  }

  /**
   * Checks the given indexes, all bucket indexes of {@code bucket}.
   *
   * @param tick called once per record scanned and per index entry walked, for the progress report
   *
   * @return the reports of the indexes whose entries do not match the records, empty when all of them do
   */
  Map<LSMTreeIndex, Report> check(final Bucket bucket, final List<LSMTreeIndex> indexes, final Runnable tick) {
    final DocumentType type = database.getSchema().getTypeByBucketId(bucket.getFileId());
    if (type == null || indexes.isEmpty())
      return Map.of();

    final Summary[] fromRecords = new Summary[indexes.size()];
    for (int i = 0; i < fromRecords.length; i++)
      fromRecords[i] = new Summary();

    final LongHashSet recordPairs = new LongHashSet(8);
    scanRecords(bucket, type, document -> {
      for (int i = 0; i < indexes.size(); i++) {
        final Summary summary = fromRecords[i];
        recordPairs.clear();
        forEachRecordPair(indexes.get(i), document, (fingerprint, keys) -> {
          // a record holding a key twice (a list item repeated) has one entry for it
          if (recordPairs.add(fingerprint))
            summary.add(fingerprint);
        });
      }
      tick.run();
    });

    Map<LSMTreeIndex, Report> reports = null;
    for (int i = 0; i < indexes.size(); i++) {
      final LSMTreeIndex index = indexes.get(i);
      final Summary fromEntries = new Summary();
      walkEntries(index, (fingerprint, rid, keys) -> {
        fromEntries.add(fingerprint);
        tick.run();
      });
      if (fromEntries.matches(fromRecords[i]))
        continue;

      final Report report = comparePrecisely(bucket, type, index, tick);
      if (report.isInconsistent()) {
        if (reports == null)
          reports = new LinkedHashMap<>();
        reports.put(index, report);
      }
    }
    return reports != null ? reports : Map.of();
  }

  /**
   * Reads the record of every entry, then scans the bucket for the keys no valid entry stands for.
   */
  private Report comparePrecisely(final Bucket bucket, final DocumentType type, final LSMTreeIndex index, final Runnable tick) {
    final Report report = new Report();
    final int bucketId = bucket.getFileId();
    final LongHashSet seen = new LongHashSet();
    final LongHashSet valid = new LongHashSet();

    walkEntries(index, (fingerprint, rid, keys) -> {
      tick.run();
      if (!seen.add(fingerprint))
        // the same pair met twice (a re-add that a compaction has not merged yet): one entry
        return;

      if (rid.getBucketId() != bucketId) {
        ++report.stale;
        example(report, "entry " + Arrays.toString(keys) + " -> " + rid + " names a record of another bucket");
        return;
      }

      final List<String> recordKeys = new ArrayList<>(2);
      final Document document = loadDocument(rid);
      if (document != null && holdsKey(index, document, fingerprint, keys, valid, recordKeys)) {
        valid.add(fingerprint);
        return;
      }

      // The entry was read before the record, and the check takes no lock: a commit in between can have changed both. The
      // candidate is judged again under the commit locks of the bucket and of the index, where no commit can land between the
      // two reads, so both see one committed state
      recordKeys.clear();
      final Verdict verdict = underCommitLocks(bucket, index, () -> {
        if (!isIndexed(index, keys, rid))
          return Verdict.GONE;
        final Document now = loadDocument(rid);
        if (now == null)
          return Verdict.DANGLING;
        return holdsKey(index, now, fingerprint, keys, valid, recordKeys) ? Verdict.VALID : Verdict.STALE;
      });
      switch (verdict) {
      case VALID -> valid.add(fingerprint);
      case DANGLING -> {
        ++report.dangling;
        example(report, "entry " + Arrays.toString(keys) + " -> " + rid + " names a record that does not exist");
      }
      case STALE -> {
        ++report.stale;
        example(report, "entry " + Arrays.toString(keys) + " -> " + rid + " names a record that does not hold that key"
            + (recordKeys.isEmpty() ? " (it has no key in this index)" : " (its key: " + String.join(", ", recordKeys) + ")"));
      }
      case GONE -> {
        // removed since the walk: no finding
      }
      }
    });

    scanRecords(bucket, type, document -> {
      tick.run();
      final RID rid = document.getIdentity();
      forEachRecordPair(index, document, (fingerprint, keys) -> {
        if (valid.contains(fingerprint) || isIndexed(index, keys, rid))
          return;
        // the same judgement under the commit locks: the record still holds the key and the index still has no entry for it
        final Verdict verdict = underCommitLocks(bucket, index, () -> {
          final Document now = loadDocument(rid);
          if (now == null)
            return Verdict.GONE;
          final boolean[] holds = new boolean[1];
          forEachRecordPair(index, now, (recordFingerprint, recordKey) -> holds[0] |= recordFingerprint == fingerprint);
          return holds[0] && !isIndexed(index, keys, rid) ? Verdict.MISSING : Verdict.GONE;
        });
        valid.add(fingerprint);
        if (verdict == Verdict.MISSING) {
          ++report.missing;
          example(report, "record " + rid + " has no entry for key " + Arrays.toString(keys));
        }
      });
    });
    return report;
  }

  private enum Verdict {VALID, GONE, DANGLING, STALE, MISSING}

  /**
   * Runs {@code judgement} holding the commit locks of the bucket and of every file of the index, which a commit writing the record
   * or its entries takes from before it validates until after it is visible. When the locks cannot be had in time (a database too
   * busy to pause the writes to one bucket), the judgement runs without them: it has already been made twice by then, once on the
   * walk and once again here.
   */
  private Verdict underCommitLocks(final Bucket bucket, final LSMTreeIndex index, final Callable<Verdict> judgement) {
    final List<Integer> files = new ArrayList<>(index.getFileIds());
    files.add(bucket.getFileId());
    try {
      return database.executeLockingFiles(files, judgement);
    } catch (final NeedRetryException e) {
      try {
        return judgement.call();
      } catch (final Exception inner) {
        throw new IndexException("Error comparing the entries of index '" + index.getName() + "' with the records", inner);
      }
    }
  }

  /**
   * Whether {@code document} holds the key of the entry whose pair has {@code fingerprint}. The record's other keys are collected
   * in {@code recordKeys} for the report (at most four).
   */
  private boolean holdsKey(final LSMTreeIndex index, final Document document, final long fingerprint, final Object[] keys,
      final LongHashSet valid, final List<String> recordKeys) {
    final boolean[] holds = new boolean[1];
    forEachRecordPair(index, document, (recordFingerprint, recordKey) -> {
      if (recordFingerprint == fingerprint)
        holds[0] = true;
      else if (sameKey(keys, recordKey, index.getBinaryKeyTypes())) {
        // one key under the index's comparator that the fingerprint spells two ways: valid, and so is the record's pair
        holds[0] = true;
        valid.add(recordFingerprint);
      } else if (recordKeys.size() < 4)
        recordKeys.add(Arrays.toString(recordKey));
    });
    return holds[0];
  }

  /**
   * Whether the index holds the pair now: a record written after the walk, which the scan met, is not a missing entry, and an
   * entry removed after the walk is no finding.
   */
  private boolean isIndexed(final LSMTreeIndex index, final Object[] keys, final RID rid) {
    final IndexCursor cursor = index.get(keys);
    try {
      while (cursor.hasNext()) {
        // the contract of IndexCursor says next() is never null: guarded anyway, a null must not abort the comparison
        final Identifiable entry = cursor.next();
        if (entry != null && rid.equals(entry.getIdentity()))
          return true;
      }
      return false;
    } finally {
      cursor.close();
    }
  }

  private boolean sameKey(final Object[] entryKey, final Object[] recordKey, final byte[] keyTypes) {
    if (entryKey.length != recordKey.length)
      return false;
    final BinaryComparator comparator = serializer.getComparator();
    for (int i = 0; i < entryKey.length; i++) {
      final boolean entryNull = entryKey[i] == null;
      final boolean recordNull = recordKey[i] == null;
      if (entryNull || recordNull) {
        // a null component equals only a null one
        if (entryNull != recordNull)
          return false;
      } else
        try {
          if (comparator.compare(entryKey[i], keyTypes[i], recordKey[i], keyTypes[i]) != 0)
            return false;
        } catch (final RuntimeException e) {
          return false;
        }
    }
    return true;
  }

  private void example(final Report report, final String message) {
    if (report.examples.size() < maxExamples)
      report.examples.add(message);
  }

  private Document loadDocument(final RID rid) {
    try {
      final Record record = database.lookupByRID(rid, true);
      return record != null ? record.asDocument(true) : null;
    } catch (final RecordNotFoundException e) {
      return null;
    }
  }

  private void scanRecords(final Bucket bucket, final DocumentType type, final Consumer<Document> consumer) {
    bucket.scan((rid, view) -> {
      final Document document;
      try {
        document = database.getRecordFactory().newImmutableRecord(database, type, rid, view, null).asDocument(true);
      } catch (final Exception e) {
        // a record that does not load is reported by the record passes, not as an index finding
        return true;
      }
      consumer.accept(document);
      return true;
    }, null);
  }

  @FunctionalInterface
  private interface PairConsumer {
    void accept(long fingerprint, Object[] storedKeys);
  }

  @FunctionalInterface
  private interface EntryConsumer {
    void accept(long fingerprint, RID rid, Object[] keys);
  }

  /**
   * The (key, RID) pairs an insert of {@code document} puts in {@code index}: the keys the indexer derives, converted as
   * {@code put()} converts them, without the all-null key a SKIP index leaves out.
   */
  private void forEachRecordPair(final LSMTreeIndex index, final Document document, final PairConsumer consumer) {
    final RID rid = document.getIdentity();
    final byte[] keyTypes = index.getBinaryKeyTypes();
    final boolean indexesNulls = index.getNullStrategy() == LSMTreeIndexAbstract.NULL_STRATEGY.INDEX;
    database.getIndexer().forEachIndexKey(index, document, keys -> {
      final Object[] stored;
      try {
        stored = index.convertLookupKeys(keys);
      } catch (final IllegalArgumentException e) {
        // a value the key type cannot read is refused by put(): the record holds no entry for it
        return;
      }
      if (!indexesNulls && LSMTreeIndexAbstract.isKeyNull(stored))
        return;
      consumer.accept(fingerprint(stored, keyTypes, rid), stored);
    });
  }

  private void walkEntries(final LSMTreeIndex index, final EntryConsumer consumer) {
    final byte[] keyTypes = index.getBinaryKeyTypes();
    final IndexCursor cursor = index.iterator(true);
    try {
      while (cursor.hasNext()) {
        final Identifiable entry = cursor.next();
        if (entry == null)
          // the contract of IndexCursor says next() is never null: guarded anyway, a null must not abort the comparison
          continue;
        final RID rid = entry.getIdentity();
        final Object[] keys = cursor.getKeys();
        consumer.accept(fingerprint(keys, keyTypes, rid), rid, keys);
      }
    } finally {
      cursor.close();
    }
  }

  /**
   * A 64-bit FNV-1a of the key serialized under the index's key types, folded with the RID. Serialized without the data
   * encryption, whose random IV gives one key a different image each time, and with a decimal stripped of its trailing zeros:
   * the index compares 5, 5.0 and 5.00 as one key and keeps the scale it was given first.
   */
  private long fingerprint(final Object[] keys, final byte[] keyTypes, final RID rid) {
    buffer.clear();
    for (int i = 0; i < keys.length; i++) {
      Object key = keys[i];
      if (key == null)
        buffer.putByte((byte) 0);
      else {
        if (key instanceof BigDecimal decimal)
          key = decimal.signum() == 0 ? BigDecimal.ZERO : decimal.stripTrailingZeros();
        buffer.putByte((byte) 1);
        serializer.serializeValue(database, buffer, keyTypes[i], key, false);
      }
    }
    final byte[] content = buffer.getContent();
    final int length = buffer.position();
    long hash = FNV_OFFSET;
    for (int i = 0; i < length; i++) {
      hash ^= content[i] & 0xFF;
      hash *= FNV_PRIME;
    }
    hash = mix(hash ^ rid.getBucketId());
    return mix(hash ^ rid.getPosition());
  }

  /** The finalizer of MurmurHash3's 64-bit variant. */
  private static long mix(long value) {
    value ^= value >>> 33;
    value *= 0xff51afd7ed558ccdL;
    value ^= value >>> 33;
    value *= 0xc4ceb9fe1a85ec53L;
    value ^= value >>> 33;
    return value;
  }
}
