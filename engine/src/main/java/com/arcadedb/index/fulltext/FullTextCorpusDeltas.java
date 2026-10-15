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
package com.arcadedb.index.fulltext;

import com.arcadedb.database.TransactionContext;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.FullTextIndexMetadata;
import com.arcadedb.schema.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

/**
 * What one transaction did to the BM25 corpus counters of the full-text indexes it wrote: the documents it indexed minus the ones
 * it removed, and the same for their lengths, per logical index.
 * <p>
 * The counters used to move at index time, before the transaction committed, and nothing undid them: a rolled-back insert left
 * them inflated (issue #10018). This accumulator is attached to the transaction and its net effect is applied when the commit
 * publishes the transaction's pages ({@link #applyCommitted}), the point where the record counters of the buckets move too, so a
 * rollback, a retried {@code ConcurrentModificationException} or a refused commit leave the counters alone.
 * <p>
 * The same net effect is what an HA cluster replicates: the leader ships {@link #snapshot()} with the transaction and every
 * node that publishes the pages from the replicated entry instead of from a local commit applies it through
 * {@link #applyReplicated}, again where the record counters move, so all nodes move the counters by the same amount at the same
 * point of the log instead of leaving the followers' copies to drift until a restart. A node applies a transaction's deltas
 * exactly once: either by committing it, or by applying the entry that carries it.
 * <p>
 * A transaction is confined to one thread, so nothing here is synchronized.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class FullTextCorpusDeltas {
  private static final String ATTACHMENT_KEY = "fulltext.corpusDeltas";

  /**
   * The net effect on one logical full-text index, as it travels in a replicated transaction.
   *
   * @param indexName the logical index (the {@code TypeIndex} name), which is what a replica resolves in its own schema
   * @param docs      documents indexed minus documents removed
   * @param length    tokens indexed minus tokens removed
   */
  public record CorpusDelta(String indexName, long docs, long length) {
  }

  // A systematic cause (a schema that does not hold the index) would fail every delta of every transaction: one warning a minute
  // per index of a database (the throttle lives on the counters of the index), and one for the deltas whose index cannot be resolved
  private static final long       FAILURE_LOG_THROTTLE_MS = 60_000L;
  private static final AtomicLong LAST_UNRESOLVED_LOG     = new AtomicLong(0L);

  private final List<Entry> entries = new ArrayList<>(2);
  // Every document of a bulk load lands on the same index: remember it, so the common case costs a reference comparison
  private       Entry       last;

  private static final class Entry {
    private final FullTextIndexMetadata metadata;
    private final String                indexName;
    private       long                  docs;
    private       long                  length;

    private Entry(final FullTextIndexMetadata metadata, final String indexName) {
      this.metadata = metadata;
      this.indexName = indexName;
    }
  }

  private FullTextCorpusDeltas() {
  }

  /**
   * Records a document indexed ({@code +1}) or removed ({@code -1}) by {@code tx}.
   *
   * @param tx        the transaction that is writing the index
   * @param metadata  the counters shared by the bucket indexes of the logical index
   * @param indexName the name of the logical index, see {@link CorpusDelta#indexName()}
   * @param docs      {@code +1} or {@code -1}
   * @param length    the length of the document, signed like {@code docs}
   */
  public static void record(final TransactionContext tx, final FullTextIndexMetadata metadata, final String indexName,
      final int docs, final long length) {
    FullTextCorpusDeltas deltas = (FullTextCorpusDeltas) tx.getAttachment(ATTACHMENT_KEY);
    if (deltas == null) {
      deltas = new FullTextCorpusDeltas();
      tx.setAttachment(ATTACHMENT_KEY, deltas);
    }
    deltas.add(metadata, indexName, docs, length);
  }

  /**
   * @return the accumulator of {@code tx}, or null when the transaction wrote no full-text index
   */
  public static FullTextCorpusDeltas peek(final TransactionContext tx) {
    return (FullTextCorpusDeltas) tx.getAttachment(ATTACHMENT_KEY);
  }

  private void add(final FullTextIndexMetadata metadata, final String indexName, final int docs, final long length) {
    Entry entry = last;
    if (entry == null || entry.metadata != metadata) {
      entry = null;
      for (final Entry e : entries)
        if (e.metadata == metadata) {
          entry = e;
          break;
        }
      if (entry == null) {
        entry = new Entry(metadata, indexName);
        entries.add(entry);
      }
      last = entry;
    }
    entry.docs += docs;
    entry.length += length;
  }

  /**
   * What this transaction has done to the counters of {@code metadata} and has not applied yet. A scan that rebuilds the
   * counters inside the writing transaction reads its uncommitted documents too, and takes them out of its result with this:
   * the commit applies them, and counting them in the scan as well would count them twice.
   */
  public long pendingDocs(final FullTextIndexMetadata metadata) {
    for (final Entry e : entries)
      if (e.metadata == metadata)
        return e.docs;
    return 0L;
  }

  /** @see #pendingDocs */
  public long pendingLength(final FullTextIndexMetadata metadata) {
    for (final Entry e : entries)
      if (e.metadata == metadata)
        return e.length;
    return 0L;
  }

  /**
   * @return the net effect per logical index, for the replicated transaction; empty when there is nothing to ship
   */
  public List<CorpusDelta> snapshot() {
    final List<CorpusDelta> result = new ArrayList<>(entries.size());
    for (final Entry e : entries)
      // an insert and a delete of the same length in one transaction net to nothing: there is nothing for another node to apply
      if (e.docs != 0 || e.length != 0)
        result.add(new CorpusDelta(e.indexName, e.docs, e.length));
    return result;
  }

  /** The commit is about to make the pages of this transaction visible, see {@link FullTextIndexMetadata#commitApplyStarted}. */
  public void commitApplyStarted() {
    for (final Entry e : entries)
      e.metadata.commitApplyStarted();
  }

  /** The delta has reached the counters, or the commit gave up: pairs {@link #commitApplyStarted}. */
  public void commitApplyFinished() {
    for (final Entry e : entries)
      e.metadata.commitApplyFinished();
  }

  /**
   * The counters a replicated entry is about to move, marked as being applied (see {@link FullTextIndexMetadata#commitApplyStarted})
   * before its pages are written. Pair it with {@link #replicatedApplyFinished} in a {@code finally}.
   *
   * @return the marked counters, empty when the entry carries no delta
   */
  public static List<FullTextIndexMetadata> replicatedApplyStarted(final Schema schema, final List<CorpusDelta> deltas) {
    if (deltas == null || deltas.isEmpty())
      return List.of();
    final List<FullTextIndexMetadata> marked = new ArrayList<>(deltas.size());
    for (final CorpusDelta delta : deltas)
      try {
        final FullTextIndexMetadata metadata = resolveMetadata(schema, delta.indexName());
        if (metadata != null) {
          metadata.commitApplyStarted();
          marked.add(metadata);
        }
      } catch (final RuntimeException e) {
        // The apply of the delta runs the same lookup and reports the failure; said here too, so a scan that ran unmarked during
        // that apply can be explained
        LogManager.instance().log(FullTextCorpusDeltas.class, Level.FINE,
            "Cannot mark the BM25 corpus counters of full-text index '%s' as being applied", e, delta.indexName());
      }
    return marked;
  }

  /** @see #replicatedApplyStarted */
  public static void replicatedApplyFinished(final List<FullTextIndexMetadata> marked) {
    for (final FullTextIndexMetadata metadata : marked)
      metadata.commitApplyFinished();
  }

  /**
   * Marks the counters of every index this transaction wrote as recomputed, so a node that already holds them adopts the
   * persisted pair of the schema that follows (see {@link FullTextIndexMetadata#adoptPersistedCounters}). For a transaction whose
   * WAL does not travel in a {@code TX_ENTRY}: one committed inside a schema session ships inside the {@code SCHEMA_ENTRY}, which
   * carries no corpus deltas but does carry the schema, and with it the leader's counters. Called once the commit has applied
   * the deltas, so the pair that is serialized already holds them.
   */
  public void advanceEpochs() {
    for (final Entry e : entries)
      e.metadata.advanceCountersEpoch();
  }

  /**
   * Applies what the transaction did, once its pages are published: called by the commit, on the node that ran it.
   * <p>
   * The pages are visible before the counters move, so a BM25 search running at that instant can score the new postings with the
   * previous corpus statistics. The counters feed only the length normalizer and the inverse document frequency, which a
   * single transaction moves by a fraction, and the next search sees them whole: the same brief skew the bucket record counts
   * have, and not worth a lock on the read path.
   */
  public void applyCommitted() {
    for (final Entry e : entries)
      try {
        e.metadata.applyCorpusDelta(e.docs, e.length);
      } catch (final RuntimeException ex) {
        // The pages are published: a commit past that point cannot be reported as failed because of an estimate. The counters are
        // armed to be checked against the live data instead, as for a replicated delta that cannot be applied
        e.metadata.requireRescan();
        logFailure(e.metadata, e.indexName, ex);
      }
  }

  /**
   * Applies the deltas a replicated transaction carried, on a node that did not run it. An index this node no longer has (a
   * {@code DROP INDEX} applied after the transaction was proposed) is nobody's business, exactly like the record counter of a
   * dropped bucket.
   */
  public static void applyReplicated(final Schema schema, final List<CorpusDelta> deltas) {
    for (final CorpusDelta delta : deltas) {
      FullTextIndexMetadata metadata = null;
      try {
        metadata = resolveMetadata(schema, delta.indexName());
        if (metadata != null)
          metadata.applyCorpusDelta(delta.docs(), delta.length());
      } catch (final RuntimeException e) {
        // A replicated transaction is not worth failing over the counters, whatever the lookup or the application raised. A node
        // that skipped a delta, or applied only part of it (the pair is two counters), has drifted from the rest of the cluster:
        // the next search checks its counters against the live data and rescans. When the lookup itself failed there is no
        // metadata to arm: this node cannot resolve an index the transaction names, so either it does not have it (a DROP applied
        // meanwhile, and there are no counters to keep) or its schema is not the cluster's, which the schema machinery repairs
        // and, with it, the counters of the index it loads: they come from the persisted definition, whose drift check runs on
        // the first search
        if (metadata != null)
          metadata.requireRescan();
        logFailure(metadata, delta.indexName(), e);
      }
    }
  }

  private static void logFailure(final FullTextIndexMetadata metadata, final String indexName, final RuntimeException e) {
    final long now = System.currentTimeMillis();
    final boolean mayLog;
    if (metadata != null)
      mayLog = metadata.claimFailureLog(now, FAILURE_LOG_THROTTLE_MS);
    else {
      final long last = LAST_UNRESOLVED_LOG.get();
      mayLog = now - last > FAILURE_LOG_THROTTLE_MS && LAST_UNRESOLVED_LOG.compareAndSet(last, now);
    }
    if (mayLog)
      LogManager.instance().log(FullTextCorpusDeltas.class, Level.WARNING,
          "Cannot apply the BM25 corpus counters of full-text index '%s' (further failures are not logged for a minute)", e,
          indexName);
  }

  /**
   * One lookup, not an existence check followed by a read: the index may be dropped between the two.
   *
   * @return the counters of the index, or null when this node has no such full-text index
   */
  private static FullTextIndexMetadata resolveMetadata(final Schema schema, final String indexName) {
    try {
      return LSMTreeFullTextIndex.metadataOf(schema.getIndexByName(indexName));
    } catch (final SchemaException e) {
      return null;
    }
  }
}
