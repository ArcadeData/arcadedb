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
package com.arcadedb.graph;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.database.RID;
import com.arcadedb.database.TransactionContext;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.Schema;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.utility.MultiIterator;
import com.arcadedb.utility.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

/**
 * Striped edge list of a "super-node" (hot vertex): the logical edge list is spread over multiple chains
 * (stripes) hosted in different files, listed by a {@link StripeDirectory}. Concurrent appends hash to
 * different stripes and therefore take different per-file commit locks, removing the single-page/single-file
 * serialisation of the classic layout. Placement is by the NEIGHBOUR vertex RID
 * ({@link StripeDirectory#stripeOf}), so the connectivity operations that key on the neighbour
 * ({@code isConnectedTo}, {@code containsVertex}, {@code removeVertex}) visit one stripe per generation instead
 * of the whole list.
 * <p>
 * Each stripe chain is itself a classic {@link EdgeLinkedList}, so all per-chain behaviour (iteration,
 * filtering, removal relinking, the commutative append merge) is reused as-is; this class only routes
 * operations to the right chain(s) and maintains the directory.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class StripedEdgeList extends EdgeLinkedList {
  private static final String STRIPE_BUCKET_INFIX = "_sn_stripe_";

  /** Guards against stampeding pool creations: one in-flight creation per database+type (value = start ms). */
  private static final ConcurrentHashMap<String, Long> POOLS_IN_CREATION = new ConcurrentHashMap<>();
  /** One-shot diagnostic latch: warn ONCE per type if a pool creation looks stuck (see ensureStripePool). */
  private static final Set<String>                     POOLS_STUCK_WARNED = ConcurrentHashMap.newKeySet();
  private static final long                            POOL_CREATION_STUCK_MS = 60_000;
  /** After this long in flight the creation is presumed dead (hung thread) and a later chunk-full re-attempts. */
  private static final long                            POOL_CREATION_RETRY_MS = 5 * 60_000;
  /** Millis of the last skipped-chain WARNING (see addChain): a hot super-node under the transient cross-file
   * publication window could otherwise flood the log with one line per read. Reads are deliberately
   * BEST-EFFORT - unlike the write path (which surfaces the same condition as a retryable conflict, see
   * loadStripeHead), a read skips the momentarily unresolvable chain, so counts/iterations during a concurrent
   * commit can transiently under-report instead of failing. */
  private static final AtomicLong                      LAST_SKIPPED_CHAIN_WARN = new AtomicLong();

  private final DatabaseInternal database;
  private       StripeDirectory  directory;

  public StripedEdgeList(final Vertex vertex, final Vertex.DIRECTION direction, final StripeDirectory directory) {
    super(vertex, direction, null);
    this.database = (DatabaseInternal) vertex.getDatabase();
    this.directory = directory;
  }

  /**
   * Name of the {@code slot}-th stripe bucket of the per-type pool. DELIBERATELY direction-agnostic: OUT and
   * IN chains of promoted vertices share the same pool to cap the file count per type (chunks are only ever
   * reached by pointer, never by scanning a file, so mixing directions in one bucket is harmless). The cost is
   * that concurrent OUT and IN appends hashing to the same slot contend on one file lock - acceptable, because
   * real hubs are typically hot in a single direction and the pool size is the tuning lever.
   */
  public static String stripeBucketName(final String typeName, final int slot) {
    return typeName + STRIPE_BUCKET_INFIX + slot;
  }

  /**
   * Makes sure the per-type stripe bucket pool exists, returning true only when EVERY bucket is ready.
   * <p>
   * Bucket creation is a SCHEMA operation: on an embedded database it is safe (and immediate) to run inline,
   * but on a wrapped database (server/HA) a schema change must NOT ride inside the promoting user transaction -
   * under Raft the SCHEMA_ENTRY has to replicate before any TX_ENTRY that references the new files (see issue
   * #4083), or followers diverge. In that case the creation is handed to a one-shot helper thread that runs it
   * outside any transaction, this promotion attempt is skipped, and the vertex simply promotes at a later
   * chunk-full once the pool is ready.
   */
  static boolean ensureStripePool(final DatabaseInternal database, final String typeName, final int stripes) {
    final Schema schema = database.getSchema();

    boolean ready = true;
    for (int i = 0; i < stripes; i++) {
      final String bucketName = stripeBucketName(typeName, i);
      if (!schema.existsBucket(bucketName)) {
        ready = false;
        break;
      }
      if (schema.getTypeByBucketId(schema.getBucketByName(bucketName).getFileId()) != null) {
        // NAME COLLISION: a USER bucket owned by a type happens to match the pool naming scheme. Adopting it
        // would write edge segments into another type's data (cross-type corruption): never promote this type.
        LogManager.instance()
            .log(StripedEdgeList.class, Level.WARNING,
                "Super-node promotion disabled for type '%s': bucket '%s' matches the stripe pool naming scheme but belongs to type '%s'",
                typeName, bucketName, schema.getTypeNameByBucketId(schema.getBucketByName(bucketName).getFileId()));
        return false;
      }
    }
    if (ready)
      return true;

    if (database.getEmbedded() == database) {
      // EMBEDDED: SCHEMA OPS ARE LOCAL AND NON-TRANSACTIONAL, SAFE TO CREATE INLINE. Why this is safe mid-user-
      // transaction: bucket creation applies immediately to the local schema (its durability is independent of
      // the open data transaction), there is no replication ordering to violate (the HA hazard the deferred
      // branch below avoids, #4083), and if the user transaction later rolls back the only residue is empty
      // pool buckets - which the next promotion attempt reuses.
      try {
        createStripePool(database, typeName, stripes);
        return true;
      } catch (final Exception e) {
        LogManager.instance()
            .log(StripedEdgeList.class, Level.WARNING, "Cannot create super-node stripe buckets for type '%s': promotion skipped", e,
                typeName);
        return false;
      }
    }

    // WRAPPED (SERVER/HA) DATABASE: DEFER THE DDL OUTSIDE THE CURRENT TRANSACTION. The guard key is released
    // in the helper thread's finally: if that thread ever HUNG inside the schema call (rather than throwing),
    // the key would stay pinned and the type could not promote until restart - acceptable, because a hung
    // schema operation means the database has bigger problems than a deferred promotion, and promotion is a
    // pure optimisation (the classic layout keeps working).
    final String key = database.getDatabasePath() + '|' + typeName;
    final Long inFlightSince = POOLS_IN_CREATION.putIfAbsent(key, System.currentTimeMillis());
    if (inFlightSince == null) {
      // NOTE (concurrency): not engine data-path parallelism but a once-per-type lifecycle action, hence a
      // short-lived dedicated thread instead of a shared pool (see the QueryEngineManager rule in CLAUDE.md).
      final Thread creator = new Thread(() -> {
        try {
          createStripePool(database, typeName, stripes);
        } catch (final Exception e) {
          LogManager.instance()
              .log(StripedEdgeList.class, Level.WARNING, "Error creating super-node stripe buckets for type '%s': promotion deferred", e,
                  typeName);
        } finally {
          POOLS_IN_CREATION.remove(key);
          POOLS_STUCK_WARNED.remove(key);
        }
      }, "arcadedb-supernode-pool-" + typeName);
      creator.setDaemon(true);
      creator.start();
    } else {
      final long inFlightMs = System.currentTimeMillis() - inFlightSince;
      if (inFlightMs > POOL_CREATION_RETRY_MS && POOLS_IN_CREATION.replace(key, inFlightSince, System.currentTimeMillis())) {
        // The previous creation is presumed dead (hung thread that never reached its finally): re-attempt.
        // createStripePool tolerates buckets the old attempt may still create concurrently (exists-check +
        // lost-race tolerance), so a duplicate run is safe.
        LogManager.instance()
            .log(StripedEdgeList.class, Level.WARNING,
                "Super-node stripe pool creation for type '%s' was in flight for %d ms: presuming it dead and re-attempting", typeName,
                inFlightMs);
        final Thread retry = new Thread(() -> {
          try {
            createStripePool(database, typeName, stripes);
          } catch (final Exception e) {
            LogManager.instance()
                .log(StripedEdgeList.class, Level.WARNING, "Error re-attempting super-node stripe bucket creation for type '%s'", e,
                    typeName);
          } finally {
            POOLS_IN_CREATION.remove(key);
            POOLS_STUCK_WARNED.remove(key);
          }
        }, "arcadedb-supernode-pool-" + typeName);
        retry.setDaemon(true);
        retry.start();
      } else if (inFlightMs > POOL_CREATION_STUCK_MS && POOLS_STUCK_WARNED.add(key))
        // In flight for over a minute: warn ONCE so an operator can tell a stuck promotion from a merely
        // deferred one (a re-attempt fires automatically past POOL_CREATION_RETRY_MS).
        LogManager.instance()
            .log(StripedEdgeList.class, Level.WARNING,
                "Super-node stripe pool creation for type '%s' has been in flight for over %d ms: promotions for this type are deferred until it completes",
                typeName, POOL_CREATION_STUCK_MS);
    }
    return false;
  }

  private static void createStripePool(final DatabaseInternal database, final String typeName, final int stripes) {
    final Schema schema = database.getSchema();
    for (int i = 0; i < stripes; i++) {
      final String bucketName = stripeBucketName(typeName, i);
      if (!schema.existsBucket(bucketName)) {
        try {
          schema.createBucket(bucketName);
        } catch (final Exception e) {
          if (!schema.existsBucket(bucketName))
            // NOT A LOST RACE WITH A CONCURRENT CREATION: GIVE UP
            throw e;
        }
      } else if (schema.getTypeByBucketId(schema.getBucketByName(bucketName).getFileId()) != null)
        // NAME COLLISION with a type-owned user bucket (see ensureStripePool): adopting it would corrupt that
        // type's data. Fail the creation - the readiness check keeps promotion off for this type.
        throw new IllegalStateException(
            "Bucket '" + bucketName + "' matches the super-node stripe pool naming scheme of type '" + typeName
                + "' but belongs to type '" + schema.getTypeNameByBucketId(schema.getBucketByName(bucketName).getFileId()) + "'");
    }
  }

  @Override
  public void add(final RID edgeRID, final RID vertexRID) {
    // FAST PATH: use the read-only directory view. A possibly stale stripe head is SAFE - an in-place append
    // into an older chunk lands mid-chain, which is valid for the unordered edge list - and crucially the
    // directory page is neither anchored nor modified, so a plain append never takes the directory file's
    // commit lock (which is held across the whole replication round under HA and would re-serialise the very
    // writers striping parallelises).
    final int generation = directory.getNewestGeneration();
    final int slot = StripeDirectory.stripeOf(vertexRID, directory.getStripes(generation));

    EdgeSegment head = null;
    final RID headRID = directory.getHead(generation, slot);
    if (headRID != null) {
      head = loadStripeHead(headRID);
      if (append(head, edgeRID, vertexRID))
        return;
    }

    // SLOT-WRITE PATH (stripe's first chunk, or head full): work on a FRESH, page-anchored view of the
    // directory - the anchored-at-read page closes the deferred-update MVCC gap (#5147) and bypassing the tx
    // record cache avoids writing a stale copy over a concurrent slot change. This is the rare path
    // (~once per ~1000 appends per stripe).
    final StripeDirectory fresh = loadDirectoryForWrite();
    final RID freshHeadRID = fresh.getHead(generation, slot);

    if (freshHeadRID == null) {
      // FIRST EDGE OF THIS STRIPE: ALLOCATE ITS CHAIN LAZILY IN THE POOL BUCKET
      final MutableEdgeSegment firstChunk = new MutableEdgeSegment(database, LocalDatabase.getNewEdgeListSize(0));
      firstChunk.add(edgeRID, vertexRID);
      database.createRecord(firstChunk, stripeBucketName(vertex.getTypeName(), slot));
      updateSlot(fresh, generation, slot, firstChunk.getIdentity());
      return;
    }

    if (!freshHeadRID.equals(headRID)) {
      // A CONCURRENT TRANSACTION ALREADY REPLACED THE HEAD: APPEND INTO THE CURRENT ONE
      head = loadStripeHead(freshHeadRID);
      if (append(head, edgeRID, vertexRID))
        return;
    }

    // STRIPE HEAD FULL: the new chunk becomes this stripe's head, recorded in the DIRECTORY (the vertex
    // record is not touched - only promotion rewrites it).
    final MutableEdgeSegment newChunk = new MutableEdgeSegment(database,
        LocalDatabase.getNewEdgeListSize(head.getRecordSize()));
    newChunk.add(edgeRID, vertexRID);
    newChunk.setPrevious(head);
    database.createRecord(newChunk, database.getSchema().getBucketById(head.getIdentity().getBucketId()).getName());
    updateSlot(fresh, generation, slot, newChunk.getIdentity());
  }

  /**
   * Loads a stripe head chunk for write, mapping a transient miss to a retryable conflict: the directory page
   * and the stripe chunk page of a concurrent commit are published one page at a time (readers take no commit
   * lock), so a freshly-read head RID can momentarily point to a record whose page is not visible yet. That is
   * transient by construction - surfacing it as {@link ConcurrentModificationException} lets the transaction
   * retry loop re-read a consistent view instead of failing with a spurious "record not found".
   */
  private EdgeSegment loadStripeHead(final RID headRID) {
    try {
      return loadChunkForWrite(headRID);
    } catch (final RecordNotFoundException e) {
      throw new ConcurrentModificationException(
          "Stripe head chunk " + headRID + " not visible yet (concurrent commit in flight on vertex " + vertex.getIdentity() + ")");
    }
  }

  /** In-place append + merge tracking; false if the chunk is full. */
  private boolean append(final EdgeSegment chunk, final RID edgeRID, final RID vertexRID) {
    if (!chunk.add(edgeRID, vertexRID))
      return false;
    database.updateRecord(chunk);
    // Commutative in-chunk append: track it so a commit-time page conflict rebases instead of retrying.
    final TransactionContext tx = database.getTransactionIfExists();
    if (tx != null)
      tx.trackEdgeAppend(chunk.getIdentity(), edgeRID, vertexRID);
    return true;
  }

  @Override
  public void addAll(final List<Pair<Identifiable, Identifiable>> entries) {
    for (int i = 0; i < entries.size(); ++i) {
      final Pair<Identifiable, Identifiable> entry = entries.get(i);
      add(entry.getFirst() != null ? entry.getFirst().getIdentity() : null, entry.getSecond().getIdentity());
    }
  }

  @Override
  public Iterator<Pair<RID, RID>> entryIterator(final String... edgeTypes) {
    final MultiIterator<Pair<RID, RID>> iterator = new MultiIterator<>();
    for (final EdgeLinkedList chain : allChains(false))
      iterator.addIterator(chain.entryIterator(edgeTypes));
    return iterator;
  }

  @Override
  public Iterator<Edge> edgeIterator(final String... edgeTypes) {
    final MultiIterator<Edge> iterator = new MultiIterator<>();
    for (final EdgeLinkedList chain : allChains(false))
      iterator.addIterator(chain.edgeIterator(edgeTypes));
    return iterator;
  }

  @Override
  public Iterator<Vertex> vertexIterator(final String... edgeTypes) {
    final MultiIterator<Vertex> iterator = new MultiIterator<>();
    for (final EdgeLinkedList chain : allChains(false))
      iterator.addIterator(chain.vertexIterator(edgeTypes));
    return iterator;
  }

  @Override
  public Iterator<RID> ridIterator(final String... edgeTypes) {
    final MultiIterator<RID> iterator = new MultiIterator<>();
    for (final EdgeLinkedList chain : allChains(false))
      iterator.addIterator(chain.ridIterator(edgeTypes));
    return iterator;
  }

  @Override
  public boolean containsEdge(final RID rid) {
    for (final EdgeLinkedList chain : allChains(false))
      if (chain.containsEdge(rid))
        return true;
    return false;
  }

  @Override
  public RID getFirstEdgeConnectedToVertex(final RID ridVertex, final int[] edgeBucketFilter) {
    for (final EdgeLinkedList chain : chainsForNeighbour(ridVertex)) {
      final RID edge = chain.getFirstEdgeConnectedToVertex(ridVertex, edgeBucketFilter);
      if (edge != null)
        return edge;
    }
    return null;
  }

  @Override
  public boolean containsVertex(final RID rid, final int[] edgeBucketFilter) {
    for (final EdgeLinkedList chain : chainsForNeighbour(rid))
      if (chain.containsVertex(rid, edgeBucketFilter))
        return true;
    return false;
  }

  @Override
  public long count(final String... edgeTypes) {
    long total = 0;
    for (final EdgeLinkedList chain : allChains(false))
      total += chain.count(edgeTypes);
    return total;
  }

  @Override
  public JSONArray toJSON() {
    final JSONArray array = new JSONArray();
    for (final EdgeLinkedList chain : allChains(false)) {
      final JSONArray chainArray = chain.toJSON();
      for (int i = 0; i < chainArray.length(); ++i)
        array.put(chainArray.getString(i));
    }
    return array;
  }

  @Override
  public void removeEdge(final Edge edge) {
    // The edge lives in the stripe of its neighbour (per generation); the non-owning candidate chains no-op.
    for (final EdgeLinkedList chain : chainsForNeighbour(direction == Vertex.DIRECTION.OUT ? edge.getIn() : edge.getOut()))
      chain.removeEdge(edge);
  }

  @Override
  public void removeEdgeRID(final RID edge) {
    // No neighbour information: check every chain (uncommon path).
    for (final EdgeLinkedList chain : allChains(true))
      chain.removeEdgeRID(edge);
  }

  @Override
  public void removeVertex(final RID vertexRID) {
    for (final EdgeLinkedList chain : chainsForNeighbour(vertexRID))
      chain.removeVertex(vertexRID);
  }

  @Override
  public void deleteAll() {
    for (final EdgeLinkedList chain : allChains(true))
      chain.deleteAll();

    // The directory delete does not commute with concurrent appends on its page: exclude it from the merge.
    final TransactionContext tx = database.getTransactionIfExists();
    if (tx != null)
      tx.poisonEdgeAppendPage(directory.getIdentity());
    directory.delete();
  }

  /**
   * Every non-null chain across all generations, NEWEST generation first. Generations hold disjoint entries: no
   * deduplication needed. NOTE: within one generation the stripes have no cross-stripe insertion order, so a
   * promoted super-node does NOT preserve the classic reverse-insertion iteration order (#689) - only a rough
   * newest-era-first approximation (newest generation's stripes, then older generations, each chain itself
   * newest-first).
   */
  private List<EdgeLinkedList> allChains(final boolean strict) {
    final List<EdgeLinkedList> chains = new ArrayList<>(directory.getChainCount());
    for (int g = directory.getGenerationCount() - 1; g >= 0; g--)
      for (int s = 0; s < directory.getStripes(g); s++)
        addChain(chains, directory.getHead(g, s), strict);
    return chains;
  }

  /**
   * The (at most one per generation) chains that can contain entries for the given neighbour vertex:
   * generation 0 is the pre-promotion chain (all neighbours), later generations localise by hash.
   */
  private List<EdgeLinkedList> chainsForNeighbour(final RID neighbour) {
    final List<EdgeLinkedList> chains = new ArrayList<>(directory.getGenerationCount());
    for (int g = 0; g < directory.getGenerationCount(); g++)
      // ALWAYS STRICT: these chains feed removals (a silently skipped chain would leave a stale back-reference
      // in a "successfully" committed transaction) and the neighbour-keyed lookups (isConnectedTo,
      // containsVertex, getFirstEdgeConnectedToVertex) that MERGE-style dedup decisions rely on - a transient
      // false-negative there could let a duplicate through. The transient miss becomes a retryable conflict.
      addChain(chains, directory.getHead(g, StripeDirectory.stripeOf(neighbour, directory.getStripes(g))), true);
    return chains;
  }

  private void addChain(final List<EdgeLinkedList> chains, final RID headRID, final boolean strict) {
    if (headRID == null)
      return;
    try {
      chains.add(new EdgeLinkedList(vertex, direction, (EdgeSegment) database.lookupByRID(headRID, true)));
    } catch (final RecordNotFoundException e) {
      if (strict)
        // MUTATING (or dedup-feeding) caller: skipping the chain would silently commit an incomplete operation
        // (e.g. a removal leaving a stale back-reference). The miss is transient by construction (cross-file
        // commit publication) - surface it as a retryable conflict, symmetric with loadStripeHead.
        throw new ConcurrentModificationException(
            "Stripe chain " + headRID + " of vertex " + vertex.getIdentity() + " not visible yet (concurrent commit in flight)");
      final long now = System.currentTimeMillis();
      final long last = LAST_SKIPPED_CHAIN_WARN.get();
      if (now - last > 60_000 && LAST_SKIPPED_CHAIN_WARN.compareAndSet(last, now))
        LogManager.instance()
            .log(this, Level.WARNING, "Cannot load stripe chain %s for vertex %s: skipped from this read (throttled warning)", e,
                headRID, vertex.getIdentity());
    }
  }

  /**
   * Loads a fresh copy of the directory from its page ANCHORED in the current transaction: the anchor makes a
   * concurrent directory rewrite fail the commit-time MVCC check (no lost update, #5147), and reading straight
   * from the bucket bypasses a possibly stale tx-cached instance.
   */
  private StripeDirectory loadDirectoryForWrite() {
    final RID dirRID = directory.getIdentity();
    final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(dirRID.getBucketId());
    try {
      bucket.fetchPageInTransaction(dirRID);
    } catch (final IOException e) {
      throw new DatabaseOperationException("Error on loading stripe directory page " + dirRID, e);
    }
    // Prefer the transaction's own WRITTEN copy of the directory: slot updates are DEFERRED (updated-records),
    // so a raw page read here would resurrect the pre-update state and each subsequent updateRecord would
    // silently ERASE the slot updates made earlier in this same transaction (orphaning their chunks - lost
    // edges). Only the WRITTEN copy qualifies: a read-only cached copy (factory load) may be STALE relative to
    // the page just anchored above, and trusting it would re-flip a slot over a concurrently committed head
    // with no MVCC conflict (the page was anchored at the current version) - orphaning that chunk instead.
    final TransactionContext tx = database.getTransactionIfExists();
    if (tx != null && tx.getWrittenRecord(dirRID) instanceof StripeDirectory inTx)
      return inTx;
    return new StripeDirectory(database, dirRID, bucket.getRecord(dirRID).copyOfContent());
  }

  private void updateSlot(final StripeDirectory dir, final int generation, final int slot, final RID head) {
    dir.setHead(generation, slot, head);
    database.updateRecord(dir);
    // Keep the instance view current: within a multi-append transaction (addAll, promotion) the next append to
    // this stripe reads the instance directory on the FAST path - without the refresh it would see the stale
    // pre-flip head and detour through the slot-write path on every subsequent append.
    this.directory = dir;
    // The slot rewrite does not commute with concurrent appends on the directory's page: poison it so the
    // commit-time merge can never rebase over it.
    final TransactionContext tx = database.getTransactionIfExists();
    if (tx != null)
      tx.poisonEdgeAppendPage(dir.getIdentity());
  }
}
