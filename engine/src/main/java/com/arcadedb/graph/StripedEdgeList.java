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

  private final DatabaseInternal database;
  private final StripeDirectory  directory;

  public StripedEdgeList(final Vertex vertex, final Vertex.DIRECTION direction, final StripeDirectory directory) {
    super(vertex, direction, null);
    this.database = (DatabaseInternal) vertex.getDatabase();
    this.directory = directory;
  }

  /** Name of the {@code slot}-th stripe bucket of the per-type pool (shared by both directions). */
  public static String stripeBucketName(final String typeName, final int slot) {
    return typeName + STRIPE_BUCKET_INFIX + slot;
  }

  /** Guards against stampeding pool creations: one in-flight creation per database+type. */
  private static final Set<String> POOLS_IN_CREATION = ConcurrentHashMap.newKeySet();

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
    for (int i = 0; i < stripes; i++)
      if (!schema.existsBucket(stripeBucketName(typeName, i))) {
        ready = false;
        break;
      }
    if (ready)
      return true;

    if (database.getEmbedded() == database) {
      // EMBEDDED: SCHEMA OPS ARE LOCAL AND NON-TRANSACTIONAL, SAFE TO CREATE INLINE
      try {
        createStripePool(database, typeName, stripes);
        return true;
      } catch (final Exception e) {
        LogManager.instance()
            .log(StripedEdgeList.class, Level.WARNING, "Cannot create super-node stripe buckets for type '%s': promotion skipped (%s)",
                typeName, e.getMessage());
        return false;
      }
    }

    // WRAPPED (SERVER/HA) DATABASE: DEFER THE DDL OUTSIDE THE CURRENT TRANSACTION
    final String key = database.getDatabasePath() + '|' + typeName;
    if (POOLS_IN_CREATION.add(key)) {
      // NOTE (concurrency): not engine data-path parallelism but a once-per-type lifecycle action, hence a
      // short-lived dedicated thread instead of a shared pool (see the QueryEngineManager rule in CLAUDE.md).
      final Thread creator = new Thread(() -> {
        try {
          createStripePool(database, typeName, stripes);
        } catch (final Exception e) {
          LogManager.instance()
              .log(StripedEdgeList.class, Level.WARNING, "Error creating super-node stripe buckets for type '%s': promotion deferred (%s)",
                  typeName, e.getMessage());
        } finally {
          POOLS_IN_CREATION.remove(key);
        }
      }, "arcadedb-supernode-pool-" + typeName);
      creator.setDaemon(true);
      creator.start();
    }
    return false;
  }

  private static void createStripePool(final DatabaseInternal database, final String typeName, final int stripes) {
    final Schema schema = database.getSchema();
    for (int i = 0; i < stripes; i++) {
      final String bucketName = stripeBucketName(typeName, i);
      if (!schema.existsBucket(bucketName))
        try {
          schema.createBucket(bucketName);
        } catch (final Exception e) {
          if (!schema.existsBucket(bucketName))
            // NOT A LOST RACE WITH A CONCURRENT CREATION: GIVE UP
            throw e;
        }
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
      head = loadChunkForWrite(headRID);
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
      head = loadChunkForWrite(freshHeadRID);
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
    for (final EdgeLinkedList chain : allChains())
      iterator.addIterator(chain.entryIterator(edgeTypes));
    return iterator;
  }

  @Override
  public Iterator<Edge> edgeIterator(final String... edgeTypes) {
    final MultiIterator<Edge> iterator = new MultiIterator<>();
    for (final EdgeLinkedList chain : allChains())
      iterator.addIterator(chain.edgeIterator(edgeTypes));
    return iterator;
  }

  @Override
  public Iterator<Vertex> vertexIterator(final String... edgeTypes) {
    final MultiIterator<Vertex> iterator = new MultiIterator<>();
    for (final EdgeLinkedList chain : allChains())
      iterator.addIterator(chain.vertexIterator(edgeTypes));
    return iterator;
  }

  @Override
  public Iterator<RID> ridIterator(final String... edgeTypes) {
    final MultiIterator<RID> iterator = new MultiIterator<>();
    for (final EdgeLinkedList chain : allChains())
      iterator.addIterator(chain.ridIterator(edgeTypes));
    return iterator;
  }

  @Override
  public boolean containsEdge(final RID rid) {
    for (final EdgeLinkedList chain : allChains())
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
    for (final EdgeLinkedList chain : allChains())
      total += chain.count(edgeTypes);
    return total;
  }

  @Override
  public JSONArray toJSON() {
    final JSONArray array = new JSONArray();
    for (final EdgeLinkedList chain : allChains()) {
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
    for (final EdgeLinkedList chain : allChains())
      chain.removeEdgeRID(edge);
  }

  @Override
  public void removeVertex(final RID vertexRID) {
    for (final EdgeLinkedList chain : chainsForNeighbour(vertexRID))
      chain.removeVertex(vertexRID);
  }

  @Override
  public void deleteAll() {
    for (final EdgeLinkedList chain : allChains())
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
  private List<EdgeLinkedList> allChains() {
    final List<EdgeLinkedList> chains = new ArrayList<>(directory.getChainCount());
    for (int g = directory.getGenerationCount() - 1; g >= 0; g--)
      for (int s = 0; s < directory.getStripes(g); s++)
        addChain(chains, directory.getHead(g, s));
    return chains;
  }

  /**
   * The (at most one per generation) chains that can contain entries for the given neighbour vertex:
   * generation 0 is the pre-promotion chain (all neighbours), later generations localise by hash.
   */
  private List<EdgeLinkedList> chainsForNeighbour(final RID neighbour) {
    final List<EdgeLinkedList> chains = new ArrayList<>(directory.getGenerationCount());
    for (int g = 0; g < directory.getGenerationCount(); g++)
      addChain(chains, directory.getHead(g, StripeDirectory.stripeOf(neighbour, directory.getStripes(g))));
    return chains;
  }

  private void addChain(final List<EdgeLinkedList> chains, final RID headRID) {
    if (headRID == null)
      return;
    try {
      chains.add(new EdgeLinkedList(vertex, direction, (EdgeSegment) database.lookupByRID(headRID, true)));
    } catch (final RecordNotFoundException e) {
      LogManager.instance()
          .log(this, Level.WARNING, "Cannot load stripe chain %s for vertex %s", e, headRID, vertex.getIdentity());
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
    return new StripeDirectory(database, dirRID, bucket.getRecord(dirRID).copyOfContent());
  }

  private void updateSlot(final StripeDirectory dir, final int generation, final int slot, final RID head) {
    dir.setHead(generation, slot, head);
    database.updateRecord(dir);
    // The slot rewrite does not commute with concurrent appends on the directory's page: poison it so the
    // commit-time merge can never rebase over it.
    final TransactionContext tx = database.getTransactionIfExists();
    if (tx != null)
      tx.poisonEdgeAppendPage(dir.getIdentity());
  }
}
