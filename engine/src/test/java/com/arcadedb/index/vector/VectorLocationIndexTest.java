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
package com.arcadedb.index.vector;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.ImmutablePage;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LSMVectorIndexMetadata;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.TypeLSMVectorIndexBuilder;
import com.arcadedb.schema.VertexType;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntConsumer;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The vector location index and its cache. Each nested class keeps the fixture of the regression test it was folded from.
 */
class VectorLocationIndexTest {
  /**
   * Regression for #5559: location cache size.
   * <p>
   * Regression test for issue #5559: a {@code locationCacheSize} on an {@code LSM_VECTOR} index used to truncate it
   * instead of bounding a cache.
   * <p>
   * The location index is not a cache in front of an authoritative set - it IS the authoritative set. A location is
   * the only record of which record a vector id belongs to and where its entry sits in the index file, and nothing on
   * disk maps a vector id back to an offset. Bounding it therefore did not spill to a slower tier: the graph was
   * rebuilt over the surviving entries only, {@code countEntries()} reported the cap instead of the live count, and a
   * search probing a vector with its own embedding answered with some other vertex. The report was taken with 200
   * vectors, 16 dimensions, INT8 quantization and a cap of 10.
   * <p>
   * Issue #5568 stopped honouring the setting and this issue removed the bounded backend outright, so the three
   * entrances now behave differently and each is pinned below:
   * <ul>
   *   <li>the {@code METADATA} clause and the Java builder <b>refuse</b> a positive value, rather than accepting one
   *       and leaving a bound in the schema that is not in force;</li>
   *   <li>the global setting is still tolerated - refusing it would stop a server booting off an existing startup
   *       line - and does not truncate the index;</li>
   *   <li>a definition persisted by an older version still loads, or the database would not open.</li>
   * </ul>
   * The self-recall probes under the global setting are the load-bearing behavioural assertions:
   * {@code countEntries()} alone would still pass on an index whose graph had been built over a truncated set,
   * because a count and a graph traversal read different state.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class LocationCacheSize extends TestHelper {

    private static final int DIMENSIONS         = 16;
    private static final int NUM_VECTORS        = 200;
    private static final int LOCATION_CACHE_CAP = 10;

    /**
     * The reported reproduction, which reached the index through the per-index METADATA clause. The statement is now
     * refused with a message that says why and what to size instead, and no index is left behind.
     */
    @Test
    void perIndexLocationCacheSizeMetadataIsRefused() {
      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX TYPE Doc");
        database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
      });

      assertThatThrownBy(() -> database.command("sql", """
          CREATE INDEX ON Doc (embedding) LSM_VECTOR \
          METADATA {"dimensions": %d, "similarity": "COSINE", "quantization": "INT8", "locationCacheSize": %d}\
          """.formatted(DIMENSIONS, LOCATION_CACHE_CAP)))
          // A parsing exception, not an execution one: it is what AbstractServerHttpHandler's 400 arm keys on, so
          // this type is what keeps the refusal a client error over HTTP rather than a 500 carrying a helpful
          // message nobody displays. Pinned end to end by Issue5559LocationCacheSizeHttpIT in the server module.
          .isInstanceOf(CommandSQLParsingException.class)
          .hasRootCauseInstanceOf(IndexException.class)
          .hasMessageContaining("locationCacheSize")
          .hasMessageContaining("no longer supported")
          .hasMessageContaining(
              VectorLocationIndex.APPROX_RETAINED_BYTES_PER_LOCATION + " bytes per live vector");

      assertThat(database.getSchema().existsIndex("Doc[embedding]"))
          .as("a refused statement must not leave a half-built index").isFalse();

      // "no limit" is what an unset builder and a metadata copy carry, so it has to stay accepted.
      assertThatCode(() -> database.command("sql", """
          CREATE INDEX ON Doc (embedding) LSM_VECTOR \
          METADATA {"dimensions": %d, "similarity": "COSINE", "locationCacheSize": -1}\
          """.formatted(DIMENSIONS))).doesNotThrowAnyException();
    }

    /** The Java builder is the second user-facing entrance and refuses through the same setter. */
    @Test
    void theJavaBuilderRefusesALocationCacheSize() {
      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX TYPE Doc");
        database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
      });

      assertThatThrownBy(() -> database.getSchema().buildTypeIndex("Doc", new String[] { "embedding" })
          .withLSMVectorType()
          .withDimensions(DIMENSIONS)
          .withLocationCacheSize(LOCATION_CACHE_CAP)
          .create())
          .hasMessageContaining("locationCacheSize")
          .hasMessageContaining("no longer supported");

      assertThat(database.getSchema().existsIndex("Doc[embedding]")).isFalse();

      // "no limit" keeps working on the builder too, and keeps returning the builder so an existing fluent chain
      // carrying it still composes. Only the SQL entrance was pinned for this above.
      final TypeLSMVectorIndexBuilder builder = database.getSchema().buildTypeIndex("Doc", new String[] { "embedding" })
          .withLSMVectorType()
          .withDimensions(DIMENSIONS);
      assertThat(builder.withLocationCacheSize(-1)).as("the fluent chain continues").isSameAs(builder);

      builder.create();
      final TypeIndex created = (TypeIndex) database.getSchema().getIndexByName("Doc[embedding]");
      assertThat(((LSMVectorIndex) created.getIndexesOnBuckets()[0]).getMetadata().locationCacheSize)
          .as("-1 means 'no limit', which is what the index does").isEqualTo(-1);
    }

    /**
     * A schema written by a version that still honoured the setting must keep loading: {@code fromJSON} reads a
     * complete persisted definition and does not go through the refusing setter, because refusing there would make an
     * existing database unopenable.
     */
    @Test
    void aPersistedDefinitionCarryingTheSettingStillLoads() {
      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX TYPE Doc");
        database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
        database.command("sql", """
            CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {"dimensions": %d, "similarity": "COSINE"}\
            """.formatted(DIMENSIONS));
      });

      final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Doc[embedding]");
      final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];

      // The shape LocalSchema reads back on open, with the key an older version wrote into it.
      final JSONObject persisted = lsmIndex.toJSON().put("locationCacheSize", LOCATION_CACHE_CAP);

      final LSMVectorIndexMetadata reloaded = new LSMVectorIndexMetadata(null, new String[0], -1);
      assertThatCode(() -> reloaded.fromJSON(persisted)).as("an old schema must still open").doesNotThrowAnyException();

      assertThat(reloaded.locationCacheSize)
          .as("the value round-trips so the schema is not rewritten behind the user's back")
          .isEqualTo(LOCATION_CACHE_CAP);
    }

    /**
     * The global setting is the entrance that stays tolerated, and this is the behavioural regression guard: with the
     * cap configured, every vector must remain counted and reachable. Restoring eviction makes
     * {@code countEntries()} answer 10 instead of 200.
     */
    @Test
    void theGlobalSettingDoesNotTruncateTheIndex() {
      // Per-database, so it is dropped with the database and cannot leak into the rest of the suite.
      database.getConfiguration().setValue(GlobalConfiguration.VECTOR_INDEX_LOCATION_CACHE_SIZE, LOCATION_CACHE_CAP);

      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX TYPE Doc");
        database.command("sql", "CREATE PROPERTY Doc.name STRING");
        database.command("sql", "CREATE PROPERTY Doc.embedding ARRAY_OF_FLOATS");
        database.command("sql", """
            CREATE INDEX ON Doc (embedding) LSM_VECTOR \
            METADATA {"dimensions": %d, "similarity": "COSINE", "quantization": "INT8"}\
            """.formatted(DIMENSIONS));
      });

      database.transaction(() -> {
        for (int i = 0; i < NUM_VECTORS; i++) {
          final var vertex = database.newVertex("Doc");
          vertex.set("name", "doc" + i);
          vertex.set("embedding", embeddingOf(i));
          vertex.save();
        }
      });

      final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Doc[embedding]");
      final LSMVectorIndex lsmIndex = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];

      assertThat(typeIndex.countEntries())
          .as("Every indexed vector must stay resident with a location cache cap of " + LOCATION_CACHE_CAP)
          .isEqualTo(NUM_VECTORS);

      // A cap of 10 kept only the last-inserted handful reachable, so probing an early vector answered with a late
      // one. Probe across the whole insertion range, including the ids that used to be evicted first.
      for (final int probe : new int[] { 0, 5, 42, 100, 150, NUM_VECTORS - 1 }) {
        final IndexCursor cursor = lsmIndex.get(new Object[] { embeddingOf(probe) }, 1);
        assertThat(cursor.hasNext()).as("A probe for doc" + probe + " must return a neighbour").isTrue();
        assertThat(cursor.next().asVertex().getString("name"))
            .as("Probing doc" + probe + " with its own embedding must return doc" + probe)
            .isEqualTo("doc" + probe);
      }

      // The same through SQL, which is how the report exercised it.
      try (final ResultSet rs = database.query("sql",
          "SELECT name FROM (SELECT expand(vectorNeighbors('Doc[embedding]', ?, 1)))", (Object) embeddingOf(5))) {
        assertThat(rs.hasNext()).as("vectorNeighbors() must answer").isTrue();
        assertThat(rs.next().<Object>getProperty("name")).as("vectorNeighbors() must find doc5 from its own embedding")
            .isEqualTo("doc5");
      }
    }

    /**
     * Well separated embeddings: a per-vertex pseudo-random direction from a fixed seed, so the run is deterministic
     * and the nearest neighbour of {@code embeddingOf(i)} under COSINE is unambiguously vertex {@code i}. A structured
     * fixture (one dominant dimension, say) would not do - COSINE ignores magnitude, so two vertices sharing a
     * dominant dimension sit within quantization noise of each other.
     */
    private static float[] embeddingOf(final int i) {
      final Random random = new Random(i * 31L + 7);
      final float[] embedding = new float[DIMENSIONS];
      for (int d = 0; d < DIMENSIONS; d++)
        embedding[d] = random.nextFloat() * 2 - 1;
      return embedding;
    }
  }

  /**
   * Regression for #5588: primitive location index.
   * <p>
   * Issue #5588: {@link VectorLocationIndex} lays its locations out in primitive arrays indexed by vector id instead
   * of in two {@code ConcurrentHashMap}s, taking the retained heap from ~90 bytes per live vector to ~32.
   * <p>
   * Every test here is written against a specific wrong implementation, because most of what this rewrite has to get
   * right is invisible in a functional test:
   * <ul>
   *   <li>a FLAT id-indexed array instead of released chunks - functionally perfect, and it retains ~190MB where the
   *       map it replaces retained ~360KB on the workload issue #5516 exists for</li>
   *   <li>the offset and its compacted flag in two arrays instead of one word - correct until a replica re-points a
   *       live id at the compacted file mid-search</li>
   *   <li>a reverse index that loses an entry across a rehash, or that sizes the rehash from its capacity rather than
   *       from the live count</li>
   *   <li>an id traversal that is not ascending, which silently breaks the {@code Arrays.binarySearch} the
   *       allow-list search does over {@code ordinalToVectorId}</li>
   * </ul>
   * The one thing deliberately NOT tested is the release/acquire on the presence word itself: dropping it is
   * invisible on x86, so a green test would only be evidence about the hardware it ran on.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class PrimitiveLocationIndex {

    /**
     * The reason the arrays are chunked. Ids are handed out monotonically and an update tombstones the id it
     * supersedes, so a re-embedding workload leaves a live set orders of magnitude smaller than the id space: issue
     * #5516 measured 9.3M ids for 4K live vectors. Residency has to follow the live set, and a flat array indexed by
     * id follows the id space instead.
     */
    @Test
    void aDrainedIdSpaceIsHandedBack() {
      final VectorLocationIndex index = new VectorLocationIndex();

      final int liveVectors = 100;
      for (int i = 0; i < liveVectors; i++)
        index.addVector(false, 1000 + i, new RID(1, i));

      // Hand out and immediately retire ids the way re-embedding does.
      final int churn = 200_000;
      for (int i = 0; i < churn; i++)
        index.markDeleted(index.addVector(false, 500_000 + i, new RID(2, i)));

      assertThat(index.getNextId()).as("every id was really handed out").isEqualTo(liveVectors + churn);
      assertThat(index.size()).as("and only the pinned ones are live").isEqualTo(liveVectors);
      assertThat(index.getDeletedCount()).isEqualTo(churn);

      assertThat(index.chunkCount())
          .as("the drained chunks must be released, not kept for the ids they no longer hold")
          .isLessThanOrEqualTo(4);

      // A flat array would be ~21 bytes per id HANDED OUT (~4.2MB here). The bound is expressed per id handed out
      // precisely so that layout fails it: what survives is the deleted-id bitset, which issue #5516 already
      // accepted at one bit per id.
      assertThat(index.estimatedRetainedBytes())
          .as("residency must follow the live vectors, not the id space")
          .isLessThan(4L * (liveVectors + churn));

      // ...and the live set is still intact, so the release did not take anything with it.
      for (int i = 0; i < liveVectors; i++) {
        assertThat(index.isLive(i)).as("live id %d", i).isTrue();
        assertThat(index.getRid(i)).isEqualTo(new RID(1, i));
        assertThat(VectorLocationIndex.offsetOf(index.getOffsetAndFlag(i))).isEqualTo(1000 + i);
        assertThat(index.getVectorIdsForRid(new RID(1, i))).containsExactly(i);
      }
    }

    /**
     * The offset and the compacted flag must come out of one 64-bit word.
     * <p>
     * {@code LSMVectorIndex.applyReplicatedPageUpdate} re-points a live id at the compacted file, on a replica, with
     * no index lock held, while searches run. Split across two arrays, a reader can pair the new flag with the stale
     * offset; {@code readVectorFromOffset} then reads a misaligned region of an INT8/BINARY page, which parses into a
     * well-formed vector of the right dimension that passes every guard and is scored at the wrong distance.
     * <p>
     * The writer here encodes the flag in the offset, so a torn read is not a rare interleaving to get lucky with -
     * it is wrong on about half of them, and the test fails in milliseconds.
     */
    @Test
    void theOffsetAndItsCompactedFlagCannotBeReadFromDifferentGenerations() throws Exception {
      final VectorLocationIndex index = new VectorLocationIndex();
      final long base = 1_000_000L;
      final RID rid = new RID(7, 42);
      // Generation 0 of the writer below (n = 0: compacted, offset base). The readers hold every generation to "compacted
      // iff (offset - base) is even", so a seed that breaks the rule is reported as a torn read by whichever reader runs
      // before the writer's first update (#10165)
      final int id = index.addVector(true, base, rid);

      final AtomicBoolean stop = new AtomicBoolean();
      final AtomicReference<String> failure = new AtomicReference<>();
      final AtomicLong reads = new AtomicLong();
      final int readerCount = 4;
      // The writer starts only once every reader has read the seeded generation, so the seed is checked against the
      // invariant on every run instead of whenever a loaded machine happens to schedule a reader first (#10165)
      final CountDownLatch seedRead = new CountDownLatch(readerCount);

      final Thread writer = new Thread(() -> {
        try {
          // A hang detector, not a latency bound: a reader that never got to its first read must fail the test, not park the writer
          if (!seedRead.await(30, TimeUnit.SECONDS)) {
            failure.compareAndSet(null, "a reader never read the seeded generation");
            return;
          }
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
        for (long n = 0; !stop.get(); n++)
          index.addOrUpdate(id, n % 2 == 0, base + n, rid, false);
      }, "issue5588-writer");

      final List<Thread> readers = new ArrayList<>();
      for (int r = 0; r < readerCount; r++)
        readers.add(new Thread(() -> {
          boolean first = true;
          while (!stop.get()) {
            final long packed = index.getOffsetAndFlag(id);
            if (first) {
              first = false;
              seedRead.countDown();
            }
            if (packed == VectorLocationIndex.ABSENT) {
              failure.compareAndSet(null, "the id was retired by an update that did not change its RID");
              return;
            }
            final long offset = VectorLocationIndex.offsetOf(packed);
            final boolean compacted = VectorLocationIndex.isCompactedOf(packed);
            if (compacted != ((offset - base) % 2 == 0)) {
              failure.compareAndSet(null,
                  "torn read: offset " + offset + " came from a different generation than isCompacted " + compacted);
              return;
            }
            reads.incrementAndGet();
          }
        }, "issue5588-reader-" + r));

      writer.start();
      readers.forEach(Thread::start);
      Thread.sleep(500);
      stop.set(true);
      // Longer than the writer's own wait for the seed read, so a writer that gave up has already said so when this returns
      writer.join(60_000);
      for (final Thread reader : readers)
        reader.join(30_000);

      assertThat(failure.get()).isNull();
      assertThat(writer.isAlive()).as("the writer must have terminated").isFalse();
      assertThat(readers).allSatisfy(reader -> assertThat(reader.isAlive()).as(reader.getName()).isFalse());
      assertThat(reads.get()).as("the readers must really have observed the id being rewritten").isGreaterThan(1000);
    }

    /**
     * The reverse index must never lose a live entry, however hard the table is churned.
     * <p>
     * Everything churned here shares the pinned RID, so every dead id lands in the same probe chain and the table
     * rehashes constantly. Two mutations die on this: publishing a rehashed table before it is fully populated, and
     * sizing the rehash from the current capacity instead of from the live count - the latter grows the table without
     * bound, because the pressure on it is dead entries and the live set never moves.
     */
    @Test
    void theReverseIndexNeverLosesALiveEntryAcrossARehash() throws Exception {
      final VectorLocationIndex index = new VectorLocationIndex();
      final RID rid = new RID(3, 99);
      final int pinned = index.addVector(false, 10, rid);

      final AtomicBoolean stop = new AtomicBoolean();
      final AtomicReference<String> failure = new AtomicReference<>();
      final AtomicInteger churned = new AtomicInteger();
      final CountDownLatch started = new CountDownLatch(1);

      final Thread writer = new Thread(() -> {
        started.countDown();
        while (!stop.get() && churned.get() < 100_000) {
          final int id = index.addVector(false, 1000 + churned.get(), rid);
          index.markDeleted(id);
          churned.incrementAndGet();
        }
      }, "issue5588-churn");

      final Thread reader = new Thread(() -> {
        try {
          started.await();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        }
        while (!stop.get()) {
          final int[] ids = index.getVectorIdsForRid(rid);
          boolean seen = false;
          for (final int id : ids)
            if (id == pinned)
              seen = true;
          if (!seen) {
            failure.compareAndSet(null, "the pinned id disappeared from the reverse index");
            return;
          }
          // Several ids can legitimately come back: the probe walk is not atomic, so ids the churn thread retired
          // between two steps of one walk were each genuinely live when they were verified. What is never legitimate
          // is the SAME id twice, which is one id owning two slots that both answer.
          for (int i = 1; i < ids.length; i++)
            if (ids[i] == ids[i - 1]) {
              failure.compareAndSet(null, "one id answered twice: " + Arrays.toString(ids));
              return;
            }
        }
      }, "issue5588-reverse-reader");

      writer.start();
      reader.start();
      writer.join(60_000);
      stop.set(true);
      reader.join(30_000);

      assertThat(failure.get()).isNull();
      assertThat(churned.get()).as("the churn must really have run").isEqualTo(100_000);
      assertThat(index.getVectorIdsForRid(rid)).as("and only the live id is mapped at the end").containsExactly(pinned);
      assertThat(index.reverseTableCapacity())
          .as("dead entries must be cleaned in place, not grow the table: the live set never left 1 entry")
          .isLessThanOrEqualTo(64);
    }

    /**
     * The id traversal is ascending and says so, which is a correctness claim rather than an optimisation.
     * <p>
     * {@code LSMVectorIndex} builds {@code ordinalToVectorId} from this stream and then reads that array with
     * {@code Arrays.binarySearch} to resolve an allow-listed RID to its graph ordinal. The {@code .sorted()} on the
     * way in is a no-op only because the spliterator reports {@link Spliterator#SORTED}; if the traversal were not
     * really ascending, the binary search would quietly stop finding ordinals and every allow-list-filtered search
     * would return fewer results, with nothing failing.
     */
    @Test
    void idsAreTraversedInAscendingOrderAndTheStreamSaysSo() {
      final VectorLocationIndex index = new VectorLocationIndex();
      index.addOrUpdate(5000, false, 1, new RID(1, 1), false);
      index.addOrUpdate(3, false, 2, new RID(1, 2), false);
      index.addOrUpdate(1_000_000, false, 3, new RID(1, 3), false);
      index.addOrUpdate(700, false, 4, new RID(1, 4), false);
      // Drains the chunk id 700 lived in, so the walk has to step over a released slot in the middle.
      index.markDeleted(700);

      assertThat(index.getAllVectorIds().toArray()).containsExactly(3, 5000, 1_000_000);
      assertThat(index.getActiveVectorIds().toArray()).containsExactly(3, 5000, 1_000_000);
      assertThat(index.getAllVectorIds().spliterator().hasCharacteristics(Spliterator.SORTED))
          .as("SORTED is what makes the .sorted() at the call sites a no-op")
          .isTrue();
      assertThat(index.getAllVectorIds().filter(id -> true).spliterator().hasCharacteristics(Spliterator.SORTED))
          .as("and it has to survive the filter() the call sites apply before sorting")
          .isTrue();
      assertThat(index.getAllVectorIds().sorted().toArray()).containsExactly(3, 5000, 1_000_000);

      // That the flag really elides the sort is a JDK behaviour worth pinning rather than asserting from memory:
      // IntPipeline.sorted() returns the sink unchanged when StreamOpFlag.SORTED is known, and the flag propagates
      // from a spliterator whose getComparator() is null. A spliterator that CLAIMS sorted and yields descending
      // values comes back descending if the sort was elided, and ascending if it was not.
      final Spliterator.OfInt liesAboutBeingSorted = new Spliterator.OfInt() {
        private int next = 5;

        @Override
        public boolean tryAdvance(final IntConsumer action) {
          if (next <= 0)
            return false;
          action.accept(next--);
          return true;
        }

        @Override
        public OfInt trySplit() {
          return null;
        }

        @Override
        public long estimateSize() {
          return 5;
        }

        @Override
        public int characteristics() {
          return ORDERED | DISTINCT | SORTED | NONNULL;
        }

        @Override
        public Comparator<? super Integer> getComparator() {
          return null;
        }
      };
      assertThat(StreamSupport.intStream(liesAboutBeingSorted, false).filter(id -> true).sorted().toArray())
          .as("IntStream.sorted() elides the sort when SORTED is known, so declaring it is load-bearing")
          .containsExactly(5, 4, 3, 2, 1);
      assertThat(index.getAllVectorIds().max().orElse(-1)).isEqualTo(1_000_000);

      assertThat(index.getNextId()).as("the sequence follows the highest id ever handed out").isEqualTo(1_000_001);
      assertThat(index.getMaxVectorId()).isEqualTo(1_000_000);
    }

    /**
     * Tombstoning an id that holds no location must not be counted.
     * <p>
     * {@code LSMVectorIndex} discards the persisted graph and rebuilds it from scratch whenever
     * {@code getDeletedCount() > 0} (issue #3135), so an unconditional count turns every database open into a full
     * rebuild - a large startup regression with no functional symptom to notice it by.
     */
    @Test
    void tombstoningAnIdThatHoldsNoLocationIsNotCounted() {
      final VectorLocationIndex index = new VectorLocationIndex();
      index.markDeleted(12_345);
      assertThat(index.getDeletedCount()).isZero();
      assertThat(index.isDeleted(12_345)).isFalse();

      final int id = index.addVector(false, 100, new RID(1, 1));
      index.markDeleted(id);
      index.markDeleted(id);
      assertThat(index.getDeletedCount()).as("a second tombstone on the same id is not a second deletion").isEqualTo(1);

      // The page-load path is deliberately different: a tombstone read off a page is recorded whether or not the
      // live entry preceding it was ever seen, because the id was handed out either way.
      index.addOrUpdate(999, false, 0, new RID(1, 2), true);
      assertThat(index.getDeletedCount()).isEqualTo(2);
      assertThat(index.isDeleted(999)).isTrue();
      assertThat(index.getNextId()).as("a tombstone still advances the sequence").isEqualTo(1000);
    }

    /**
     * A vector must be visible to a lock-free reader as soon as the writer that added it returns.
     * <p>
     * Honest about what this can and cannot kill: it exercises directory growth and chunk allocation against
     * concurrent readers, so it catches a reader that walks a stale directory length or dereferences a released
     * chunk. It does NOT prove the release/acquire on the directory slot - the handoff through the queue below is
     * itself an ordering edge, and on x86 a plain store would pass anyway. That ordering is enforced by review.
     */
    @Test
    void aVectorIsVisibleToAConcurrentReaderAsSoonAsItIsAdded() throws Exception {
      final VectorLocationIndex index = new VectorLocationIndex();
      final AtomicInteger published = new AtomicInteger(-1);
      final AtomicReference<String> failure = new AtomicReference<>();
      final int total = 200_000;

      final Thread reader = new Thread(() -> {
        int last = -1;
        while (last < total - 1 && failure.get() == null) {
          final int id = published.get();
          if (id < 0 || id == last)
            continue;
          last = id;
          if (!index.isLive(id)) {
            failure.compareAndSet(null, "id " + id + " was added but is not visible");
            return;
          }
          if (!index.isLocationOf(id, new RID(1, id))) {
            failure.compareAndSet(null, "id " + id + " resolved to the wrong RID");
            return;
          }
        }
      }, "issue5588-visibility-reader");

      reader.start();
      for (int i = 0; i < total; i++)
        published.set(index.addVector(false, 1000 + i, new RID(1, i)));
      reader.join(60_000);

      assertThat(failure.get()).isNull();
      assertThat(index.size()).isEqualTo(total);
    }

    /**
     * The whole observable surface, against a reference model, over a random operation sequence.
     * <p>
     * This is what catches the off-by-one in a live counter, a reverse-index entry that was never inserted, and the
     * id reuse {@link VectorLocationIndex#clear()} allows - it is the one call that resets the sequence, so it is the
     * one place a stale chunk could resolve a fresh id to the record the old one belonged to.
     * <p>
     * {@code size()} and {@code getActiveCount()} are asserted separately on purpose: the implementation derives them
     * from two independent things (a counter and a popcount over the presence bits), so agreeing is evidence and not
     * a tautology.
     */
    @Test
    void theIndexMatchesAReferenceModelUnderRandomOperations() {
      final VectorLocationIndex index = new VectorLocationIndex();
      final Map<Integer, long[]> liveModel = new HashMap<>();     // id -> {bucketId, position, offset, compacted}
      final Set<Integer> deletedModel = new HashSet<>();
      final Random random = new Random(5588);
      int modelNextId = 0;

      // A small RID pool so several ids can share one RID, which is what makes the reverse index non-trivial.
      final RID[] rids = new RID[16];
      for (int i = 0; i < rids.length; i++)
        rids[i] = new RID(i % 3, i);

      for (int step = 0; step < 4000; step++) {
        final int operation = random.nextInt(100);
        final RID rid = rids[random.nextInt(rids.length)];
        final long offset = 1 + random.nextInt(1_000_000);
        final boolean compacted = random.nextBoolean();

        if (operation < 45) {
          final int id = index.addVector(compacted, offset, rid);
          assertThat(id).as("addVector must hand out the next id in sequence").isEqualTo(modelNextId);
          liveModel.put(id, new long[] { rid.getBucketId(), rid.getPosition(), offset, compacted ? 1 : 0 });
          modelNextId++;
        } else if (operation < 70) {
          // An out-of-sequence id, the way the page-load path replays what the file holds.
          final int id = random.nextInt(modelNextId + 200);
          index.addOrUpdate(id, compacted, offset, rid, false);
          liveModel.put(id, new long[] { rid.getBucketId(), rid.getPosition(), offset, compacted ? 1 : 0 });
          deletedModel.remove(id);
          modelNextId = Math.max(modelNextId, id + 1);
        } else if (operation < 85) {
          final int id = random.nextInt(Math.max(1, modelNextId));
          index.addOrUpdate(id, compacted, offset, rid, true);
          liveModel.remove(id);
          deletedModel.add(id);
          modelNextId = Math.max(modelNextId, id + 1);
        } else if (operation < 98) {
          final int id = random.nextInt(Math.max(1, modelNextId));
          index.markDeleted(id);
          if (liveModel.remove(id) != null)
            deletedModel.add(id);
        } else {
          index.clear();
          liveModel.clear();
          deletedModel.clear();
          modelNextId = 0;
        }

        assertModelMatches(index, liveModel, deletedModel, modelNextId, rids, step);
      }
    }

    private static void assertModelMatches(final VectorLocationIndex index, final Map<Integer, long[]> liveModel,
        final Set<Integer> deletedModel, final int modelNextId, final RID[] rids, final int step) {
      assertThat(index.size()).as("size() at step %d", step).isEqualTo(liveModel.size());
      assertThat(index.getActiveCount()).as("getActiveCount() at step %d", step).isEqualTo(liveModel.size());
      assertThat(index.getDeletedCount()).as("getDeletedCount() at step %d", step).isEqualTo(deletedModel.size());
      assertThat(index.getNextId()).as("getNextId() at step %d", step).isEqualTo(modelNextId);

      final int[] expectedIds = liveModel.keySet().stream().mapToInt(Integer::intValue).sorted().toArray();
      assertThat(index.getAllVectorIds().toArray()).as("the id traversal at step %d", step).containsExactly(expectedIds);
      assertThat(index.getActiveVectorIds().toArray()).as("the active id traversal at step %d", step)
          .containsExactly(expectedIds);

      for (final Map.Entry<Integer, long[]> entry : liveModel.entrySet()) {
        final int id = entry.getKey();
        final long[] expected = entry.getValue();
        final RID expectedRid = new RID((int) expected[0], expected[1]);

        assertThat(index.isLive(id)).as("isLive(%d) at step %d", id, step).isTrue();
        assertThat(index.getRid(id)).as("getRid(%d) at step %d", id, step).isEqualTo(expectedRid);
        assertThat(index.getBucketId(id)).isEqualTo((int) expected[0]);
        assertThat(index.getPosition(id)).isEqualTo(expected[1]);
        assertThat(index.isLocationOf(id, expectedRid)).isTrue();

        final long packed = index.getOffsetAndFlag(id);
        assertThat(VectorLocationIndex.offsetOf(packed)).as("offset of %d at step %d", id, step).isEqualTo(expected[2]);
        assertThat(VectorLocationIndex.isCompactedOf(packed)).isEqualTo(expected[3] == 1);

        final VectorLocationIndex.VectorLocation location = index.getLocation(id);
        assertThat(location).isNotNull();
        assertThat(location.absoluteFileOffset).isEqualTo(expected[2]);
        assertThat(location.isCompacted).isEqualTo(expected[3] == 1);
        assertThat(location.rid).isEqualTo(expectedRid);
        assertThat(location.deleted).as("a resident location is never a tombstone since issue #5516").isFalse();
      }

      for (final int id : deletedModel) {
        assertThat(index.isDeleted(id)).as("isDeleted(%d) at step %d", id, step).isTrue();
        if (!liveModel.containsKey(id)) {
          assertThat(index.getLocation(id)).as("getLocation(%d) at step %d", id, step).isNull();
          assertThat(index.getOffsetAndFlag(id)).isEqualTo(VectorLocationIndex.ABSENT);
          assertThat(index.getRid(id)).isNull();
        }
      }

      for (final RID rid : rids) {
        final int[] expected = liveModel.entrySet().stream()
            .filter(e -> e.getValue()[0] == rid.getBucketId() && e.getValue()[1] == rid.getPosition())
            .mapToInt(Map.Entry::getKey)
            .sorted()
            .toArray();
        assertThat(index.getVectorIdsForRid(rid)).as("reverse index for %s at step %d", rid, step)
            .containsExactly(expected);
      }
    }

    /**
     * Re-pointing a live vector id at a different record retires the id for the duration of the write, so a
     * lock-free search running through that window does not see a vector that is live. The branch does the safe
     * thing - the alternative is a torn RID, which is how a delete of record A tombstones the vector of record B -
     * but no engine path may take it, and {@code LSMVectorIndexTombstoneMemoryTest} pins that on the re-embedding
     * workload that would trip it first. This is the other half: proof the counter that pins it is not dead code.
     */
    @Test
    void rewritingALiveIdsRidInPlaceIsCountedAndStillCorrect() {
      final VectorLocationIndex index = new VectorLocationIndex();
      final RID first = new RID(1, 10);
      final RID second = new RID(2, 20);

      final int id = index.addVector(false, 100, first);
      assertThat(index.inPlaceRidRewriteCount()).as("an ordinary insert is not a rewrite").isZero();

      // A re-publication of the same id at a new offset is not a rewrite either - it is what a replicated page
      // replay does on every compaction - and counting it would make the pin above meaningless.
      index.addOrUpdate(id, true, 200, first, false);
      assertThat(index.inPlaceRidRewriteCount()).isZero();
      assertThat(index.getVectorIdsForRid(first)).containsExactly(id);

      index.addOrUpdate(id, false, 300, second, false);
      assertThat(index.inPlaceRidRewriteCount()).as("re-pointing the id at another record is").isEqualTo(1);

      // ...and having taken the branch, the index is still consistent: the old RID resolves to nothing, the new one
      // to the id, and the location reads back whole.
      assertThat(index.getVectorIdsForRid(first)).isEmpty();
      assertThat(index.getVectorIdsForRid(second)).containsExactly(id);
      assertThat(index.getRid(id)).isEqualTo(second);
      assertThat(VectorLocationIndex.offsetOf(index.getOffsetAndFlag(id))).isEqualTo(300);
      assertThat(index.size()).isEqualTo(1);
      assertThat(index.getActiveCount()).isEqualTo(1);
    }

    /**
     * The acceptance criterion of issue #5588, measured off the arrays the index has actually allocated rather than
     * inferred from object headers.
     */
    @Test
    void theRetainedHeapPerLiveVectorIsFarBelowTheMapItReplaces() {
      final VectorLocationIndex index = new VectorLocationIndex();
      final int vectors = 100_000;
      for (int i = 0; i < vectors; i++)
        index.addVector(i % 2 == 0, 1000L + i, new RID(1 + i % 8, i));

      assertThat(index.size()).isEqualTo(vectors);
      assertThat(index.getActiveCount()).isEqualTo(vectors);

      final long perVector = index.estimatedRetainedBytes() / vectors;
      assertThat(perVector).as("the layout this issue replaces retained ~90 bytes per live vector").isLessThan(40L);
      assertThat(perVector).as("and the chunk store alone is ~21, the reverse index another 8 to 16")
          .isBetween(20L, 40L);
    }

    /**
     * A capacity hint pre-sizes the structures; it is not, and can never again be, an eviction limit (issue #5559).
     */
    @Test
    void theCapacityHintNeitherEvictsNorAcceptsTheOldUnlimitedSentinel() {
      final VectorLocationIndex index = new VectorLocationIndex(8);
      final List<Integer> ids = new ArrayList<>();
      for (int i = 0; i < 512; i++)
        ids.add(index.addVector(false, 100 + i, new RID(4, i)));

      assertThat(index.size()).as("nothing may be dropped past the hint").isEqualTo(512);
      for (int i = 0; i < 512; i++) {
        assertThat(index.isLive(ids.get(i))).isTrue();
        assertThat(index.getVectorIdsForRid(new RID(4, i))).containsExactly(ids.get(i));
      }

      assertThat(Arrays.stream(index.getAllVectorIds().toArray()).boxed().toList()).isEqualTo(ids);

      try {
        new VectorLocationIndex(-1);
        throw new AssertionError("the old -1 = unlimited sentinel must be refused, not read as a capacity");
      } catch (final IllegalArgumentException e) {
        assertThat(e.getMessage()).contains("eviction limit");
      }
    }

    /**
     * The offset shares its word with the compacted flag, so the range it may take is checked at the door rather
     * than silently wrapping into the flag and reading back as a compacted entry elsewhere in the file.
     * <p>
     * {@code -1} is inside that range and has to be: the document-scan fallback of a rebuild registers a vector it
     * found on a record but not on any page with exactly that offset, meaning "no page entry, read the document".
     */
    @Test
    void theOffsetRangeIsCheckedAndKeepsTheNoPageEntrySentinel() {
      final VectorLocationIndex index = new VectorLocationIndex();

      final int id = index.addVector(false, VectorLocationIndex.NO_FILE_OFFSET, new RID(1, 1));
      final long packed = index.getOffsetAndFlag(id);
      assertThat(packed).isNotEqualTo(VectorLocationIndex.ABSENT);
      assertThat(VectorLocationIndex.offsetOf(packed))
          .as("a vector with no page entry stays readable, it just has to come from its document")
          .isEqualTo(VectorLocationIndex.NO_FILE_OFFSET);
      assertThat(VectorLocationIndex.isCompactedOf(packed)).isFalse();
      assertThat(index.getLocation(id).absoluteFileOffset).isEqualTo(VectorLocationIndex.NO_FILE_OFFSET);

      // The same sentinel in the compacted file, so the flag survives a negative offset.
      final int compactedId = index.addVector(true, VectorLocationIndex.NO_FILE_OFFSET, new RID(1, 2));
      assertThat(VectorLocationIndex.offsetOf(index.getOffsetAndFlag(compactedId)))
          .isEqualTo(VectorLocationIndex.NO_FILE_OFFSET);
      assertThat(VectorLocationIndex.isCompactedOf(index.getOffsetAndFlag(compactedId))).isTrue();

      for (final long invalid : new long[] { -2L, Long.MIN_VALUE, Long.MAX_VALUE })
        try {
          index.addVector(false, invalid, new RID(1, 3));
          throw new AssertionError("an out-of-range absolute file offset must be refused: " + invalid);
        } catch (final IllegalArgumentException e) {
          assertThat(e.getMessage()).contains("compacted flag");
        }

      assertThat(index.size()).as("and a refused offset leaves nothing behind").isEqualTo(2);
    }

    /** A latch-free sanity check that the concurrency helpers above do not silently skip their bodies. */
    @Test
    void concurrentAddsAndReadsAgreeOnTheFinalState() throws Exception {
      final VectorLocationIndex index = new VectorLocationIndex();
      final int threads = 8;
      final int perThread = 4000;
      final CountDownLatch ready = new CountDownLatch(threads);
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final List<Thread> workers = new ArrayList<>();

      for (int t = 0; t < threads; t++) {
        final int bucket = t;
        workers.add(new Thread(() -> {
          ready.countDown();
          try {
            ready.await();
            for (int i = 0; i < perThread; i++) {
              index.addVector(false, 1000L + i, new RID(bucket, i));
              index.getActiveVectorIds().count();
              index.getAllVectorIds().count();
              index.getActiveCount();
            }
          } catch (final Throwable e) {
            failure.compareAndSet(null, e);
          }
        }, "issue5588-worker-" + t));
      }

      workers.forEach(Thread::start);
      for (final Thread worker : workers) {
        worker.join(120_000);
        assertThat(worker.isAlive()).as("%s must finish", worker.getName()).isFalse();
      }

      assertThat(failure.get()).isNull();
      assertThat(index.size()).isEqualTo(threads * perThread);
      assertThat(index.getActiveCount()).isEqualTo(threads * perThread);
      assertThat(index.getAllVectorIds().count()).isEqualTo(threads * perThread);
      for (int t = 0; t < threads; t++)
        for (int i = 0; i < perThread; i++)
          assertThat(index.getVectorIdsForRid(new RID(t, i))).as("bucket %d position %d", t, i).hasSize(1);
    }
  }

  /**
   * Regression for #6762: a failed first-use location load must not leave the vector-id allocator at 0.
   * <p>
   * Regression test for issue #6762: a failed first-use materialisation must not leave the vector-id allocator at 0.
   * <p>
   * {@code materializeLocations()} catches a load failure, logs a WARNING and still marks the materialisation done -
   * deliberately, because the alternative is re-parsing a corrupt index once per query instead of once per open. But
   * {@code nextId} was only advanced at the END of a successful load, so a failure that threw after some entries had
   * already been inserted left the map populated and the allocator at 0. {@code allocateVectorId()} exists precisely
   * to stop ids restarting at 0 on top of live entries, and it would then hand out 0, 1, 2... - so the next insert
   * superseded a live vector instead of adding one, losing one vector per insert.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class VectorIdAllocatorAfterFailedLoad {
    private static final String DB_PATH    = "target/databases/VectorIdAllocatorAfterFailedLoad";
    private static final int    DIMENSIONS = 8;
    private static final int    VECTORS    = 20;

    @AfterEach
    void cleanUp() {
      final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
      if (factory.exists())
        factory.open().drop();
      FileUtils.deleteRecursively(new File(DB_PATH));
    }

    @Test
    void anInsertAfterAFailedMaterialisationDoesNotSupersedeALiveVector() throws Exception {
      createDatabaseWithVectorIndex();

      try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
        final Database db = factory.open();
        try {
          final LSMVectorIndex index = vectorIndexOf(db);

          // Populate the location map for real, then put the index back into the state a PARTIAL load leaves behind:
          // entries already in the map, the one-attempt flag not yet latched, and the allocator still at its initial
          // 0. Breaking the mutable component makes the materialisation that follows fail the way the real one does.
          assertThat(index.countEntries()).isEqualTo(VECTORS);
          final Object mutable = readField(index, "mutable");
          setField(index, "locationsMaterialized", false);
          ((AtomicInteger) readField(index, "nextId")).set(0);
          setField(index, "mutable", null);
          try {
            index.getVectorIndex(); // materialises: throws internally, and the failure is swallowed by design
          } finally {
            setField(index, "mutable", mutable);
          }

          assertThat(index.areLocationsMaterializedForTest())
              .as("the one-attempt contract still holds: a failed load is not retried")
              .isTrue();
          assertThat(((AtomicInteger) readField(index, "nextId")).get())
              .as("but the allocator must sit past every id the map already holds, not back at 0")
              .isGreaterThanOrEqualTo(VECTORS);

          // The observable consequence: with the allocator at 0 this insert would overwrite the entry holding id 0. A load
          // that failed cannot tell which ids it missed either, so the insert is now refused outright until the index is
          // rebuilt, rather than trusting an allocator that only knows what was read.
          assertThatThrownBy(
              () -> db.transaction(() -> db.newDocument("Doc").set("id", VECTORS).set("embedding", randomVector(7)).save()))
              .as("refused when the record is saved, before the commit applies anything (issue #9892)")
              .isInstanceOf(IndexException.class)
              .hasMessageContaining("refuses new vectors");

          assertThat(index.countEntries())
              .as("no live vector was superseded")
              .isEqualTo(VECTORS);
        } finally {
          db.close();
        }
      }
    }

    private LSMVectorIndex vectorIndexOf(final Database db) {
      final TypeIndex typeIndex = (TypeIndex) db.getSchema().getIndexByName("Doc[embedding]");
      return (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];
    }

    private static float[] randomVector(final long seed) {
      final Random rnd = new Random(seed);
      final float[] v = new float[DIMENSIONS];
      for (int j = 0; j < DIMENSIONS; j++)
        v[j] = (float) rnd.nextGaussian();
      return v;
    }

    private void createDatabaseWithVectorIndex() {
      FileUtils.deleteRecursively(new File(DB_PATH));

      try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
        final Database db = factory.create();
        try {
          db.transaction(() -> {
            final DocumentType type = db.getSchema().createDocumentType("Doc");
            type.createProperty("id", Type.INTEGER);
            type.createProperty("embedding", Type.ARRAY_OF_FLOATS);
            for (int i = 0; i < VECTORS; i++)
              db.newDocument("Doc").set("id", i).set("embedding", randomVector(i)).save();
          });
          db.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
              + ", \"similarity\": \"EUCLIDEAN\" }");
        } finally {
          db.close();
        }
      }
    }

    private static Object readField(final Object target, final String name) throws Exception {
      final Field f = LSMVectorIndex.class.getDeclaredField(name);
      f.setAccessible(true);
      return f.get(target);
    }

    private static void setField(final Object target, final String name, final Object value) throws Exception {
      final Field f = LSMVectorIndex.class.getDeclaredField(name);
      f.setAccessible(true);
      f.set(target, value);
    }
  }

  /**
   * Regression for #8852: the first search after a reopen does not re-read every vector document.
   * <p>
   * Regression test for issue #8852: the first search after a reopen must not re-read the document of every vector to
   * rebuild the ordinal map when nothing was deleted and the ordinal map recorded next to the graph still describes the
   * live set. The map is validated against the in-memory location index instead.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @Tag("vector")
  class ReopenFirstSearchOrdinalMap {
    private static final String DB_PATH     = "target/test-databases/ReopenFirstSearchOrdinalMap";
    private static final int    DIMENSIONS  = 16;
    private static final int    NUM_VECTORS = 2_000;

    @BeforeEach
    void setUp() {
      FileUtils.deleteRecursively(new File(DB_PATH));
    }

    @AfterEach
    void tearDown() {
      FileUtils.deleteRecursively(new File(DB_PATH));
    }

    @Test
    void reopenLoadsTheGraphThroughThePersistedOrdinalMap() {
      build();

      try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
        final Database db = factory.open();
        try {
          final LSMVectorIndex index = vectorIndex(db);
          final RID rid = db.query("sql", "SELECT FROM Doc WHERE id = 7").next().getIdentity().get();

          final List<Pair<RID, Float>> results = index.findNeighborsFromVector(embedding(7), 5, 64);

          assertThat(results.stream().map(Pair::getFirst)).contains(rid);
          assertThat(index.getStats().get("graphLoadsFromOrdinalMap"))
              .as("the ordinal map must replace the per-vector document walk").isEqualTo(1L);
          assertThat(index.getStats().get("graphRebuildCount")).isZero();
          assertThat(index.getStats().get("graphNodeCount")).isEqualTo((long) NUM_VECTORS);
        } finally {
          db.drop();
        }
      }
    }

    @Test
    void warmUpLoadsTheGraphBeforeTheFirstSearch() {
      build();

      try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
        final Database db = factory.open();
        try {
          final LSMVectorIndex index = vectorIndex(db);
          assertThat(index.getStats().get("graphNodeCount")).isZero();

          index.warmUp();

          assertThat(index.getStats().get("graphNodeCount")).isEqualTo((long) NUM_VECTORS);
          assertThat(index.findNeighborsFromVector(embedding(3), 3, 64)).hasSize(3);
        } finally {
          db.drop();
        }
      }
    }

    @Test
    void deletesFallBackToThePreviousPaths() {
      build();

      try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
        final Database db = factory.open();
        try {
          db.transaction(() -> db.command("sql", "DELETE FROM Doc WHERE id = 5"));
          // The location index of a fresh session holds no tombstone, so a vector id that is gone is simply absent
          // from the map's point of view: whatever path loads the graph must still answer correctly.
          final LSMVectorIndex index = vectorIndex(db);
          assertThat(index.findNeighborsFromVector(embedding(9), 3, 64)).hasSize(3);
        } finally {
          db.drop();
        }
      }
    }

    private static void build() {
      try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
        final Database db = factory.create();
        try {
          db.transaction(() -> {
            final var type = db.getSchema().createDocumentType("Doc");
            type.createProperty("id", Type.INTEGER);
            type.createProperty("vector", Type.ARRAY_OF_FLOATS);
            db.command("sql", "CREATE INDEX ON Doc (vector) LSM_VECTOR METADATA { \"dimensions\": " + DIMENSIONS
                + ", \"similarity\": \"COSINE\" }");
          });
          db.begin();
          for (int i = 0; i < NUM_VECTORS; i++) {
            db.newDocument("Doc").set("id", i).set("vector", embedding(i)).save();
            if (i % 1000 == 999) {
              db.commit();
              db.begin();
            }
          }
          db.commit();
          vectorIndex(db).buildVectorGraphNow();
        } finally {
          if (db.isOpen())
            db.close();
        }
      }
    }

    private static float[] embedding(final int id) {
      final Random random = new Random(0x8852L * 31 + id);
      final float[] vector = new float[DIMENSIONS];
      for (int d = 0; d < DIMENSIONS; d++)
        vector[d] = random.nextFloat();
      return vector;
    }

    private static LSMVectorIndex vectorIndex(final Database db) {
      return (LSMVectorIndex) db.getSchema().getType("Doc").getPolymorphicIndexByProperties("vector")
          .getIndexesOnBuckets()[0];
    }
  }

  /**
   * Regression for #9428: a replicated page advances the vector-id allocator.
   * <p>
   * Issue #9428: a page written by another node reaches this one through {@link LSMVectorIndex#applyReplicatedPageUpdate},
   * which added its entries to the location index but left the id allocator where it was. The next local insert then
   * minted an id a replicated entry already held and superseded it: on a 3-node cluster the vectors a follower inserted
   * after the leader's first batch replaced the leader's in the index of every node.
   * <p>
   * The peer's write is simulated with an entry appended at an explicit id - the id the allocator here hands out next -
   * which the location index learns of but the allocator does not, exactly the state a replicated page leaves behind.
   */
  @Nested
  class ReplicatedPageAdvancesVectorId extends TestHelper {
    private static final String TYPE_NAME = "Probe";

    @Test
    void aReplicatedPageAdvancesTheVectorIdAllocator() throws Exception {
      final VertexType type = database.getSchema().buildVertexType().withName(TYPE_NAME).withTotalBuckets(1).create();
      type.createProperty("vector", float[].class);
      // NOT INDEXED: ONLY HOLDS THE RECORD THE SIMULATED PEER ENTRY POINTS AT
      database.getSchema().buildVertexType().withName("Other").withTotalBuckets(1).create();
      final TypeLSMVectorIndexBuilder builder = database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "vector" })
          .withLSMVectorType();
      builder.withDimensions(2);
      final TypeIndex typeIndex = builder.create();
      final LSMVectorIndex index = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];

      database.transaction(() -> database.newVertex(TYPE_NAME).set("vector", new float[] { 0.1f, 0.2f }).save());
      assertThat(index.countEntries()).isEqualTo(1);

      // THE PEER'S ENTRY: THE NEXT ID THIS NODE WOULD MINT, FOR A RECORD THIS NODE DID NOT INDEX ITSELF
      final int peerId = index.residentLocationsForTest().getNextId();
      database.transaction(() -> {
        final RID peerRecord = database.newVertex("Other").save().getIdentity();
        index.persistEntryForTest(peerId, peerRecord, new float[] { 0.3f, 0.4f });
      });
      assertThat(index.countEntries()).isEqualTo(2);

      // THE PAGE ARRIVES THROUGH REPLICATION
      final DatabaseInternal db = (DatabaseInternal) database;
      final int lastPage = index.getTotalPages() - 1;
      final ImmutablePage page = db.getPageManager()
          .getImmutablePage(new PageId(db, index.getFileId(), lastPage), index.getPageSize(), false, false);
      index.applyReplicatedPageUpdate(page.modify());

      database.transaction(() -> database.newVertex(TYPE_NAME).set("vector", new float[] { 0.5f, 0.6f }).save());
      assertThat(index.countEntries()).as("the next local insert must not reuse the replicated entry's id").isEqualTo(3);
    }
  }

  /**
   * Regression for #9857: a replicated page changes the search state as a local write does.
   * <p>
   * Issue #9857, at the level of one index: a page that reaches {@link LSMVectorIndex#applyReplicatedPageUpdate} from a PEER
   * changes the search state as a local write does - a tombstone over a live id counts as a mutation and turns the graph
   * MUTABLE, once, however many times the page is applied - while the same page replayed from this database's own WAL at
   * open only moves the locations, because the first search reconciles the persisted graph against the pages and charges
   * the tombstones it finds there itself (issue #7842).
   * <p>
   * The peer's delete is simulated with a tombstone appended to the pages only, which the location index has not seen:
   * exactly the state a replicated page arrives in.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ReplicatedPageSearchState extends TestHelper {
    private static final String TYPE_NAME = "Probe";
    private static final int    RECORDS   = 20;

    private LSMVectorIndex index;

    @BeforeEach
    void createIndexAndBuildGraph() {
      final VertexType type = database.getSchema().buildVertexType().withName(TYPE_NAME).withTotalBuckets(1).create();
      type.createProperty("id", Integer.class);
      type.createProperty("vector", float[].class);
      final TypeLSMVectorIndexBuilder builder = database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "vector" })
          .withLSMVectorType();
      builder.withDimensions(2);
      final TypeIndex typeIndex = builder.create();
      index = (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[0];

      database.transaction(() -> {
        for (int i = 0; i < RECORDS; i++)
          database.newVertex(TYPE_NAME).set("id", i, "vector", new float[] { i + 1f, 1f }).save();
      });

      // The first search builds the graph: from here on only a mutation makes the next search rebuild it
      assertThat(index.findNeighborsFromVector(new float[] { 1f, 1f }, 5)).hasSize(5);
      assertThat(index.getStats().get("mutationsSinceRebuild")).as("precondition: the build paid every write off").isZero();
      assertThat(index.getStats().get("graphState")).as("precondition: the graph is IMMUTABLE (1)").isEqualTo(1L);
    }

    @Test
    void aPeerTombstoneIsChargedOnceAndTurnsTheGraphMutable() {
      final int vectorId = tombstoneOnThePagesOnly(3);

      index.applyReplicatedPageUpdate(lastPage(), true);

      assertThat(index.residentLocationsForTest().isDeleted(vectorId)).isTrue();
      assertThat(index.getStats().get("mutationsSinceRebuild"))
          .as("the tombstone is one mutation, and the live entries the page also carries are none").isEqualTo(1L);
      assertThat(index.getStats().get("graphState")).as("the graph owes a rebuild: MUTABLE (2)").isEqualTo(2L);

      // The same page applied again (a torn-write repair, a node's own commit echoed back) changes nothing
      index.applyReplicatedPageUpdate(lastPage(), true);
      assertThat(index.getStats().get("mutationsSinceRebuild")).isEqualTo(1L);
    }

    @Test
    void theSamePageReplayedByRecoveryMovesTheLocationsOnly() {
      final int vectorId = tombstoneOnThePagesOnly(3);

      index.applyReplicatedPageUpdate(lastPage(), false);

      assertThat(index.residentLocationsForTest().isDeleted(vectorId)).as("the locations still follow the page").isTrue();
      assertThat(index.getStats().get("mutationsSinceRebuild")).as("recovery charges nothing").isZero();
      assertThat(index.getStats().get("graphState")).as("and leaves the graph as it was: IMMUTABLE (1)").isEqualTo(1L);
    }

    /** Appends a tombstone for the record with this {@code id} to the pages, leaving the location index untouched. */
    private int tombstoneOnThePagesOnly(final int id) {
      final RID rid;
      try (final ResultSet rs = database.query("sql", "SELECT @rid AS rid FROM " + TYPE_NAME + " WHERE id = ?", id)) {
        rid = rs.next().getProperty("rid");
      }
      final int[] vectorIds = index.residentLocationsForTest().getVectorIdsForRid(rid);
      assertThat(vectorIds).hasSize(1);
      database.transaction(() -> index.persistTombstoneForTest(vectorIds[0], rid));
      assertThat(index.residentLocationsForTest().isLive(vectorIds[0])).as("precondition: only the pages know of the delete")
          .isTrue();
      return vectorIds[0];
    }

    private MutablePage lastPage() {
      final DatabaseInternal db = (DatabaseInternal) database;
      try {
        final ImmutablePage page = db.getPageManager()
            .getImmutablePage(new PageId(db, index.getFileId(), index.getTotalPages() - 1), index.getPageSize(), false, false);
        return page.modify();
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
