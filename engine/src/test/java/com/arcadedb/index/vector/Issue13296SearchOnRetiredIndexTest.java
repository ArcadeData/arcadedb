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

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.AbstractSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #13296: a Raft follower applies a replicated {@code COMPACT INDEX} by deleting the superseded component files and only then
 * reloading the schema, so a search that still resolves the previous generation of a sub-index reads a file that is gone. A sub-index
 * that was cold (its locations never materialised since the previous schema load of the same statement) parsed no entries from it
 * and the search answered normally without its rows: an exhaustive search lacked whole buckets for the 1 to 2 seconds the apply takes.
 * It now fails with a retryable exception instead.
 * <p>
 * The refusal is for that case only. A sub-index whose locations are resident and complete answers from memory, and from the records
 * where it needs a vector, so its retired file is not read at all: it answers every row, and refusing it would turn a correct answer
 * into a failure for the first of the sixteen entries of a compaction. A search that fails while its file is retired, or with a
 * conflict of the engine's own (a page of a file dropped under a read), is reported as that conflict and not as an index failure.
 * <p>
 * The tests do by hand what the apply does to the files of one sub-index of a 4-bucket index.
 */
class Issue13296SearchOnRetiredIndexTest extends TestHelper {
  private static final int    DIMENSIONS = 8;
  private static final int    BUCKETS    = 4;
  private static final int    RECORDS    = 400;
  private static final int    VICTIM     = 1;
  private static final String EXHAUSTIVE = "SELECT @rid AS rid FROM (SELECT expand(vector.neighbors('Doc[v]', :q, :k)))";

  /** The three layouts of the stored vectors, each by the page and graph reads it makes. */
  static Stream<Arguments> layouts() {
    return Stream.of(Arguments.of("INT8", true), Arguments.of("INT8", false), Arguments.of("NONE", false));
  }

  /** Every layout with a top-10 search and with an exhaustive one (k far above the live count). */
  static Stream<Arguments> layoutsAndRequestedRows() {
    return layouts().flatMap(layout -> Stream.of(10, 100_000).map(k -> Arguments.of(layout.get()[0], layout.get()[1], k)));
  }

  @Test
  void anExhaustiveSearchOverARetiredSubIndexFailsInsteadOfAnsweringShort() {
    final float[] query = createIndexAndRecords("INT8", true);
    assertThat(exhaustiveRows(query, 100_000)).as("the warm index answers every record").isEqualTo(RECORDS);

    // Reopened: no location index is materialised and no graph is resident, as for the index objects a schema load has just made
    reopenDatabase();
    retire(subIndex());

    assertThatThrownBy(() -> exhaustiveRows(query, 100_000)).as("a search over a retired sub-index is refused, not answered short")
        .isInstanceOf(NeedRetryException.class).hasMessageContaining("retired by a schema change");
  }

  @Test
  void aGroupedSearchOverARetiredColdSubIndexFailsInsteadOfAnsweringShort() {
    final float[] query = createIndexAndRecords("INT8", true);
    reopenDatabase();
    final LSMVectorIndex subIndex = subIndex();
    retire(subIndex);

    assertThatThrownBy(() -> subIndex.findNeighborsFromVectorGrouped(query, 10, 2, -1, null, rid -> rid.getPosition() % 5))
        .as("the grouped search refuses a retired cold sub-index too").isInstanceOf(NeedRetryException.class)
        .hasMessageContaining("retired by a schema change");
  }

  @ParameterizedTest
  @MethodSource("layoutsAndRequestedRows")
  void aWarmSubIndexWhoseFileWasRetiredStillAnswersEveryRecord(final String quantization, final boolean storeVectorsInGraph,
      final int k) {
    final float[] query = createIndexAndRecords(quantization, storeVectorsInGraph);
    assertThat(exhaustiveRows(query, 100_000)).as("the index is warm: it has answered once").isEqualTo(RECORDS);

    retire(subIndex());

    assertThat(exhaustiveRows(query, k)).as("its locations and graph are resident: the retired file is not what it answers from")
        .isEqualTo(Math.min(k, RECORDS));
  }

  @ParameterizedTest
  @MethodSource("layoutsAndRequestedRows")
  void aWarmSearchThatFinishedIsNotRefusedBecauseItsFileWasRetiredWhileItRan(final String quantization,
      final boolean storeVectorsInGraph, final int k) {
    final float[] query = createIndexAndRecords(quantization, storeVectorsInGraph);
    assertThat(exhaustiveRows(query, 100_000)).as("the index is warm: it has answered once").isEqualTo(RECORDS);
    final LSMVectorIndex subIndex = subIndex();
    final HookedAllowList allowList = new HookedAllowList(membersOfBucketOf(subIndex), () -> retire(subIndex));

    // The file goes at the first membership test of the walk, which is under the search's locks: after every check made up front
    final int expected = Math.min(k, RECORDS / BUCKETS);
    assertThat(subIndex.findNeighborsFromVector(query, k, -1, allowList)).as("a search that already ran is answered, not refused")
        .hasSize(expected);
    assertThat(allowList.fired()).as("the file was retired while the search ran").isTrue();
  }

  @Test
  void aSearchThatFailsBecauseItsFileWasRetiredIsRetryable() {
    final float[] query = createIndexAndRecords("INT8", true);
    assertThat(exhaustiveRows(query, 100_000)).as("the index is warm: it has answered once").isEqualTo(RECORDS);
    final LSMVectorIndex subIndex = subIndex();
    final int fileId = subIndex.getFileId();
    // What a read of a file that is gone reports, in the layers below: not an exception of the engine's own
    final HookedAllowList allowList = new HookedAllowList(membersOfBucketOf(subIndex), () -> {
      retire(subIndex);
      throw new IllegalArgumentException("File with id '" + fileId + "' was not found");
    });

    assertThatThrownBy(() -> subIndex.findNeighborsFromVector(query, 10, -1, allowList))
        .as("a failure of a search whose file was retired is the conflict with the schema change, not a failure of the index")
        .isInstanceOf(ConcurrentModificationException.class).hasMessageContaining("retired by a schema change")
        .hasRootCauseInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void aRetryableConflictOfTheEngineDuringASearchKeepsItsClass() {
    final float[] query = createIndexAndRecords("INT8", true);
    assertThat(exhaustiveRows(query, 100_000)).as("the index is warm: it has answered once").isEqualTo(RECORDS);
    final LSMVectorIndex subIndex = subIndex();
    final int fileId = subIndex.getFileId();
    // What PageManager.loadPage reports for a page of a file dropped under the read (issue #12139); the file is still registered
    final HookedAllowList allowList = new HookedAllowList(membersOfBucketOf(subIndex), () -> {
      throw new ConcurrentModificationException("File with id '" + fileId + "' has been removed");
    });

    assertThatThrownBy(() -> subIndex.findNeighborsFromVector(query, 10, -1, allowList))
        .as("a conflict the caller retries is not wrapped into an IndexException, which no retry loop retries").isInstanceOf(NeedRetryException.class)
        .isNotInstanceOf(IndexException.class).hasMessageContaining("has been removed");
  }

  @Test
  void aFailureOfTheSearchOfAHealthyIndexIsStillAnIndexFailure() {
    final float[] query = createIndexAndRecords("INT8", true);
    assertThat(exhaustiveRows(query, 100_000)).as("the index is warm: it has answered once").isEqualTo(RECORDS);
    final LSMVectorIndex subIndex = subIndex();
    final HookedAllowList allowList = new HookedAllowList(membersOfBucketOf(subIndex), () -> {
      throw new IllegalStateException("the walk failed");
    });

    assertThatThrownBy(() -> subIndex.findNeighborsFromVector(query, 10, -1, allowList)).as("only a conflict is passed through")
        .isInstanceOf(IndexException.class).hasMessageContaining("Error performing vector search")
        .hasRootCauseInstanceOf(IllegalStateException.class);
  }

  @Test
  void aFileIdReadBeforeALeaderCompactionSwappedItIsNotARetiredIndex() {
    final float[] query = createIndexAndRecords("INT8", true);
    final LSMVectorIndex subIndex = subIndex();
    final int before = subIndex.getFileId();
    assertThat(subIndex.isFileRetired(before)).as("a healthy index owns its file").isFalse();

    // The leader's compaction swaps the data file of the index and then drops the old one. A search that read the id just before
    // the swap and tests it after the drop finds the old file gone, and must see that it no longer is this index's
    database.command("sql", "COMPACT INDEX `Doc[v]`").close();
    final int after = subIndex.getFileId();
    assertThat(after).as("the compaction swapped the data file").isNotEqualTo(before);
    assertThat(((DatabaseInternal) database).getFileManager().existsFile(before)).as("and dropped the old one").isFalse();

    assertThat(subIndex.isFileRetired(before)).as("the old id is not this index's any more: nothing is retired").isFalse();
    assertThat(subIndex.isFileRetired(after)).as("the new file is there").isFalse();
    assertThat(exhaustiveRows(query, 100_000)).as("and the healthy index answers every record").isEqualTo(RECORDS);

    retire(subIndex);
    assertThat(subIndex.isFileRetired(after)).as("a file retired under the index that still owns it is").isTrue();
  }

  /** The files of one sub-index are gone by design: the integrity check at the end would only report them. */
  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  /** Creates the 4-bucket type and its vector index, then {@link #RECORDS} records; returns a query vector. */
  private float[] createIndexAndRecords(final String quantization, final boolean storeVectorsInGraph) {
    final Random random = new Random(13296);
    database.transaction(() -> {
      database.getSchema().buildVertexType().withName("Doc").withTotalBuckets(BUCKETS).create().createProperty("v", Type.ARRAY_OF_FLOATS);
      database.command("sql", "CREATE INDEX ON Doc (v) LSM_VECTOR METADATA {\"dimensions\": " + DIMENSIONS
          + ", \"similarity\": \"COSINE\", \"quantization\": \"" + quantization + "\", \"storeVectorsInGraph\": " + storeVectorsInGraph
          + "}");
    });
    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newVertex("Doc").set("v", gaussian(random)).save();
    });
    return gaussian(random);
  }

  private LSMVectorIndex subIndex() {
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName("Doc[v]");
    return (LSMVectorIndex) typeIndex.getIndexesOnBuckets()[VICTIM];
  }

  /** What a follower's apply thread does to the files an entry supersedes. */
  private void retire(final LSMVectorIndex subIndex) {
    final int fileId = subIndex.getFileId();
    final DatabaseInternal internal = (DatabaseInternal) database;
    try {
      internal.getPageManager().deleteFile(internal, fileId);
      internal.getFileManager().dropFile(fileId);
      internal.getSchema().getEmbedded().removeFile(fileId);
    } catch (final IOException e) {
      throw new IllegalStateException("could not retire file " + fileId, e);
    }
  }

  private Set<RID> membersOfBucketOf(final LSMVectorIndex subIndex) {
    final Set<RID> members = new HashSet<>();
    final Iterator<Record> records = database.iterateType("Doc", false);
    while (records.hasNext()) {
      final RID rid = records.next().getIdentity();
      if (rid.getBucketId() == subIndex.getAssociatedBucketId())
        members.add(rid);
    }
    assertThat(members).as("the records of the bucket of the sub-index").hasSize(RECORDS / BUCKETS);
    return members;
  }

  private int exhaustiveRows(final float[] query, final int k) {
    final Set<String> rids = new HashSet<>();
    try (final ResultSet resultSet = database.query("sql", EXHAUSTIVE, Map.of("q", query, "k", k))) {
      while (resultSet.hasNext())
        rids.add(String.valueOf((Object) resultSet.next().getProperty("rid")));
    }
    return rids.size();
  }

  private static float[] gaussian(final Random random) {
    final float[] vector = new float[DIMENSIONS];
    for (int i = 0; i < DIMENSIONS; i++)
      vector[i] = (float) random.nextGaussian();
    return vector;
  }

  /**
   * An allow-list that covers its whole bucket, so it filters nothing, and runs {@code action} at the first membership test: the
   * walk makes it under the search's locks, after every check made up front.
   */
  private static final class HookedAllowList extends AbstractSet<RID> {
    private final Set<RID>  members;
    private final Runnable  action;
    private       boolean   fired;

    private HookedAllowList(final Set<RID> members, final Runnable action) {
      this.members = members;
      this.action = action;
    }

    @Override
    public boolean contains(final Object rid) {
      if (!fired)
        fire();
      return members.contains(rid);
    }

    @Override
    public Iterator<RID> iterator() {
      return members.iterator();
    }

    @Override
    public int size() {
      return members.size();
    }

    boolean fired() {
      return fired;
    }

    private void fire() {
      fired = true;
      action.run();
    }
  }
}
