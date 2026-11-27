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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.TransactionException;
import com.arcadedb.index.vector.LSMVectorIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #13130: a {@code save()} that an index refuses (an {@code LSM_VECTOR} index answers a wrong dimension, a NaN or an Infinity with an
 * {@code IllegalArgumentException}; a unique index answers a duplicate inline with a {@code DuplicatedKeyException}) left the refused value
 * on the instance the transaction had queued for its commit, when that record had already been saved earlier in the same transaction. The
 * exception undid the queued index changes only, and since #11751 the commit indexes what was assigned to a record after its save, so the
 * commit met the refused value again, threw, and rolled back the whole transaction: every valid write of the caller that had handled the
 * refusal and gone on was lost with it.
 * <p>
 * A record not saved earlier in the transaction was clean already (it is taken out of the queue by the refusal), and so is a SQL statement
 * (its savepoint takes it back). What this pins is the direct API on a record the transaction holds queued: the refused save leaves its
 * indexed properties as the last save the indexes accepted left them, so the commit has nothing to re-index and the record stored is the one
 * the indexes describe.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue13130RefusedSaveOfQueuedRecordTest extends TestHelper {
  private static final String TYPE = "C";

  private enum Reach {
    /** What any Java caller does: the transaction answers its own instance. */
    LOOKUP,
    /** What the gRPC upsert does: the row of a query, taken to its document. */
    SELECT
  }

  private enum Refusal {
    NAN(new float[] { Float.NaN, 0, 0, 1 }),
    INFINITY(new float[] { Float.POSITIVE_INFINITY, 0, 0, 1 }),
    WRONG_DIMENSION(new float[] { 1, 2, 3 });

    final float[] vector;

    Refusal(final float[] vector) {
      this.vector = vector;
    }
  }

  static Stream<Arguments> everyShape() {
    final Stream.Builder<Arguments> shapes = Stream.builder();
    for (final Reach reach : Reach.values())
      for (final Refusal refusal : Refusal.values())
        shapes.add(Arguments.of(reach, refusal));
    return shapes.build();
  }

  private void createVectorType(final String type, final boolean scalarIndexed) {
    database.getSchema().buildVertexType().withName(type).withTotalBuckets(1).create().createProperty("embedding", Type.ARRAY_OF_FLOATS);
    database.getSchema().getType(type).createProperty("ext", Type.INTEGER);
    if (scalarIndexed)
      database.command("sql", "CREATE INDEX ON " + type + " (ext) NOTUNIQUE").close();
    database.command("sql", "CREATE INDEX ON " + type + " (embedding) LSM_VECTOR METADATA {dimensions: 4, similarity: 'EUCLIDEAN'}").close();
  }

  private RID insertSeed(final String type) {
    final RID[] rid = new RID[1];
    database.transaction(
        () -> rid[0] = database.newVertex(type).set("ext", 0).set("embedding", new float[] { 0, 1, 2, 3 }).save().getIdentity());
    return rid[0];
  }

  private MutableDocument reach(final Reach how, final String type, final RID rid) {
    if (how == Reach.LOOKUP)
      return database.lookupByRID(rid, true).asDocument().modify();
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + type + " WHERE ext < 100")) {
      return rs.next().getElement().get().asDocument().modify();
    }
  }

  private void insert(final String type, final int ext, final float... embedding) {
    database.newVertex(type).set("ext", ext).set("embedding", embedding).save();
  }

  private long countWithExt(final String type, final int from, final int to) {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM " + type + " WHERE ext >= " + from + " AND ext <= " + to)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private float[] committedEmbedding(final RID rid) {
    return (float[]) database.lookupByRID(rid, true).asDocument().get("embedding");
  }

  /** The index itself, never a scan: the RIDs the vector index returns for the nearest vector of {@code query}. */
  private List<RID> nearest(final String type, final float[] query, final int k) {
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName(type + "[embedding]");
    final List<RID> rids = new ArrayList<>();
    for (final IndexInternal bucketIndex : typeIndex.getIndexesOnBuckets())
      for (final Pair<RID, Float> neighbour : ((LSMVectorIndex) bucketIndex).findNeighborsFromVector(query, k))
        rids.add(neighbour.getFirst());
    return rids;
  }

  private long activeVectors(final String type) {
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName(type + "[embedding]");
    long total = 0;
    for (final IndexInternal bucketIndex : typeIndex.getIndexesOnBuckets())
      total += ((LSMVectorIndex) bucketIndex).getStats().get("activeVectors");
    return total;
  }

  @ParameterizedTest(name = "{0} {1}")
  @MethodSource("everyShape")
  void aRefusedVectorLeavesTheRecordAsItsLastAcceptedSaveAndTheTransactionCommits(final Reach how, final Refusal refusal) {
    createVectorType(TYPE, false);
    final RID rid = insertSeed(TYPE);

    database.begin();
    insert(TYPE, 100, 1, 1, 1, 1);
    reach(how, TYPE, rid).set("embedding", new float[] { 9, 9, 9, 9 }).save();

    assertThatThrownBy(() -> reach(how, TYPE, rid).set("embedding", refusal.vector).save()).isInstanceOf(IllegalArgumentException.class);

    insert(TYPE, 101, 2, 2, 2, 2);
    database.commit();

    assertThat(countWithExt(TYPE, 100, 101)).as("the valid inserts of the transaction are stored").isEqualTo(2);
    assertThat(committedEmbedding(rid)).as("the committed embedding is the last one the index accepted").containsExactly(9, 9, 9, 9);
    assertThat(nearest(TYPE, new float[] { 9, 9, 9, 9 }, 1)).containsExactly(rid);
    assertThat(nearest(TYPE, new float[] { 0, 1, 2, 3 }, 1)).as("the committed vector it replaced is gone").doesNotContain(rid);
    assertThat(activeVectors(TYPE)).isEqualTo(3);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("reachShapes")
  void aRefusedSaveAfterASaveThatChangedNoIndexedPropertyKeepsTheCommittedVector(final Reach how) {
    // no snapshot of an indexed state was taken (nothing indexed changed): the baseline is the committed buffer
    createVectorType(TYPE, false);
    final RID rid = insertSeed(TYPE);

    database.begin();
    reach(how, TYPE, rid).set("ext", 5).save();
    assertThatThrownBy(() -> reach(how, TYPE, rid).set("embedding", new float[] { Float.NaN, 0, 0, 1 }).save()).isInstanceOf(
        IllegalArgumentException.class);
    insert(TYPE, 100, 1, 1, 1, 1);
    database.commit();

    assertThat(countWithExt(TYPE, 100, 100)).isEqualTo(1);
    final Document stored = database.lookupByRID(rid, true).asDocument();
    assertThat(stored.getInteger("ext")).as("the accepted save of the other property stands").isEqualTo(5);
    assertThat((float[]) stored.get("embedding")).containsExactly(0, 1, 2, 3);
    assertThat(nearest(TYPE, new float[] { 0, 1, 2, 3 }, 1)).containsExactly(rid);
    assertThat(activeVectors(TYPE)).isEqualTo(2);
  }

  static Stream<Arguments> reachShapes() {
    return Stream.of(Arguments.of(Reach.LOOKUP), Arguments.of(Reach.SELECT));
  }

  @Test
  void aRefusedSaveOfARecordNotSavedBeforeInTheTransactionStaysClean() {
    // the case that was never broken: the record is taken out of the queue by the refusal
    createVectorType(TYPE, false);
    final RID rid = insertSeed(TYPE);

    database.begin();
    assertThatThrownBy(() -> reach(Reach.LOOKUP, TYPE, rid).set("embedding", new float[] { Float.NaN, 0, 0, 1 }).save()).isInstanceOf(
        IllegalArgumentException.class);
    insert(TYPE, 100, 1, 1, 1, 1);
    database.commit();

    assertThat(countWithExt(TYPE, 100, 100)).isEqualTo(1);
    assertThat(committedEmbedding(rid)).containsExactly(0, 1, 2, 3);
    assertThat(nearest(TYPE, new float[] { 0, 1, 2, 3 }, 1)).containsExactly(rid);
  }

  @Test
  void aRefusedSaveOfARecordCreatedInTheTransactionKeepsItsLastAcceptedVector() {
    // the insert then upsert of one key in a batch: the record is created by this transaction, updated (queued), then refused
    createVectorType(TYPE, false);

    database.begin();
    final MutableDocument created = database.newVertex(TYPE).set("ext", 100).set("embedding", new float[] { 1, 1, 1, 1 });
    created.save();
    created.set("embedding", new float[] { 9, 9, 9, 9 }).save();
    assertThatThrownBy(() -> created.set("embedding", new float[] { Float.NaN, 0, 0, 1 }).save()).isInstanceOf(IllegalArgumentException.class);
    insert(TYPE, 101, 2, 2, 2, 2);
    database.commit();

    assertThat(countWithExt(TYPE, 100, 101)).isEqualTo(2);
    assertThat(committedEmbedding(created.getIdentity())).containsExactly(9, 9, 9, 9);
    assertThat(nearest(TYPE, new float[] { 9, 9, 9, 9 }, 1)).containsExactly(created.getIdentity());
    assertThat(activeVectors(TYPE)).isEqualTo(2);
  }

  @Test
  void aRefusedSaveRightAfterTheCreateOfTheRecordInTheTransactionStoresTheCreatedVector() {
    createVectorType(TYPE, false);

    database.begin();
    final MutableDocument created = database.newVertex(TYPE).set("ext", 100).set("embedding", new float[] { 1, 1, 1, 1 });
    created.save();
    assertThatThrownBy(() -> created.set("embedding", new float[] { Float.NaN, 0, 0, 1 }).save()).isInstanceOf(IllegalArgumentException.class);
    insert(TYPE, 101, 2, 2, 2, 2);
    database.commit();

    assertThat(countWithExt(TYPE, 100, 101)).isEqualTo(2);
    assertThat(committedEmbedding(created.getIdentity())).containsExactly(1, 1, 1, 1);
    assertThat(nearest(TYPE, new float[] { 1, 1, 1, 1 }, 1)).containsExactly(created.getIdentity());
    assertThat(activeVectors(TYPE)).isEqualTo(2);
  }

  @Test
  void aValidSaveAfterTheRefusalIsWhatCommitsAndIsTheOnlyEntryTheIndexHolds() {
    createVectorType(TYPE, false);
    final RID rid = insertSeed(TYPE);

    database.begin();
    reach(Reach.LOOKUP, TYPE, rid).set("embedding", new float[] { 9, 9, 9, 9 }).save();
    assertThatThrownBy(() -> reach(Reach.LOOKUP, TYPE, rid).set("embedding", new float[] { Float.NaN, 0, 0, 1 }).save()).isInstanceOf(
        IllegalArgumentException.class);
    reach(Reach.LOOKUP, TYPE, rid).set("embedding", new float[] { 7, 7, 7, 7 }).save();
    database.commit();

    assertThat(committedEmbedding(rid)).containsExactly(7, 7, 7, 7);
    assertThat(nearest(TYPE, new float[] { 7, 7, 7, 7 }, 1)).containsExactly(rid);
    assertThat(activeVectors(TYPE)).as("one vector, the last accepted one").isEqualTo(1);
  }

  @Test
  void aRefusalLeavesTheEntriesOfTheOtherIndexesOfTheRecordAsTheLastAcceptedSaveLeftThem() {
    // ext is indexed too: the refused save changed it along with the embedding, and both go back
    createVectorType(TYPE, true);
    final RID rid = insertSeed(TYPE);

    database.begin();
    reach(Reach.LOOKUP, TYPE, rid).set("ext", 1).save();
    assertThatThrownBy(
        () -> reach(Reach.LOOKUP, TYPE, rid).set("ext", 2).set("embedding", new float[] { Float.NaN, 0, 0, 1 }).save()).isInstanceOf(
        IllegalArgumentException.class);
    insert(TYPE, 100, 1, 1, 1, 1);
    database.commit();

    assertThat(database.lookupByRID(rid, true).asDocument().getInteger("ext")).isEqualTo(1);
    final Index ext = database.getSchema().getType(TYPE).getPolymorphicIndexByProperties("ext");
    assertThat(ext.get(new Object[] { 1 }).next().getIdentity()).isEqualTo(rid);
    assertThat(ext.get(new Object[] { 2 }).hasNext()).as("the refused key is not indexed").isFalse();
    assertThat(ext.get(new Object[] { 0 }).hasNext()).as("the key the first save replaced is not indexed").isFalse();
    assertThat(countWithExt(TYPE, 100, 100)).isEqualTo(1);
  }

  @Test
  void aDuplicateRefusedInlineByAUniqueIndexLeavesTheQueuedRecordWithItsKey() {
    // not a vector: the same save path of any index that refuses an update of a queued record, here a unique key another record of
    // the transaction holds (checked inline, against the keys the transaction queued)
    database.getSchema().createDocumentType("Keyed").createProperty("name", Type.STRING);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Keyed", "name");

    database.begin();
    database.newDocument("Keyed").set("name", "a").set("row", 1).save();
    final MutableDocument mb = database.newDocument("Keyed").set("name", "b").set("row", 2);
    mb.save();
    mb.set("row", 20).save();
    mb.set("name", "a");
    assertThatThrownBy(mb::save).isInstanceOf(DuplicatedKeyException.class);
    // the caller goes on without putting the key back
    database.newDocument("Keyed").set("name", "c").set("row", 3).save();
    database.commit();

    assertThat(database.countType("Keyed", false)).isEqualTo(3);
    final Index byName = database.getSchema().getType("Keyed").getPolymorphicIndexByProperties("name");
    assertThat(byName.get(new Object[] { "b" }).next().asDocument().getInteger("row")).isEqualTo(20);
    assertThat(byName.get(new Object[] { "a" }).next().asDocument().getInteger("row")).isEqualTo(1);
    assertThat(byName.get(new Object[] { "c" }).hasNext()).isTrue();
  }

  @Test
  void aPrimaryKeyChangeRefusedByAPartitionedTypeLeavesTheQueuedRecordWithItsKey() {
    database.getSchema().createDocumentType("Partitioned", 3).createProperty("id", Type.INTEGER);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Partitioned", "id");
    database.getSchema().getType("Partitioned").setBucketSelectionStrategy("partitioned('id')");
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("Partitioned").set("id", 1).set("note", "x").save().getIdentity());

    database.begin();
    final MutableDocument d = database.lookupByRID(rid[0], true).asDocument().modify();
    d.set("note", "y").save();
    d.set("id", 2);
    assertThatThrownBy(d::save).hasMessageContaining("partitioned");
    database.newDocument("Partitioned").set("id", 7).save();
    database.commit();

    assertThat(database.countType("Partitioned", false)).isEqualTo(2);
    final Document stored = database.lookupByRID(rid[0], true).asDocument();
    assertThat(stored.getInteger("id")).isEqualTo(1);
    assertThat(stored.getString("note")).isEqualTo("y");
    final Index byId = database.getSchema().getType("Partitioned").getPolymorphicIndexByProperties("id");
    assertThat(byId.get(new Object[] { 1 }).next().getIdentity()).isEqualTo(rid[0]);
    assertThat(byId.get(new Object[] { 2 }).hasNext()).isFalse();
  }

  @Test
  void aRefusedSaveOfAnIndexedValueThatCannotBePutBackRefusesTheCommitInsteadOfStoringItSilently() {
    // The indexed value is a nested path of a map, which the snapshot of the indexed state holds resolved (a.k) and not as the map it
    // lives in: there is nothing to put back, so the transaction is refused its commit, as for a statement that cannot be taken back
    database.getSchema().createDocumentType("Nested").createProperty("a", Type.MAP);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Nested", "a.k");

    final RID[] rid = new RID[2];
    database.begin();
    rid[0] = database.newDocument("Nested").set("a", Map.of("k", 1)).save().getIdentity();
    rid[1] = database.newDocument("Nested").set("a", Map.of("k", 2)).save().getIdentity();
    final MutableDocument second = database.lookupByRID(rid[1], true).asDocument().modify();
    second.set("a", Map.of("k", 3)).save();
    second.set("a", Map.of("k", 1));
    assertThatThrownBy(second::save).isInstanceOf(DuplicatedKeyException.class);

    assertThatThrownBy(database::commit).isInstanceOf(TransactionException.class).hasMessageContaining("index refused");
    database.rollback();
    assertThat(database.countType("Nested", false)).as("nothing of the refused transaction is durable").isZero();
  }
}
