/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.IndexInternal.INDEX_STATUS;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract.NULL_STRATEGY;
import com.arcadedb.index.lsm.LSMTreeIndexMutable;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests on the schema-level behavior of type indexes: sub-index validity, propagation from super-types, the
 * per-bucket lookup limit, dropping while reading and the null strategy of the sub-indexes. Each nested class keeps the
 * fixture of the issue it was written for.
 */
class TypeIndexSchemaTest {

  /**
   * Regression test for issue #4743: when a compacted sub-index could not be resolved on load,
   * {@code LSMTreeIndexMutable.onAfterLoad()} called {@code database.getSchema().dropIndex(...)}.
   * <p>
   * That "self-repair" was wrong twice over. On a Raft replica the schema write threw
   * {@code ServerIsNotTheLeaderException} ("Changes to the schema must be executed on the leader server"),
   * which masked the real error, was then retried as if transient and finally escalated the whole database
   * to a snapshot resync - the exact sequence the reporter's leader log showed right after a failed
   * compaction left the schema pointing at a file id that no longer existed. And on a single node it
   * silently DELETED a user index because one of its files failed to load, turning a reportable and
   * repairable condition into data loss.
   * <p>
   * The index must survive: the unreadable sub-index is detached, the remaining (mutable) content stays
   * queryable, and the operator is told to run REBUILD INDEX.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @DisplayName("#4743 invalid sub-index")
  class InvalidSubIndex extends TestHelper {
    private static final String TYPE_NAME  = "Address";
    private static final String INDEX_NAME = "Address[uid]";
    private static final int    TOTAL      = 2_000;

    @Test
    void unresolvableSubIndexDetachesInsteadOfDroppingTheIndex() throws Exception {
      database.transaction(() -> {
        final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
        type.createProperty("uid", Integer.class);
        database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "uid" })
            .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).withPageSize(4096).create();
      });

      final int bucketId = database.getSchema().getType(TYPE_NAME).getFirstBucketId();
      final LSMTreeIndex bucketIndex = bucketIndex();
      database.transaction(() -> {
        for (int i = 0; i < TOTAL; i++)
          bucketIndex.getMutableIndex().put(new Object[] { i }, new RID[] { new RID(bucketId, i) });
      });

      // Compact so a sub-index (compacted) file exists and page 0 of the mutable index records its file id.
      bucketIndex.scheduleCompaction();
      bucketIndex.compact();
      assertThat(bucketIndex.getMutableIndex().getSubIndex())
          .as("the compaction must have produced a sub-index for this test to mean anything").isNotNull();

      // Point the mutable index at a file id that does not exist, reproducing the reporter's
      // "File with id '445' was not found": the compaction created the file, the schema recorded it, and
      // the replication of that schema change never completed.
      final int missingFileId = 9_999;
      writeSubIndexFileId(bucketIndex.getMutableIndex(), missingFileId);

      reopenDatabase();

      // Before the fix the index was gone from the schema entirely (or, under HA, the apply blew up).
      assertThat(database.getSchema().existsIndex(INDEX_NAME))
          .as("a sub-index that fails to load must NOT delete the whole index").isTrue();

      final LSMTreeIndex reloaded = bucketIndex();
      assertThat(reloaded.getMutableIndex().getSubIndex())
          .as("the unreadable sub-index must be detached, not kept in a broken state").isNull();

      // The index keeps working on what it can still read, and it accepts new entries.
      database.transaction(() -> {
        reloaded.getMutableIndex().put(new Object[] { TOTAL }, new RID[] { new RID(bucketId, TOTAL) });
      });
      assertThat(reloaded.get(new Object[] { TOTAL }).hasNext())
          .as("a degraded index must still serve its mutable content").isTrue();
    }

    /**
     * Overwrites the SUB-INDEX FILE ID slot in page 0 of the mutable index. The layout matches
     * {@code LSMTreeIndexMutable.createNewPage()}: maxContentSize(4) + entriesCount(4) + mutableFlag(1) +
     * compactedPages(4), then the sub-index file id.
     */
    private void writeSubIndexFileId(final LSMTreeIndexMutable mutable, final int fileId) {
      final int offset = Integer.BYTES + Integer.BYTES + Byte.BYTES + Integer.BYTES;
      final DatabaseInternal db = (DatabaseInternal) database;
      db.transaction(() -> {
        try {
          final MutablePage page = db.getTransaction()
              .getPageToModify(new PageId(db, mutable.getFileId(), 0), mutable.getPageSize(), false);
          page.writeInt(offset, fileId);
        } catch (final IOException e) {
          throw new IllegalStateException(e);
        }
      });
    }

    private LSMTreeIndex bucketIndex() {
      final TypeIndex typeIndex = database.getSchema().getType(TYPE_NAME).getIndexesByProperties("uid").getFirst();
      for (final Index index : typeIndex.getIndexesOnBuckets())
        if (index instanceof LSMTreeIndex lsmIndex)
          return lsmIndex;
      throw new IllegalStateException("No LSM bucket index found for type " + TYPE_NAME);
    }
  }

  /**
   * Regression test for issue #6359, item 1: linking a super type propagates its indexes over buckets that ALREADY hold
   * records, so that build has exactly the problem {@code CREATE INDEX} had in issue #6324 - it must see the writes of
   * the transaction it runs in.
   * <p>
   * {@code LocalDocumentType.addSuperType} calls {@code LocalSchema.createBucketIndex} directly instead of going through
   * {@link com.arcadedb.schema.TypeIndexBuilder} / {@link com.arcadedb.schema.BucketIndexBuilder}, so it kept building in
   * a transaction of its own: the caller's records are not committed, the scan does not see them, and they were saved
   * before the index existed to stage an entry for. The result was an index that is readable, reported healthy by
   * {@code CHECK DATABASE}, and answers the lookup it exists for with nothing.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @DisplayName("#6359 super-type index propagation")
  class SuperTypeIndexPropagation extends TestHelper {

    /** The API shape: write into the subtype, then link the super type whose index has to cover those writes. */
    @Test
    @Timeout(60)
    void anIndexPropagatedByAddSuperTypeCoversTheCallersUncommittedRecords() {
      database.transaction(() -> {
        database.getSchema().createDocumentType("Super", 1).createProperty("id", Type.INTEGER);
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Super", "id");
        database.getSchema().createDocumentType("Sub", 1);
      });

      database.transaction(() -> {
        for (int i = 0; i < 10; i++) {
          final MutableDocument v = database.newDocument("Sub");
          v.set("id", i);
          v.save();
        }
        database.getSchema().getType("Sub").addSuperType("Super");
      });

      assertThat(database.countType("Sub", false)).as("the records are there").isEqualTo(10);

      final Index index = database.getSchema().getIndexByName("Super[id]");
      assertThat(index.countEntries()).as("and so are their index entries").isEqualTo(10);
      for (int i = 0; i < 10; i++)
        assertThat(index.get(new Object[] { i }).hasNext()).as("id " + i + " must be found through the index").isTrue();
    }

    /** The same through SQL, which is how a user reaches it: {@code ALTER TYPE ... SUPERTYPE +...} in one script. */
    @Test
    @Timeout(60)
    void theSameThroughOneSqlScript() {
      database.transaction(() -> {
        database.getSchema().createDocumentType("Super", 1).createProperty("id", Type.INTEGER);
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Super", "id");
        database.getSchema().createDocumentType("Sub", 1);
      });

      database.transaction(() -> database.command("sqlscript",
          "INSERT INTO Sub SET id = 7; ALTER TYPE Sub SUPERTYPE +Super;").close());

      final Index index = database.getSchema().getIndexByName("Super[id]");
      assertThat(index.countEntries()).isEqualTo(1);
      assertThat(index.get(new Object[] { 7 }).hasNext()).as("an index that answers the lookup it exists for").isTrue();
    }

    /**
     * A record UPDATED in the same transaction is indexed under its new key: the deferred update is parked and only
     * serialized at commit, so a bucket scan inside the transaction still reads the OLD content unless the build asks
     * the transaction for the written copy.
     */
    @Test
    @Timeout(60)
    void aRecordUpdatedInTheSameTransactionIsPropagatedUnderItsNewKey() {
      database.transaction(() -> {
        database.getSchema().createDocumentType("Super", 1).createProperty("id", Type.INTEGER);
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Super", "id");
        database.getSchema().createDocumentType("Sub", 1);
      });

      database.transaction(() -> {
        final MutableDocument v = database.newDocument("Sub");
        v.set("id", 1);
        v.save();
      });

      database.transaction(() -> {
        database.command("sql", "UPDATE Sub SET id = 42").close();
        database.getSchema().getType("Sub").addSuperType("Super");
      });

      final Index index = database.getSchema().getIndexByName("Super[id]");
      assertThat(index.countEntries()).isEqualTo(1);
      assertThat(index.get(new Object[] { 42 }).hasNext()).as("the new key is the one the index answers on").isTrue();
      assertThat(index.get(new Object[] { 1 }).hasNext()).as("and the old key is not").isFalse();
    }

    /**
     * The propagated index COMPONENT is committed on its own, whatever the caller's transaction goes on to do: the
     * schema entry naming it is written by {@code recordFileChanges} regardless, so an index whose first page was left
     * in a rolled-back transaction would be a schema entry pointing at a file with no pages, and the next write to it
     * fails with "the file is invalid".
     */
    @Test
    @Timeout(60)
    void aPropagatedIndexSurvivesTheRollbackOfTheTransactionThatPropagatedIt() {
      database.transaction(() -> {
        database.getSchema().createDocumentType("Super", 1).createProperty("id", Type.INTEGER);
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Super", "id");
        database.getSchema().createDocumentType("Sub", 1);
      });

      database.begin();
      final MutableDocument doomed = database.newDocument("Sub");
      doomed.set("id", 1);
      doomed.save();
      database.getSchema().getType("Sub").addSuperType("Super");
      database.rollback();

      assertThat(database.countType("Sub", false)).as("the record went with the rollback").isZero();

      final Index index = database.getSchema().getIndexByName("Super[id]");
      assertThat(index.countEntries()).as("and the propagated entry with it").isZero();

      // The point of the test: the propagated sub-index is WRITEABLE afterwards, not merely present.
      database.transaction(() -> {
        final MutableDocument v = database.newDocument("Sub");
        v.set("id", 7);
        v.save();
      });
      assertThat(index.get(new Object[] { 7 }).hasNext()).isTrue();
    }

    /**
     * The other side of seeing the caller's pending writes: a UNIQUE super-type index propagated over records that
     * already conflict now REFUSES, where the separate-transaction build silently produced an index missing one of
     * them. The refusal has to reach the caller, and to leave nothing behind.
     * <p>
     * Two cleanups are needed for that, and neither can hang off the transaction's error callback - a
     * {@code DuplicatedKeyException} raised inside a JOINED transaction is rethrown immediately by
     * {@code LocalDatabase.transaction} (issue #661 - retrying would roll back a transaction it does not own), so the
     * callback never runs. The half-built COMPONENTS are already committed and attached to the super type's
     * {@code TypeIndex} by then, and leaving them would answer the lookup from an index that was never populated. And
     * the in-memory LINK has to go back as well: the transaction retries a duplicate once (#4959), and on that retry
     * this method would find the super type already in place, return early without propagating anything, and let the
     * transaction COMMIT - handing back a linked super type whose index holds no entry for a single one of the
     * subtype's records, which is this issue's own defect reached through the retry rather than through the scan.
     */
    @Test
    @Timeout(60)
    void aPropagationThatHitsADuplicateRefusesAndLeavesNeitherIndexNorLinkBehind() {
      database.transaction(() -> {
        database.getSchema().createDocumentType("Super", 1).createProperty("id", Type.INTEGER);
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Super", "id");
        database.getSchema().createDocumentType("Sub", 1);
      });

      final int subBucketId = database.getSchema().getType("Sub").getBuckets(false).getFirst().getFileId();

      assertThatThrownBy(() -> database.transaction(() -> {
        for (int i = 0; i < 2; i++) {
          final MutableDocument v = database.newDocument("Sub");
          v.set("id", 5);
          v.save();
        }
        database.getSchema().getType("Sub").addSuperType("Super");
      })).as("the duplicate reaches the caller instead of being swallowed by the retry")
          .isInstanceOf(DuplicatedKeyException.class);

      assertThat(database.getSchema().getType("Sub").getSuperTypes())
          .as("a propagation that failed leaves the type as unlinked as it found it").isEmpty();
      assertThat(database.countType("Sub", false)).as("and the caller's records went with its rollback").isZero();

      // Asked of the SUPER TYPE's index, which is the one a propagated sub-index attaches to: a half-built sub-index
      // left registered on it would answer `Super[id]` lookups over Sub's bucket with nothing.
      final TypeIndex propagated = (TypeIndex) database.getSchema().getIndexByName("Super[id]");
      for (final IndexInternal sub : propagated.getIndexesOnBuckets())
        assertThat(sub.getAssociatedBucketId()).as("no sub-index survives on the bucket the failed build scanned")
            .isNotEqualTo(subBucketId);

      // And the index is still the working index of its own type afterwards.
      database.transaction(() -> {
        final MutableDocument v = database.newDocument("Super");
        v.set("id", 9);
        v.save();
      });
      assertThat(propagated.get(new Object[] { 9 }).hasNext()).isTrue();

      // The link can still be made, once the conflict is gone: the refusal took nothing away permanently.
      database.transaction(() -> {
        final MutableDocument v = database.newDocument("Sub");
        v.set("id", 11);
        v.save();
        database.getSchema().getType("Sub").addSuperType("Super");
      });
      assertThat(propagated.get(new Object[] { 11 }).hasNext()).isTrue();
    }

    /**
     * The cleanup covers EVERY sub-index the propagation made, not only the ones that still had a build outstanding.
     * <p>
     * An index family that cannot share the caller's transaction - the vector ones, whose search path reads through the
     * page cache rather than through the transaction - is built INLINE while the components are created, so it never
     * reaches the list of pending builds. A super type carrying both a vector index and an ordinary one therefore has
     * one of each, and a cleanup keyed on the pending list alone left the vector sub-index committed and attached to a
     * type relationship that had just been undone.
     * <p>
     * Getting there also required {@link com.arcadedb.index.vector.LSMVectorIndex} to answer which {@code TypeIndex} it
     * belongs to. It used to answer null, so {@code LocalSchema.dropIndexInternal} never detached a dropped vector
     * sub-index from its wrapper - on every path that drops one, not only this one - and {@code addSuperType} could not
     * recognise an already-propagated vector index, minting a second one on the retry.
     */
    @Test
    @Timeout(60)
    void aRefusalTakesAwayTheIndexesThatWereBuiltInlineToo() {
      database.transaction(() -> {
        final DocumentType superType = database.getSchema().createDocumentType("Super", 1);
        superType.createProperty("id", Type.INTEGER);
        superType.createProperty("embedding", Type.ARRAY_OF_FLOATS);
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Super", "id");
        database.command("sql",
            "CREATE INDEX ON Super (embedding) LSM_VECTOR METADATA {\"dimensions\": 4, \"similarity\": \"cosine\"}").close();
        database.getSchema().createDocumentType("Sub", 1);
      });

      final int subBucketId = database.getSchema().getType("Sub").getBuckets(false).getFirst().getFileId();

      assertThatThrownBy(() -> database.transaction(() -> {
        for (int i = 0; i < 2; i++) {
          final MutableDocument v = database.newDocument("Sub");
          v.set("id", 5);
          v.set("embedding", new float[] { 1, 2, 3, 4 });
          v.save();
        }
        database.getSchema().getType("Sub").addSuperType("Super");
      })).isInstanceOf(DuplicatedKeyException.class);

      assertThat(database.getSchema().getType("Sub").getSuperTypes()).isEmpty();

      // BOTH wrappers, because the vector one is the index that was built inline and the whole point of the test.
      for (final String indexName : new String[] { "Super[id]", "Super[embedding]" })
        for (final IndexInternal sub : ((TypeIndex) database.getSchema().getIndexByName(indexName)).getIndexesOnBuckets())
          assertThat(sub.getAssociatedBucketId()).as(indexName + " keeps no sub-index on the subtype's bucket")
              .isNotEqualTo(subBucketId);
    }

    /**
     * The propagation holds the async workers while it builds, like every other index build does (issue #6303, item 2).
     * <p>
     * The barrier answers about the past; a build needs the other half too - that nothing WRITES during the scan. A
     * record an async worker saves in the window between the barrier and the sub-index's registration is in neither the
     * scan nor the index, which is the same gap this propagation exists to close, reopened for async writers. This is
     * the end-to-end shape: the executor is genuinely busy while the super type is linked.
     * <p>
     * The timeout is a HANG DETECTOR and not a latency bound, which is why it is this generous and why the test is not
     * in the {@code slow} lane: the records are queued rather than awaited, so it runs in milliseconds, but a
     * quiescence that cannot park a worker gives up only after its own 60-second budget and the failure has to arrive
     * as that refusal rather than as a timeout.
     */
    @Test
    @Timeout(180)
    void aPropagationRunWhileTheExecutorIsBusyCoversEveryRecord() {
      final int records = 1_000;

      database.transaction(() -> {
        database.getSchema().createDocumentType("Super", 1).createProperty("id", Type.INTEGER);
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Super", "id");
        database.getSchema().createDocumentType("Sub", 1);
      });
      database.async().setParallelLevel(2);

      for (int i = 0; i < records; i++) {
        final MutableDocument v = database.newDocument("Sub");
        v.set("id", i);
        database.async().createRecord(v, null);
      }

      database.getSchema().getType("Sub").addSuperType("Super");

      database.async().waitCompletion();

      assertThat(database.countType("Sub", false)).isEqualTo(records);
      final Index propagated = database.getSchema().getIndexByName("Super[id]");
      assertThat(propagated.countEntries())
          .as("every record must have an entry, whatever the async side was doing while the index was propagated")
          .isEqualTo(records);
    }

    /**
     * The link is handed back on EVERY refusal, not only on the one the index propagation raises.
     * <p>
     * Linking a super type applies the linkage first and then does three things that can each still refuse: pairing
     * external buckets, propagating the indexes, and inheriting the bucket selection strategy. Only the second used to
     * roll the linkage back, so a refusal from either of the others handed the caller a type that IS a subtype with
     * none of what being one implies. The shape below is the third: a partition that is legal on the super type and
     * unsuitable for this subtype, whose own index on the partition key is declared {@code COLLATE CI} (#5637).
     */
    @Test
    @Timeout(60)
    void aRefusalFromOutsideTheIndexPropagationHandsTheLinkBackToo() {
      database.transaction(() -> {
        database.getSchema().buildDocumentType().withName("Super").withTotalBuckets(4).create();
        database.command("sql", "CREATE PROPERTY Super.k STRING").close();
        database.command("sql", "CREATE INDEX ON Super (k) UNIQUE").close();
        database.command("sql", "ALTER TYPE Super BucketSelectionStrategy `partitioned('k')`").close();

        database.getSchema().buildDocumentType().withName("Sub").withTotalBuckets(4).create();
        database.command("sql", "CREATE PROPERTY Sub.k STRING").close();
        database.command("sql", "CREATE INDEX ON Sub (k COLLATE CI) UNIQUE").close();
      });

      assertThatThrownBy(() -> database.transaction(() -> database.getSchema().getType("Sub").addSuperType("Super")))
          .isInstanceOf(SchemaException.class).hasMessageContaining("COLLATE CI");

      assertThat(database.getSchema().getType("Sub").getSuperTypes())
          .as("the subtype is as unlinked as the refusal left it").isEmpty();
      assertThat(database.getSchema().getType("Super").getSubTypes())
          .as("and so is the super type, on the other side of the same link").isEmpty();

      // AND the indexes the propagation had already committed go with it. This refusal comes AFTER the propagation
      // succeeded, so the sub-indexes exist and are attached to the super type's wrapper by the time it is raised;
      // leaving them would point the super type's index at buckets belonging to a type that is no longer its subtype.
      final List<Integer> subBucketIds = database.getSchema().getType("Sub").getBuckets(false).stream()
          .map(Bucket::getFileId).toList();
      for (final IndexInternal sub : ((TypeIndex) database.getSchema().getIndexByName("Super[k]")).getIndexesOnBuckets())
        assertThat(subBucketIds).as("no propagated sub-index survives the refusal that followed it")
            .doesNotContain(sub.getAssociatedBucketId());

      // The refusal took nothing away permanently: a subtype the partition IS suitable for still links.
      database.transaction(() -> {
        database.getSchema().buildDocumentType().withName("Ok").withTotalBuckets(4).create();
        database.getSchema().getType("Ok").addSuperType("Super");
      });
      assertThat(database.getSchema().getType("Ok").getSuperTypes()).hasSize(1);
    }

    /**
     * The FIRST of the three post-linkage steps, and the last arm of that guard nothing asserted: pairing the external
     * buckets.
     * <p>
     * A super type carrying an EXTERNAL property (own or inherited) means every subtype has to own a paired
     * {@code <primaryBucket>_ext} bucket of its own, because records of a subtype live in the subtype's buckets. That
     * pairing refuses on its own terms - {@code ensureExternalBucketFor} will not adopt a bucket that is already some
     * user type's PRIMARY bucket - so no fault injection is needed to reach it: name a primary bucket after the one the
     * subtype's pairing would want, and the refusal arrives from step one, before an index has been propagated or a
     * strategy inherited.
     * <p>
     * What it pins is that the refusal leaves the type as unlinked as it found it, on BOTH sides of the link, and with
     * no half-adopted external bucket recorded against it.
     */
    @Test
    @Timeout(60)
    void aRefusalFromTheExternalBucketPairingHandsTheLinkBackToo() {
      database.transaction(() -> {
        database.getSchema().createDocumentType("Super", 1).createProperty("payload", Type.STRING).setExternal(true);
        database.getSchema().createDocumentType("Sub", 1);
        database.getSchema().createDocumentType("Squatter", 1);
      });

      // The name Sub's paired external bucket WOULD take, claimed first as a primary bucket of an unrelated type.
      final String contended = database.getSchema().getType("Sub").getBuckets(false).getFirst().getName() + "_ext";
      database.transaction(() -> database.command("sql", "ALTER TYPE Squatter BUCKET +`" + contended + "`").close());

      assertThatThrownBy(() -> database.transaction(() -> database.getSchema().getType("Sub").addSuperType("Super")))
          .isInstanceOf(SchemaException.class).hasMessageContaining("already a primary bucket of another user type");

      assertThat(database.getSchema().getType("Sub").getSuperTypes())
          .as("the subtype is as unlinked as the refusal left it").isEmpty();
      assertThat(database.getSchema().getType("Super").getSubTypes())
          .as("and so is the super type, on the other side of the same link").isEmpty();
      assertThat(((LocalDocumentType) database.getSchema().getType("Sub")).hasExternalBuckets())
          .as("and nothing was half-adopted on the way out").isFalse();
      assertThat(database.getSchema().getType("Squatter").getBuckets(false).stream().map(Bucket::getName))
          .as("the contended bucket still belongs to whoever owned it").contains(contended);

      // The refusal took nothing away permanently: with the collision gone, the very same link is made and the pairing
      // this time creates the bucket it could not adopt.
      database.transaction(() -> {
        database.command("sql", "ALTER TYPE Squatter BUCKET -`" + contended + "`").close();
        database.command("sql", "DROP BUCKET `" + contended + "`").close();
      });
      database.transaction(() -> database.getSchema().getType("Sub").addSuperType("Super"));

      assertThat(database.getSchema().getType("Sub").getSuperTypes()).hasSize(1);
      final LocalDocumentType sub = (LocalDocumentType) database.getSchema().getType("Sub");
      assertThat(sub.getExternalBucketIdFor(sub.getBuckets(false).getFirst().getFileId()))
          .as("the inherited EXTERNAL property now has a bucket of its own to write into").isNotNull();

      // And it is a working one, not merely a registered one.
      database.transaction(() -> database.newDocument("Sub").set("payload", "x".repeat(4096)).save());
      assertThat(database.countType("Sub", false)).isEqualTo(1);
    }

    /**
     * The sibling call site, {@code LocalDocumentType.createBucket}, is SAFE and must not be "fixed" by symmetry: the
     * bucket it indexes has just been created, so no transaction can hold uncommitted writes into it and there is
     * nothing for the scan to miss. Nailed down here so the property is asserted rather than merely asserted about -
     * the records already in the type's OTHER buckets keep their entries, and the new bucket gets its own sub-index.
     */
    @Test
    @Timeout(60)
    void addingABucketToAPopulatedTypeInsideATransactionKeepsTheIndexComplete() {
      database.transaction(() -> {
        final DocumentType type = database.getSchema().createDocumentType("V", 1);
        type.createProperty("id", Type.INTEGER);
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "V", "id");
      });

      database.transaction(() -> {
        for (int i = 0; i < 5; i++) {
          final MutableDocument v = database.newDocument("V");
          v.set("id", i);
          v.save();
        }
      });

      database.transaction(() -> {
        final MutableDocument v = database.newDocument("V");
        v.set("id", 100);
        v.save();
        database.command("sql", "ALTER TYPE V BUCKET +V_extra").close();
      });

      final Index index = database.getSchema().getIndexByName("V[id]");
      assertThat(index.countEntries()).isEqualTo(6);
      for (int i = 0; i < 5; i++)
        assertThat(index.get(new Object[] { i }).hasNext()).as("id " + i).isTrue();
      assertThat(index.get(new Object[] { 100 }).hasNext()).as("the record written in the same transaction").isTrue();
    }
  }

  /**
   * Regression test for issue #7795: {@code TypeIndex.get(keys, limit)} asked each bucket sub-index for
   * {@code collected - limit} rows instead of {@code limit - collected}. The value was always negative, which every
   * sub-index reads as "no limit", so each bucket materialized all its entries for the key and the type index threw the
   * surplus away.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @DisplayName("#7795 type index bucket limit")
  class TypeIndexBucketLimit extends TestHelper {
    private static final int BUCKETS  = 4;
    private static final int PER_KEY  = 40;

    @Test
    void eachBucketIsAskedOnlyForTheRowsStillMissing() {
      final DocumentType type = seed();
      final TypeIndex real = (TypeIndex) database.getSchema().getIndexByName("Item[key]");

      // THE SAME BUCKET INDEXES, BEHIND A PROXY THAT RECORDS THE LIMIT EACH ONE IS ASKED FOR
      final List<Integer> limits = new ArrayList<>();
      final TypeIndex probe = new TypeIndex("probe", type);
      for (final IndexInternal bucketIndex : real.getIndexesOnBuckets())
        probe.addIndexOnBucket(recordingLimits(bucketIndex, limits));

      database.transaction(() -> {
        // HOW MANY ENTRIES EACH BUCKET HOLDS FOR THE KEY, IN THE ORDER THE TYPE INDEX VISITS THEM
        final IndexInternal[] buckets = real.getIndexesOnBuckets();
        final int[] perBucket = new int[buckets.length];
        for (int b = 0; b < buckets.length; b++)
          try (final IndexCursor cursor = buckets[b].get(new Object[] { "k" })) {
            while (cursor.hasNext()) {
              cursor.next();
              perBucket[b]++;
            }
          }

        for (final int limit : new int[] { 1, 5, 15, PER_KEY - 1 }) {
          limits.clear();
          final List<Object> rows = new ArrayList<>();
          probe.get(new Object[] { "k" }, limit).forEachRemaining(rows::add);
          assertThat(rows).hasSize(limit);

          // EVERY BUCKET IS ASKED FOR EXACTLY WHAT THE BUCKETS BEFORE IT LEFT MISSING, AND NONE AFTER THE LIMIT IS MET
          final List<Integer> expected = new ArrayList<>();
          int remaining = limit;
          for (int b = 0; b < buckets.length && remaining > 0; b++) {
            expected.add(remaining);
            remaining -= Math.min(remaining, perBucket[b]);
          }
          assertThat(limits).as("limits pushed down under limit %d", limit).isEqualTo(expected);
        }
        assertThat(limits).as("limit %d must span more than one bucket", PER_KEY - 1).hasSizeGreaterThan(1);

        // NO LIMIT STAYS NO LIMIT
        limits.clear();
        final List<Object> all = new ArrayList<>();
        probe.get(new Object[] { "k" }, -1).forEachRemaining(all::add);
        assertThat(all).hasSize(PER_KEY);
        assertThat(limits).hasSize(BUCKETS).containsOnly(-1);
      });
    }

    /** The row count the real type index answers is unchanged by the fix. */
    @Test
    void theTypeIndexStillAnswersExactlyTheLimit() {
      seed();
      final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Item[key]");
      database.transaction(() -> {
        for (final int limit : new int[] { 1, 7, PER_KEY, PER_KEY + 10 }) {
          final List<Object> rows = new ArrayList<>();
          index.get(new Object[] { "k" }, limit).forEachRemaining(rows::add);
          assertThat(rows).hasSize(Math.min(limit, PER_KEY));
        }
      });
    }

    private DocumentType seed() {
      final DocumentType[] type = new DocumentType[1];
      database.transaction(() -> {
        type[0] = database.getSchema().createDocumentType("Item", BUCKETS);
        type[0].createProperty("key", Type.STRING);
        database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Item", "key");
        for (int i = 0; i < PER_KEY; i++)
          database.newDocument("Item").set("key", "k").save();
      });
      // THE KEY'S ROWS MUST BE SPREAD, OR THERE IS NO SECOND BUCKET TO PUSH A LIMIT INTO
      for (final IndexInternal bucketIndex : ((TypeIndex) database.getSchema().getIndexByName("Item[key]")).getIndexesOnBuckets())
        assertThat(bucketIndex.countEntries()).isPositive();
      return type[0];
    }

    private static IndexInternal recordingLimits(final IndexInternal delegate, final List<Integer> limits) {
      return (IndexInternal) Proxy.newProxyInstance(IndexInternal.class.getClassLoader(), new Class<?>[] { IndexInternal.class },
          (proxy, method, args) -> {
            if (method.getName().equals("setTypeIndex"))
              // THE REAL BUCKET INDEX KEEPS POINTING AT ITS REAL TYPE INDEX
              return null;
            if (method.getName().equals("get") && args != null && args.length == 2 && args[1] instanceof Integer limit)
              limits.add(limit);
            try {
              return method.invoke(delegate, args);
            } catch (final InvocationTargetException e) {
              throw e.getCause();
            }
          });
    }
  }

  /**
   * Issue #9651, engine side: a drop takes the sub-indexes of a type index away one at a time while the index still answers as
   * valid. A cursor opened in that window read the live sub-index list a second time and, finding it emptied, raised a
   * {@link java.util.NoSuchElementException}, which a Gremlin traversal takes for the end of the stream: an empty answer
   * returned as a success. The index now reads one snapshot and reports an emptied index as the stale-index
   * {@link IndexException} every reader handles, is not ready for queries while a drop is under way, and tells a reader
   * whether what it read is still the whole index.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @DisplayName("#9651 type index concurrent drop and read")
  class TypeIndexConcurrentDropRead extends TestHelper {

    @Test
    void anIndexWhoseSubIndexesAreGoneIsReportedAsStaleNotAsAnEmptyStream() {
      final TypeIndex index = createIndex();
      final IndexInternal[] snapshot = index.getIndexesOnBuckets();
      assertThat(index.isUnchangedSince(snapshot)).isTrue();

      // the state a concurrent drop leaves for a moment: the wrapper still valid, its sub-indexes gone
      for (final IndexInternal subIndex : snapshot)
        index.removeIndexOnBucket(subIndex);
      try {
        assertThat(index.isValid()).isTrue();
        assertThat(index.isReadyForQueries()).isFalse();
        assertThat(index.isUnchangedSince(snapshot)).isFalse();
        assertThatThrownBy(() -> index.iterator(true, new Object[] { 1L }, true)).isInstanceOf(IndexException.class);
        assertThatThrownBy(() -> index.iterator(true)).isInstanceOf(IndexException.class);
        assertThatThrownBy(() -> index.range(true, new Object[] { 1L }, true, new Object[] { 5L }, true))
            .isInstanceOf(IndexException.class);
      } finally {
        for (final IndexInternal subIndex : snapshot)
          index.addIndexOnBucket(subIndex);
      }
      assertThat(index.isUnchangedSince(snapshot)).isTrue();
    }

    @Test
    void aDroppedIndexIsNoLongerUnchangedAndItsReadsAreStale() {
      final TypeIndex index = createIndex();
      final IndexInternal[] snapshot = index.getIndexesOnBuckets();

      database.getSchema().dropIndex(index.getName());

      assertThat(index.isUnchangedSince(snapshot)).isFalse();
      assertThat(index.isReadyForQueries()).isFalse();
      assertThat(TypeIndex.isStaleRead(index, new IndexException("gone"))).isTrue();
      assertThat(TypeIndex.isStaleRead(index, new IllegalArgumentException("File with id 7 was not found"))).isTrue();
      assertThat(TypeIndex.isStaleRead(index, new IllegalArgumentException("something else"))).isFalse();
    }

    @Test
    void aDropRefusedBeforeTakingAnythingAwayLeavesTheIndexReady() {
      final TypeIndex index = createIndex();
      final IndexInternal subIndex = index.getIndexesOnBuckets()[0];
      // a sub-index busy compacting: the drop cannot acquire it and is refused before removing anything
      assertThat(subIndex.setStatus(new IndexInternal.INDEX_STATUS[] { IndexInternal.INDEX_STATUS.AVAILABLE },
          IndexInternal.INDEX_STATUS.COMPACTION_IN_PROGRESS)).isTrue();
      try {
        assertThatThrownBy(index::drop).isInstanceOf(NeedRetryException.class);
      } finally {
        subIndex.setStatus(new IndexInternal.INDEX_STATUS[] { IndexInternal.INDEX_STATUS.COMPACTION_IN_PROGRESS },
            IndexInternal.INDEX_STATUS.AVAILABLE);
      }

      assertThat(index.isValid()).isTrue();
      assertThat(index.isReadyForQueries()).isTrue();
      assertThat(index.isUnchangedSince(index.getIndexesOnBuckets())).isTrue();
    }

    private TypeIndex createIndex() {
      final DocumentType type = database.getSchema().createDocumentType("Doc");
      type.createProperty("n", Type.LONG);
      database.transaction(() -> {
        for (long i = 0; i < 10; i++)
          database.newDocument("Doc").set("n", i).save();
      });
      return type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "n");
    }
  }

  /**
   * Regression test for issue #10081: {@link TypeIndex#setNullStrategy} applied the strategy to the first bucket's index only,
   * while its getter read that same first index. The change read back as applied, but the other buckets kept enforcing the old
   * strategy, so the same null-key insert was refused or accepted depending on the bucket the record happened to land in.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  @DisplayName("#10081 type index null strategy")
  class TypeIndexNullStrategy extends TestHelper {
    private static final String TYPE_NAME = "V10081";
    private static final int    BUCKETS   = 4;

    @Test
    void setNullStrategyReachesEveryBucketIndex() {
      final TypeIndex typeIndex = createTypeIndex();
      assertThat(typeIndex.getIndexesOnBuckets()).hasSize(BUCKETS);
      assertThat(strategies(typeIndex)).containsOnly(NULL_STRATEGY.SKIP);

      typeIndex.setNullStrategy(NULL_STRATEGY.ERROR);

      assertThat(typeIndex.getNullStrategy()).isEqualTo(NULL_STRATEGY.ERROR);
      assertThat(strategies(typeIndex)).as("every bucket index must enforce the strategy the facade reports").hasSize(BUCKETS)
          .containsOnly(NULL_STRATEGY.ERROR);
    }

    @Test
    void nullKeyIsRefusedOnEveryBucketAfterTheChange() {
      final TypeIndex typeIndex = createTypeIndex();
      final DocumentType type = database.getSchema().getType(TYPE_NAME);
      final List<String> bucketNames = new ArrayList<>();
      type.getBuckets(false).forEach(bucket -> bucketNames.add(bucket.getName()));
      assertThat(bucketNames).hasSize(BUCKETS);

      typeIndex.setNullStrategy(NULL_STRATEGY.ERROR);

      // the landing bucket is chosen by the bucket selection strategy: the outcome must not depend on it
      for (final String bucketName : bucketNames)
        assertThatThrownBy(() -> database.transaction(() -> database.newDocument(TYPE_NAME).save(bucketName)))
            .as("a null key must be refused on bucket %s", bucketName).rootCause().hasMessageContaining("cannot be NULL");

      assertThat(database.countType(TYPE_NAME, true)).isZero();
    }

    @Test
    void newStrategyIsKeptAfterReopen() {
      final TypeIndex typeIndex = createTypeIndex();
      typeIndex.setNullStrategy(NULL_STRATEGY.INDEX);
      // the strategy is persisted with the schema: force a save, as any later schema change would
      database.getSchema().getType(TYPE_NAME).createProperty("other", Type.STRING);

      reopenDatabase();

      final TypeIndex reloaded = (TypeIndex) database.getSchema().getIndexByName(TYPE_NAME + "[k]");
      assertThat(reloaded.getNullStrategy()).isEqualTo(NULL_STRATEGY.INDEX);
      assertThat(strategies(reloaded)).hasSize(BUCKETS).containsOnly(NULL_STRATEGY.INDEX);
    }

    /**
     * Every write on {@link TypeIndex} must either fan out over all the bucket indexes or refuse: a setter that borrows the
     * single-index read helper changes one bucket and reads back as applied. A new {@code setXxx} on the {@link Index} or
     * {@link IndexInternal} interfaces fails here until it is classified.
     */
    @Test
    void everySetterAppliesToEveryBucketIndexOrRefuses() throws Exception {
      final TypeIndex typeIndex = createTypeIndex();

      final Set<Method> setters = new LinkedHashSet<>();
      for (final Class<?> contract : new Class<?>[] { Index.class, IndexInternal.class })
        for (final Method method : contract.getMethods())
          if (method.getName().startsWith("set"))
            setters.add(TypeIndex.class.getMethod(method.getName(), method.getParameterTypes()));

      assertThat(setters).as("the Index and IndexInternal setters").isNotEmpty();

      for (final Method setter : setters) {
        switch (setter.getName() + "/" + setter.getParameterCount()) {
        case "setNullStrategy/1" -> {
          typeIndex.setNullStrategy(NULL_STRATEGY.INDEX);
          assertThat(strategies(typeIndex)).as(setter.toString()).hasSize(BUCKETS).containsOnly(NULL_STRATEGY.INDEX);
        }
        case "setMetadata/1", "setTypeIndex/1" -> assertRefuses(setter, typeIndex);
        case "setStatus/2" -> {
          // a facade has no status of its own: it must not report a change that no bucket index went through
          assertThat(typeIndex.setStatus(new INDEX_STATUS[] { INDEX_STATUS.AVAILABLE }, INDEX_STATUS.UNAVAILABLE))
              .as(setter.toString()).isFalse();
        }
        default -> throw new AssertionError("Unclassified setter " + setter + ": TypeIndex must apply it to every bucket index "
            + "(loop over indexesOnBuckets) or refuse it, never delegate it to getFirstUnderlyingIndex(). Add it to this test.");
        }
      }
    }

    private void assertRefuses(final Method setter, final TypeIndex typeIndex) {
      final Object[] args = new Object[setter.getParameterCount()];
      assertThatThrownBy(() -> {
        try {
          setter.invoke(typeIndex, args);
        } catch (final InvocationTargetException e) {
          throw e.getCause();
        }
      }).as(setter.toString()).isInstanceOf(UnsupportedOperationException.class);
    }

    private TypeIndex createTypeIndex() {
      database.transaction(() -> {
        final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(BUCKETS).create();
        type.createProperty("k", Type.INTEGER);
        database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "k" }).withType(Schema.INDEX_TYPE.LSM_TREE)
            .withUnique(false).withNullStrategy(NULL_STRATEGY.SKIP).create();
      });
      return (TypeIndex) database.getSchema().getIndexByName(TYPE_NAME + "[k]");
    }

    private static NULL_STRATEGY[] strategies(final TypeIndex typeIndex) {
      final IndexInternal[] onBuckets = typeIndex.getIndexesOnBuckets();
      final NULL_STRATEGY[] result = new NULL_STRATEGY[onBuckets.length];
      for (int i = 0; i < onBuckets.length; i++)
        result[i] = onBuckets[i].getNullStrategy();
      return result;
    }
  }
}
