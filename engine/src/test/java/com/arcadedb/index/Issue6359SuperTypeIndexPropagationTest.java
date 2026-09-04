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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
class Issue6359SuperTypeIndexPropagationTest extends TestHelper {

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

    final int subBucketId = database.getSchema().getType("Sub").getBuckets(false).get(0).getFileId();

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

    final int subBucketId = database.getSchema().getType("Sub").getBuckets(false).get(0).getFileId();

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
    final String contended = database.getSchema().getType("Sub").getBuckets(false).get(0).getName() + "_ext";
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
    assertThat(sub.getExternalBucketIdFor(sub.getBuckets(false).get(0).getFileId()))
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
