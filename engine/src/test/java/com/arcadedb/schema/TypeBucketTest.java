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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.SerializationException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.BinarySerializerTestHelper;
import com.arcadedb.serializer.BinaryTypes;
import com.arcadedb.serializer.JsonGraphSerializer;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for the buckets of a type and the attach of a super type, one nested class per fixed issue.
 */
class TypeBucketTest {
  /**
   * Regression for #5636: getBucketById and getBucketByName answer null instead of raising where a caller would test for null.
   * <p>
   * Regression test for issue #5636 item 1: {@code Schema.getBucketById(int)} and {@code getBucketByName(String)} RAISE
   * in exactly the cases a caller would test for {@code null}, so every {@code if (bucket == null)} written after one of
   * them is dead code. #5608 fixed one such site; this pins the API contract and the three siblings it had.
   * <p>
   * The one that cost something is {@code BinarySerializer.readExternalValue}: its unreachable branch held a
   * deliberately written, user-actionable message about {@code arcadedb.externalPropertyBucketPath}, and the user who
   * hit precisely the scenario the comment describes got a bare "Bucket with id 'N' was not found" instead.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class BucketLookupTest extends TestHelper {

    /** An id far past the end of the files array: the "component not loaded" shape, without corrupting anything. */
    private static final int UNLOADED_BUCKET_ID = 9999;

    @Test
    void getBucketByIdThrowsWhereTheIfExistsFormReturnsNull() {
      final LocalSchema schema = database.getSchema().getEmbedded();

      // Out of range.
      assertThatThrownBy(() -> schema.getBucketById(UNLOADED_BUCKET_ID)).isInstanceOf(SchemaException.class)
          .hasMessageContaining("9999");
      assertThat(schema.getBucketByIdIfExists(UNLOADED_BUCKET_ID)).isNull();

      // Negative id: REBUILD INDEX * reaches this with associatedBucketId=-1 for a type-level index.
      assertThatThrownBy(() -> schema.getBucketById(-1)).isInstanceOf(SchemaException.class);
      assertThat(schema.getBucketByIdIfExists(-1)).isNull();

      // In range but NOT a bucket: the schema's dictionary is file 0, and it is a Component, not a LocalBucket.
      // This is the second arm that made the null checks unreachable, and the easier one to miss.
      assertThat(schema.getFileById(0)).isNotNull();
      assertThat(schema.getFileById(0)).isNotInstanceOf(com.arcadedb.engine.LocalBucket.class);
      assertThatThrownBy(() -> schema.getBucketById(0)).isInstanceOf(SchemaException.class);
      assertThat(schema.getBucketByIdIfExists(0)).isNull();
    }

    @Test
    void getBucketByIdIfExistsStillReturnsARealBucket() {
      database.getSchema().createDocumentType("Doc", 1);
      final int bucketId = database.getSchema().getType("Doc").getBuckets(false).getFirst().getFileId();

      final LocalSchema schema = database.getSchema().getEmbedded();
      assertThat(schema.getBucketByIdIfExists(bucketId)).isNotNull();
      assertThat(schema.getBucketByIdIfExists(bucketId)).isSameAs(schema.getBucketById(bucketId));
    }

    @Test
    void getBucketByNameThrowsWhereTheIfExistsFormReturnsNull() {
      final Schema schema = database.getSchema();

      assertThatThrownBy(() -> schema.getBucketByName("NoSuchBucket")).isInstanceOf(SchemaException.class);
      assertThat(schema.getBucketByNameIfExists("NoSuchBucket")).isNull();

      database.getSchema().createDocumentType("Doc", 1);
      final String bucketName = database.getSchema().getType("Doc").getBuckets(false).getFirst().getName();
      assertThat(schema.getBucketByNameIfExists(bucketName)).isNotNull();
    }

    /**
     * The read path of an EXTERNAL property whose paired bucket is not loaded must deliver the guidance someone wrote
     * for exactly this moment, not the schema's generic "not found".
     */
    @Test
    void readingAnExternalValueFromAnUnloadedBucketExplainsHowToRecoverIt() {
      final DatabaseInternal db = (DatabaseInternal) database;

      assertThatThrownBy(() -> db.getSerializer().readExternalValue(db, UNLOADED_BUCKET_ID, 0, null))
          .isInstanceOf(SerializationException.class)
          .hasMessageContaining("Cannot read EXTERNAL property")
          .hasMessageContaining("external bucket id=" + UNLOADED_BUCKET_ID)
          .hasMessageContaining("arcadedb.externalPropertyBucketPath");
    }

    /**
     * ...and so must the WRITE path. It used to dereference the bucket with no check at all, so it raised the bare
     * {@code SchemaException} while its sibling one method above tried to explain itself.
     */
    @Test
    void writingAnExternalValueToAnUnloadedBucketExplainsHowToRecoverItToo() {
      final DatabaseInternal db = (DatabaseInternal) database;

      database.transaction(() -> assertThatThrownBy(
          () -> BinarySerializerTestHelper.injectOrphanExternalRecord(db.getSerializer(), db, UNLOADED_BUCKET_ID,
              BinaryTypes.TYPE_STRING, "payload", null)).isInstanceOf(SerializationException.class)
          .hasMessageContaining("Cannot write EXTERNAL property")
          .hasMessageContaining("external bucket id=" + UNLOADED_BUCKET_ID)
          .hasMessageContaining("arcadedb.externalPropertyBucketPath"));
    }

    /**
     * TRUNCATE BUCKET's own "not found" message was unreachable, so the outer {@code catch (Exception)} rewrapped the
     * SchemaException and the user read the noun twice: "Bucket not found: Bucket with id '9999' was not found".
     */
    @Test
    void truncateBucketReportsAMissingIdOnce() {
      assertThatThrownBy(() -> database.command("sql", "truncate bucket " + UNLOADED_BUCKET_ID))
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("Bucket with id " + UNLOADED_BUCKET_ID + " not found")
          .matches(e -> !e.getMessage().contains("Bucket not found:"), "the message must not be doubled by the rewrap");
    }

    @Test
    void truncateBucketReportsAMissingNameOnce() {
      assertThatThrownBy(() -> database.command("sql", "truncate bucket NoSuchBucket"))
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("Bucket 'NoSuchBucket' not found")
          .matches(e -> !e.getMessage().contains("Bucket not found:"), "the message must not be doubled by the rewrap");
    }

    /**
     * MATCH's bucket-arity estimator carried its own "not defined" message behind a guard the throwing lookup made
     * unreachable - and it paid for two lookups to do it.
     */
    @Test
    void matchOnAnUnknownBucketReportsTheMatchSpecificMessage() {
      database.getSchema().createVertexType("V1");

      assertThatThrownBy(() -> database.query("sql", "match {bucket: NoSuchBucket, as: v} return v").stream().toList())
          .isInstanceOf(SchemaException.class)
          // "not defined" and not just the bucket name: the generic SchemaException this replaces also quotes the
          // name, so asserting on the name alone would not notice a regression back to it.
          .hasMessageContaining("Bucket 'NoSuchBucket' not defined");
    }

    /**
     * The fifth instance of the pattern, and the one easiest to miss: {@code handleInsertSelect} has no
     * {@code else bucket = null} arm - unlike {@code handleInsertInto} in the same class, whose guard IS reachable -
     * so both lookups raised before its own message could run.
     */
    @Test
    void insertIntoAnUnknownBucketFromASelectReportsTheTargetMessage() {
      database.getSchema().createDocumentType("Doc");

      assertThatThrownBy(
          () -> database.command("sql", "insert into bucket:NoSuchBucket from select from Doc").close())
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("Target bucket 'NoSuchBucket' not found");

      assertThatThrownBy(
          () -> database.command("sql", "insert into bucket:" + UNLOADED_BUCKET_ID + " from select from Doc").close())
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("Target bucket with id " + UNLOADED_BUCKET_ID + " not found");
    }

    /**
     * A target with neither a name nor a number is reachable through a bound parameter, and used to unbox {@code null}
     * from {@code getBucketNumber()}. It now says so instead of throwing NPE.
     */
    @Test
    void insertFromASelectIntoAnUnnamedTargetSaysSoInsteadOfThrowingNpe() {
      database.getSchema().createDocumentType("Doc", 1);

      assertThatThrownBy(() -> database.command("sql", "insert into bucket:? from select from Doc",
          Map.of("0", "NoSuchBucket")).close())
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("Target bucket 'NoSuchBucket' not found");
    }

    /**
     * The same drift hid a functional bug, not just a message one: {@code handleInsertSelect} read
     * {@code getBucketName()} directly rather than calling {@code resolveBucketName()} like its sibling, so a
     * PARAMETERIZED target never resolved its parameter and the statement failed even for a bucket that exists.
     */
    @Test
    void insertFromASelectResolvesAParameterizedBucketName() {
      database.getSchema().createDocumentType("Doc", 1);
      database.getSchema().createDocumentType("Copy", 1);
      final String target = database.getSchema().getType("Copy").getBuckets(false).getFirst().getName();

      database.transaction(() -> {
        database.newDocument("Doc").set("k", 1).save();
        database.command("sql", "insert into bucket:? from select from Doc", Map.of("0", target)).close();
      });

      assertThat(database.countType("Copy", false)).isEqualTo(1);
    }

    /**
     * ...and the INSERT ... SELECT form must still work against a real bucket, which is the path both lookups above
     * normally take.
     */
    @Test
    void insertIntoAKnownBucketFromASelectStillWorks() {
      database.getSchema().createDocumentType("Doc", 1);
      database.getSchema().createDocumentType("Copy", 1);
      final String target = database.getSchema().getType("Copy").getBuckets(false).getFirst().getName();

      database.transaction(() -> {
        database.newDocument("Doc").set("k", 1).save();
        database.command("sql", "insert into bucket:" + target + " from select from Doc").close();
      });

      assertThat(database.countType("Copy", false)).isEqualTo(1);
    }

    /**
     * The sibling guard in {@code handleCreateRecord} looked reachable - it has an {@code else bucket = null} arm - but
     * that arm only covers "no bucket named at all"; an unknown name or id still raised out of the throwing lookup
     * first. So the branch was dead for exactly the input its message describes, and once made live the message itself
     * had to change: "Target not specified" is untrue when the target was specified and simply does not exist.
     */
    @Test
    void insertIntoAnUnknownBucketNamesTheBucketItCouldNotFind() {
      database.getSchema().createDocumentType("Doc");

      assertThatThrownBy(() -> database.command("sql", "insert into bucket:NoSuchBucket set k = 1").close())
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("Target bucket 'NoSuchBucket' not found");

      assertThatThrownBy(() -> database.command("sql", "insert into bucket:" + UNLOADED_BUCKET_ID + " set k = 1").close())
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("Target bucket with id " + UNLOADED_BUCKET_ID + " not found");
    }

    /**
     * Targeting a bucket by id and by name are the same user mistake, and used to get two different error contracts:
     * {@code SELECT FROM bucket:NoSuchBucket} reached the planner's own "does not exist", while
     * {@code SELECT FROM bucket:9999} escaped as a raw {@link SchemaException} from whichever of the four id lookups
     * on that path ran first (two planner probes, the target calculation, then the fetch step).
     */
    @Test
    void selectFromAnUnknownBucketReportsTheSameWayByIdAndByName() {
      database.getSchema().createDocumentType("Doc", 1);

      assertThatThrownBy(() -> database.query("sql", "select from bucket:" + UNLOADED_BUCKET_ID).stream().toList())
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("Bucket with id " + UNLOADED_BUCKET_ID + " does not exist");

      assertThatThrownBy(() -> database.query("sql", "select from bucket:NoSuchBucket").stream().toList())
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("does not exist");
    }

    /**
     * ...and a real bucket must still be selectable by id, which is the path all four of those lookups normally take.
     */
    @Test
    void selectFromAKnownBucketByIdStillWorks() {
      database.getSchema().createDocumentType("Doc", 1);
      final int bucketId = database.getSchema().getType("Doc").getBuckets(false).getFirst().getFileId();

      database.transaction(() -> database.newDocument("Doc").set("k", 1).save());

      assertThat(database.query("sql", "select from bucket:" + bucketId).stream().toList()).hasSize(1);
    }

    /**
     * A real bucket must still insert: the null-tolerant lookups must not have broken the happy path.
     */
    @Test
    void insertIntoAKnownBucketStillWorks() {
      database.getSchema().createDocumentType("Doc", 1);
      final String bucketName = database.getSchema().getType("Doc").getBuckets(false).getFirst().getName();

      database.transaction(() -> database.command("sql", "insert into bucket:" + bucketName + " set k = 1").close());

      assertThat(database.countType("Doc", false)).isEqualTo(1);
    }
  }

  /**
   * Regression for #11335: {@code ALTER TYPE ... BUCKET -x} leaves the bucket's records in place with no type, and every path that met one read
   * the type without a check: {@code UPDATE BUCKET:x SET ...} ended on a {@link NullPointerException}, and so did the other update
   * forms, a bare {@code save()}, {@code toJSON()} of a plain {@code SELECT}, and every delete, which left the records impossible to
   * clean up.
   * <p>
   * The contract: such a record can be read and deleted, and writing one is refused saying its bucket belongs to no type.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class BucketOfNoTypeTest extends TestHelper {
    private RID document;
    private RID vertex;

    @BeforeEach
    void detachBucketsHoldingRecords() {
      database.command("sql", "CREATE DOCUMENT TYPE A");
      database.command("sql", "CREATE PROPERTY A.id INTEGER");
      database.command("sql", "CREATE INDEX ON A (id) UNIQUE");
      database.command("sql", "CREATE VERTEX TYPE V1");
      database.command("sql", "CREATE BUCKET x");
      database.command("sql", "CREATE BUCKET y");
      database.command("sql", "ALTER TYPE A BUCKET +x");
      database.command("sql", "ALTER TYPE V1 BUCKET +y");

      database.transaction(() -> {
        document = insert("INSERT INTO BUCKET:x CONTENT { \"id\": 1, \"on\": \"2026-11-06\" }");
        vertex = insert("INSERT INTO BUCKET:y CONTENT { \"id\": 2 }");
      });

      database.command("sql", "ALTER TYPE A BUCKET -x");
      database.command("sql", "ALTER TYPE V1 BUCKET -y");
    }

    private RID insert(final String sql) {
      try (final ResultSet rs = database.command("sql", sql)) {
        return rs.next().getIdentity().orElseThrow();
      }
    }

    @ParameterizedTest
    @ValueSource(strings = { "UPDATE BUCKET:x SET v = 1", "UPDATE BUCKET:x CONTENT { \"v\": 1 }", "UPDATE BUCKET:x MERGE { \"v\": 1 }",
        "UPDATE BUCKET:x REMOVE id", "UPDATE BUCKET:y SET v = 1" })
    void everySqlUpdateIsRefusedSayingTheBucketBelongsToNoType(final String update) {
      assertThatThrownBy(() -> database.transaction(() -> database.command("sql", update).close()))//
          .hasMessageContaining("belongs to no type");
      assertThat(document.asDocument().get("v")).isNull();
    }

    @Test
    void updateOfTheRecordByRidIsRefused() {
      assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "UPDATE " + document + " SET v = 1").close()))//
          .hasMessageContaining("belongs to no type");
    }

    @Test
    void modifyIsRefusedForADocumentAndAVertex() {
      database.transaction(() -> {
        assertThatThrownBy(() -> document.asDocument().modify())//
            .isInstanceOf(SchemaException.class).hasMessageContaining("bucket 'x' belongs to no type");
        assertThatThrownBy(() -> vertex.asVertex().modify())//
            .isInstanceOf(SchemaException.class).hasMessageContaining("bucket 'y' belongs to no type");
      });
    }

    @Test
    void aRecordOfTheBucketReadsWithoutAType() {
      final Document read = document.asDocument();
      assertThat(read.getType()).isNull();
      assertThat(read.getTypeName()).isNull();
      assertThat(read.getInteger("id")).isEqualTo(1);
      assertThat(read.getLocalDate("on")).isEqualTo(LocalDate.of(2026, 11, 6));

      final JSONObject json = read.toJSON(true);
      assertThat(json.getInt("id")).isEqualTo(1);
      assertThat(json.has(Property.TYPE_PROPERTY)).isFalse();
      assertThat(read.toMap(true)).containsEntry("id", 1).doesNotContainKey(Property.TYPE_PROPERTY);
      assertThat(vertex.asVertex().toJSON(true).has(Property.TYPE_PROPERTY)).isFalse();
    }

    @Test
    void aSelectOfTheBucketSerializesItsRecords() {
      try (final ResultSet rs = database.query("sql", "SELECT FROM BUCKET:x")) {
        final Result row = rs.next();
        assertThat(row.toJSON().getInt("id")).isEqualTo(1);
        assertThat(row.toMap()).containsEntry("id", 1);

        final JsonSerializer serializer = new JsonSerializer(database);
        assertThat(serializer.serializeResult(database, row).has(Property.TYPE_PROPERTY)).isFalse();
        assertThat(serializer.serializeDocument(row.toElement()).has(Property.TYPE_PROPERTY)).isFalse();
        assertThat(JsonGraphSerializer.createJsonGraphSerializer().serializeGraphElement(row.toElement()).has("t")).isFalse();
      }
    }

    @Test
    void aScanOfTheBucketReadsItsRecords() {
      final List<Record> scanned = new ArrayList<>();
      database.scanBucket("x", scanned::add);
      assertThat(scanned).hasSize(1);
      assertThat(((Document) scanned.getFirst()).getInteger("id")).isEqualTo(1);
    }

    @Test
    void deletingTheRecordsEmptiesTheBucket() {
      database.transaction(() -> database.command("sql", "DELETE FROM BUCKET:x").close());
      assertThat(database.countBucket("x")).isZero();

      database.transaction(() -> vertex.asVertex().delete());
      assertThat(database.countBucket("y")).isZero();
    }

    @Test
    void deletingARecordByRidAndThroughTheApiWorks() {
      final RID[] more = new RID[2];
      database.command("sql", "ALTER TYPE A BUCKET +x");
      database.transaction(() -> {
        more[0] = insert("INSERT INTO BUCKET:x CONTENT { \"id\": 10 }");
        more[1] = insert("INSERT INTO BUCKET:x CONTENT { \"id\": 11 }");
      });
      database.command("sql", "ALTER TYPE A BUCKET -x");

      database.transaction(() -> database.command("sql", "DELETE FROM " + more[0]).close());
      database.transaction(() -> more[1].asDocument().delete());
      assertThat(database.countBucket("x")).isEqualTo(1);
    }

    @Test
    void deletingARecordOfTheBucketLeavesTheIndexOfItsFormerTypeAlone() {
      // THE SUB-INDEX OF THE BUCKET LEFT WITH IT, SO THE KEY OF THE DETACHED RECORD IS FREE AND A NEW RECORD OF THE TYPE CAN TAKE IT.
      // DELETING THE DETACHED RECORD MUST NOT TAKE THAT ENTRY WITH IT
      final RID[] live = new RID[1];
      database.transaction(() -> live[0] = database.newDocument("A").set("id", 1).save().getIdentity());

      database.transaction(() -> document.asDocument().delete());

      final IndexCursor found = database.lookupByKey("A", "id", 1);
      assertThat(found.hasNext()).isTrue();
      assertThat(found.next().getIdentity()).isEqualTo(live[0]);
      assertThat(found.hasNext()).isFalse();
    }

    @Test
    void aVertexOfTheBucketDeletesWithItsEdges() {
      database.command("sql", "CREATE EDGE TYPE E1");
      database.command("sql", "CREATE VERTEX TYPE V2");
      database.command("sql", "ALTER TYPE V1 BUCKET +y");
      final RID[] other = new RID[1];
      database.transaction(() -> {
        final MutableVertex to = database.newVertex("V2").set("id", 3).save();
        other[0] = to.getIdentity();
        vertex.asVertex().modify().newEdge("E1", to);
      });
      database.command("sql", "ALTER TYPE V1 BUCKET -y");

      database.transaction(() -> vertex.asVertex().delete());

      assertThat(database.countBucket("y")).isZero();
      assertThat(database.countType("E1", true)).isZero();
      assertThat(other[0].asVertex().countEdges(Vertex.DIRECTION.IN, "E1")).isZero();
    }

    @Test
    void truncatingTheBucketEmptiesIt() {
      database.command("sql", "TRUNCATE BUCKET x");
      assertThat(database.countBucket("x")).isZero();
    }

    @Test
    void theRecordIsWritableAgainOnceTheBucketIsAttached() {
      database.command("sql", "ALTER TYPE A BUCKET +x");

      database.transaction(() -> database.command("sql", "UPDATE BUCKET:x SET v = 1").close());

      final Document read = document.asDocument();
      assertThat(read.getTypeName()).isEqualTo("A");
      assertThat(read.getInteger("v")).isEqualTo(1);
    }

    @Test
    void aRecordSavedIntoABucketOfNoTypeReadsAndDeletesLikeTheOthers() {
      // THE OTHER WAY TO MAKE ONE: SAVING INTO A FREE BUCKET IS ALLOWED, AND READ BACK THE RECORD HAS NO TYPE
      final RID[] saved = new RID[1];
      database.transaction(() -> {
        final MutableDocument created = database.newDocument("A").set("id", 99);
        created.save("x");
        saved[0] = created.getIdentity();
      });

      assertThat(database.query("sql", "SELECT FROM " + saved[0]).next().toJSON().getInt("id")).isEqualTo(99);

      database.transaction(() -> saved[0].asDocument().delete());
      assertThat(database.countBucket("x")).isEqualTo(1);
    }
  }

  /**
   * Regression for #11302: {@code ALTER TYPE A SUPERTYPE +S} over a type whose records hold a key the
   * super type's UNIQUE index already holds was accepted, and left the sub-index propagated over A's bucket with NO
   * entry at all (not only the duplicate: every record of the bucket). Index-backed lookups then missed records a scan
   * finds, {@code CHECK DATABASE} reported nothing, and the next open found a sub-index file with no header page and
   * declared the super type's index incomplete.
   * <p>
   * The attach must be refused with the {@link DuplicatedKeyException} the linkage guard of
   * {@code LocalDocumentType.addSuperTypeInternal} is written for, and leave the schema as it found it: A not a subtype,
   * no sub-index of A's bucket under the super type's index, before and after a reopen.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class SuperTypeAttachOverDuplicateTest extends TestHelper {

    @Test
    void attachOverAKeyTheSuperTypeAlreadyHoldsIsRefusedAndUndone() {
      createSuperTypeHolding("x");
      createSubTypeCandidateHolding("x", "solo");

      assertThatThrownBy(() -> database.command("sql", "ALTER TYPE A SUPERTYPE +S"))//
          .isInstanceOf(DuplicatedKeyException.class)//
          .hasMessageContaining("x");

      assertRefusedAndUndone();
      reopenDatabase();
      assertRefusedAndUndone();
    }

    @Test
    void attachOverAKeyDuplicatedInsideTheSubTypeIsRefusedAndUndone() {
      // NO RECORD IN S: THE DUPLICATE IS BETWEEN TWO RECORDS OF A, WHICH HAS NO UNIQUE INDEX OF ITS OWN
      createSuperTypeHolding();
      createSubTypeCandidateHolding("x", "x", "solo");

      assertThatThrownBy(() -> database.getSchema().getType("A").addSuperType("S"))//
          .isInstanceOf(DuplicatedKeyException.class)//
          .hasMessageContaining("x");

      assertThat(database.getSchema().getType("A").isSubTypeOf("S")).isFalse();
      assertThat(subIndexNamesOfA()).isEmpty();
      assertThat(database.getSchema().getType("S").getPolymorphicIndexByProperties("k").isValid()).isTrue();

      reopenDatabase();
      assertThat(database.getSchema().getType("A").isSubTypeOf("S")).isFalse();
      assertThat(subIndexNamesOfA()).isEmpty();
    }

    @Test
    void attachInsideACallerTransactionRefusesOnlyThePendingDuplicate() {
      // THE BUILD SHARES THE CALLER'S TRANSACTION WHEN IT HOLDS WRITES IN A'S BUCKETS: THE COMMITTED RECORDS ARE INDEXED IN
      // A TRANSACTION OF THEIR OWN, THE CALLER'S PENDING ONES QUEUE THEIR ENTRIES ON THE CALLER'S TRANSACTION AND ARE CHECKED
      // WHEN IT COMMITS, EXACTLY AS CREATE INDEX IN A TRANSACTION DOES. THE ATTACH IS A SCHEMA CHANGE AND STANDS; THE
      // DUPLICATE IS REFUSED WITH THE RECORD THAT CARRIES IT, AND THE INDEX COVERS EVERY RECORD THAT DID COMMIT
      createSuperTypeHolding("x");
      createSubTypeCandidateHolding("solo");

      assertThatThrownBy(() -> database.transaction(() -> {
        database.newDocument("A").set("k", "x").save();
        database.getSchema().getType("A").addSuperType("S");
      }))//
          .isInstanceOf(DuplicatedKeyException.class)//
          .hasMessageContaining("x");

      assertIndexCoversTheCommittedRecords();
      reopenDatabase();
      assertIndexCoversTheCommittedRecords();
    }

    private void assertIndexCoversTheCommittedRecords() {
      assertThat(database.getSchema().getType("A").isSubTypeOf("S")).isTrue();
      assertThat(subIndexNamesOfA()).hasSize(1);
      final TypeIndex index = database.getSchema().getType("S").getPolymorphicIndexByProperties("k");
      assertThat(index.isValid()).isTrue();
      assertThat(index.isComplete()).isTrue();
      assertThat(index.countEntries()).isEqualTo(2L);
      assertThat(count("SELECT count(*) AS c FROM A")).isEqualTo(1L);
      assertThat(count("SELECT count(*) AS c FROM S WHERE k = 'solo'")).isEqualTo(1L);
      assertThat(count("SELECT count(*) AS c FROM S WHERE k = 'x'")).isEqualTo(1L);
      assertCheckDatabaseClean();
    }

    @Test
    void attachWithoutADuplicateIndexesEveryRecordOfTheSubType() {
      // CONTROL: THE SAME SHAPE WITHOUT THE DUPLICATE IS ACCEPTED AND EVERY KEY IS FOUND THROUGH THE INDEX
      createSuperTypeHolding("x");
      createSubTypeCandidateHolding("a1", "solo");

      database.command("sql", "ALTER TYPE A SUPERTYPE +S");

      assertAttachedAndIndexed();
      reopenDatabase();
      assertAttachedAndIndexed();
    }

    @Test
    void createUniqueIndexOverADuplicateLeavesNoComponentBehind() {
      final DocumentType t = database.getSchema().createDocumentType("T", 1);
      t.createProperty("k", Type.STRING);
      database.transaction(() -> {
        database.newDocument("T").set("k", "x").save();
        database.newDocument("T").set("k", "x").save();
      });

      assertThatThrownBy(() -> t.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "k")).hasStackTraceContaining("DuplicatedKeyException");

      assertThat(Arrays.stream(database.getSchema().getIndexes()).map(Index::getName).toList()).isEmpty();
      reopenDatabase();
      assertThat(Arrays.stream(database.getSchema().getIndexes()).map(Index::getName).toList()).isEmpty();
    }

    @Test
    void createUniqueIndexOverADuplicateAcrossBucketsLeavesNoComponentBehind() {
      // THE DUPLICATE IS FOUND WHEN THE SECOND BUCKET'S BUILD COMMITS, WHICH IS WHERE THE PROPAGATION LOST ITS BUILD
      final DocumentType t = database.getSchema().createDocumentType("T", 2);
      t.createProperty("k", Type.STRING);
      database.transaction(() -> {
        database.newDocument("T").set("k", "x").save();
        database.newDocument("T").set("k", "x").save();
      });
      assertThat(t.getBuckets(false).stream().filter(b -> b.count() > 0).count()).isEqualTo(2L);

      assertThatThrownBy(() -> t.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "k")).hasStackTraceContaining("DuplicatedKeyException");

      assertThat(Arrays.stream(database.getSchema().getIndexes()).map(Index::getName).toList()).isEmpty();
      reopenDatabase();
      assertThat(Arrays.stream(database.getSchema().getIndexes()).map(Index::getName).toList()).isEmpty();
    }

    @Test
    void attachOfAPopulatedBucketOverADuplicateIsRefusedAndUndone() {
      // THE SIBLING PROPAGATION, addBucket, OVER A BUCKET THAT ALREADY HOLDS RECORDS: ONE A TYPE GAVE BACK. ITS RETRY OF THE
      // DUPLICATE CREATED A SECOND COMPONENT BESIDE THE FIRST ATTEMPT'S, AND THE BUCKET STAYED ATTACHED WITH BOTH
      createSuperTypeHolding("x");
      final DocumentType a = database.getSchema().createDocumentType("A", 1);
      final String bucketName = a.getBuckets(false).getFirst().getName();
      database.transaction(() -> database.newDocument("A").set("k", "x").save());
      a.removeBucket(database.getSchema().getBucketByName(bucketName));

      assertThatThrownBy(() -> database.command("sql", "ALTER TYPE S BUCKET +" + bucketName))//
          .isInstanceOf(DuplicatedKeyException.class)//
          .hasMessageContaining("x");

      assertBucketNotAttached(bucketName);
      reopenDatabase();
      assertBucketNotAttached(bucketName);

      // THE BUCKET IS FREE AGAIN: ONCE THE DUPLICATE IS GONE THE SAME ATTACH IS ACCEPTED AND INDEXED
      database.command("sql", "ALTER TYPE A BUCKET +" + bucketName);
      database.transaction(() -> database.command("sql", "UPDATE A SET k = 'y'"));
      database.command("sql", "ALTER TYPE A BUCKET -" + bucketName);
      database.command("sql", "ALTER TYPE S BUCKET +" + bucketName);
      assertThat(indexNames().stream().filter(n -> n.startsWith(bucketName + "_")).toList()).hasSize(1);
      assertThat(count("SELECT count(*) AS c FROM S WHERE k = 'y'")).isEqualTo(1L);
      assertCheckDatabaseClean();
    }

    private void assertBucketNotAttached(final String bucketName) {
      assertThat(database.getSchema().getType("S").getBuckets(false).stream().map(b -> b.getName()).toList()).doesNotContain(bucketName);
      assertThat(indexNames().stream().filter(n -> n.startsWith(bucketName + "_")).toList()).isEmpty();
      final TypeIndex index = database.getSchema().getType("S").getPolymorphicIndexByProperties("k");
      assertThat(index.isValid()).isTrue();
      assertThat(index.countEntries()).isEqualTo(1L);
      assertThat(database.getSchema().getType("S").isNeedsRepartition()).isFalse();
    }

    private List<String> indexNames() {
      return Arrays.stream(database.getSchema().getIndexes()).map(Index::getName).toList();
    }

    private void assertAttachedAndIndexed() {
      assertThat(database.getSchema().getType("A").isSubTypeOf("S")).isTrue();
      assertThat(subIndexNamesOfA()).hasSize(1);
      final TypeIndex index = database.getSchema().getType("S").getPolymorphicIndexByProperties("k");
      assertThat(index.isValid()).isTrue();
      assertThat(index.isComplete()).isTrue();
      assertThat(index.countEntries()).isEqualTo(3L);
      for (final String key : List.of("x", "a1", "solo"))
        assertThat(count("SELECT count(*) AS c FROM S WHERE k = '" + key + "'")).as("key " + key).isEqualTo(1L);
      assertCheckDatabaseClean();
    }

    private void assertRefusedAndUndone() {
      assertThat(database.getSchema().getType("A").isSubTypeOf("S")).isFalse();
      assertThat(database.getSchema().getType("S").getSubTypes()).isEmpty();
      assertThat(subIndexNamesOfA()).as("no sub-index of A may be left under S[k]").isEmpty();

      final TypeIndex index = database.getSchema().getType("S").getPolymorphicIndexByProperties("k");
      assertThat(index.isValid()).isTrue();
      assertThat(index.isComplete()).isTrue();
      assertThat(index.countEntries()).isEqualTo(1L);

      assertThat(count("SELECT count(*) AS c FROM S")).isEqualTo(1L);
      assertThat(count("SELECT count(*) AS c FROM S WHERE k = 'x'")).isEqualTo(1L);
      assertThat(count("SELECT count(*) AS c FROM A")).isEqualTo(2L);
      assertCheckDatabaseClean();
    }

    private void createSuperTypeHolding(final String... keys) {
      final DocumentType s = database.getSchema().createDocumentType("S");
      s.createProperty("k", Type.STRING);
      s.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "k");
      database.transaction(() -> {
        for (final String key : keys)
          database.newDocument("S").set("k", key).save();
      });
    }

    private void createSubTypeCandidateHolding(final String... keys) {
      database.getSchema().createDocumentType("A", 1);
      database.transaction(() -> {
        for (final String key : keys)
          database.newDocument("A").set("k", key).save();
      });
    }

    private List<String> subIndexNamesOfA() {
      return Arrays.stream(database.getSchema().getIndexes()).map(Index::getName).filter(n -> n.startsWith("A_")).toList();
    }

    private long count(final String sql) {
      try (final ResultSet rs = database.query("sql", sql)) {
        return rs.next().<Number>getProperty("c").longValue();
      }
    }

    private void assertCheckDatabaseClean() {
      try (final ResultSet rs = database.command("sql", "CHECK DATABASE")) {
        final Result r = rs.next();
        assertThat(r.<Number>getProperty("totalErrors").longValue()).isZero();
        assertThat(r.<Collection<?>>getProperty("corruptedIndexes")).isNullOrEmpty();
        assertThat(r.<Collection<?>>getProperty("incompleteIndexes")).isNullOrEmpty();
      }
    }
  }
}
