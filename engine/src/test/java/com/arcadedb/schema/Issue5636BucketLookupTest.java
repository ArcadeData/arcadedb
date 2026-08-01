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
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.SerializationException;
import com.arcadedb.serializer.BinarySerializerTestHelper;
import com.arcadedb.serializer.BinaryTypes;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
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
class Issue5636BucketLookupTest extends TestHelper {

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
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("Bucket with id " + UNLOADED_BUCKET_ID + " not found")
        .matches(e -> !e.getMessage().contains("Bucket not found:"), "the message must not be doubled by the rewrap");
  }

  @Test
  void truncateBucketReportsAMissingNameOnce() {
    assertThatThrownBy(() -> database.command("sql", "truncate bucket NoSuchBucket"))
        .isInstanceOf(CommandExecutionException.class)
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
        .isInstanceOf(CommandExecutionException.class)
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
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("Target bucket 'NoSuchBucket' not found");

    assertThatThrownBy(
        () -> database.command("sql", "insert into bucket:" + UNLOADED_BUCKET_ID + " from select from Doc").close())
        .isInstanceOf(CommandSQLParsingException.class)
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
        .isInstanceOf(CommandSQLParsingException.class)
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
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("Target bucket 'NoSuchBucket' not found");

    assertThatThrownBy(() -> database.command("sql", "insert into bucket:" + UNLOADED_BUCKET_ID + " set k = 1").close())
        .isInstanceOf(CommandSQLParsingException.class)
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
        .isInstanceOf(CommandExecutionException.class)
        .hasMessageContaining("Bucket with id " + UNLOADED_BUCKET_ID + " does not exist");

    assertThatThrownBy(() -> database.query("sql", "select from bucket:NoSuchBucket").stream().toList())
        .isInstanceOf(CommandExecutionException.class)
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
