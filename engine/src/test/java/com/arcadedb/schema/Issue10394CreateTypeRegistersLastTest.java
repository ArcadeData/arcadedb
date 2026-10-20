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
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.TypeIndex;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10394: creating a type registered it in the schema and gave it its buckets BEFORE its super types were linked, and the link
 * can still refuse (a TIMESERIES type on either end, a paired external-property bucket another type owns, a partition the subtype
 * cannot take, an index that cannot be propagated). The refusal then left the type behind, registered, with its buckets and with
 * the super types that had been linked before the refused one, also after a reopen. #9846 and #10317 each added a pre-check in
 * front of the registration for the refusals they knew; the rule is that a refused create leaves the schema as it found it:
 * <ul>
 *   <li>every refusal {@code addSuperType} can give before it changes anything is asked before anything is created;</li>
 *   <li>a refusal that can only come out of the linkage itself takes the type back, with the buckets it created and the super
 *       types it linked, and leaves alone the buckets it was given or found.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10394CreateTypeRegistersLastTest extends TestHelper {

  @Test
  void typeBuilderUnderATimeSeriesTypeLeavesNothingBehind() {
    createTimeSeriesType("Metrics");

    assertThatThrownBy(() -> database.getSchema().buildDocumentType().withName("Child").withSuperType("Metrics").create())
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("TIMESERIES");

    assertNothingLeftOf("Child");
    assertThat(database.getSchema().getType("Metrics").getSubTypes()).isEmpty();

    reopenDatabase();

    assertNothingLeftOf("Child");
    assertThat(database.getSchema().getType("Metrics").getSubTypes()).isEmpty();
  }

  @Test
  void typeBuilderOfEveryKindUnderATimeSeriesTypeLeavesNothingBehind() {
    createTimeSeriesType("Metrics");

    assertThatThrownBy(() -> database.getSchema().buildVertexType().withName("V").withSuperType("Metrics").create())
        .isInstanceOf(SchemaException.class);
    assertThatThrownBy(() -> database.getSchema().buildEdgeType().withName("E").withSuperType("Metrics").create())
        .isInstanceOf(SchemaException.class);

    assertNothingLeftOf("V");
    assertNothingLeftOf("E");
    // A vertex type owns two more buckets for its edge lists: they are not left either
    assertThat(database.getSchema().getBuckets()).extracting(Bucket::getName).noneMatch(name -> name.startsWith("V_"));
  }

  @Test
  void aRefusedSecondSuperTypeLeavesTheFirstOneUnlinked() {
    database.getSchema().createDocumentType("Plain");
    createTimeSeriesType("Metrics");

    assertThatThrownBy(
        () -> database.getSchema().buildDocumentType().withName("Child").withSuperType("Plain").withSuperType("Metrics").create())
        .isInstanceOf(SchemaException.class);

    assertNothingLeftOf("Child");
    // The super type that was linked before the refused one does not keep a sub type that does not exist
    assertThat(database.getSchema().getType("Plain").getSubTypes()).isEmpty();

    reopenDatabase();

    assertNothingLeftOf("Child");
    assertThat(database.getSchema().getType("Plain").getSubTypes()).isEmpty();
  }

  @Test
  void sqlCreateTypeUnderATimeSeriesTypeLeavesNothingBehind() {
    createTimeSeriesType("Metrics");
    database.getSchema().createDocumentType("Plain");

    for (final String kind : new String[] { "DOCUMENT", "VERTEX", "EDGE" }) {
      assertThatThrownBy(() -> database.command("sql", "CREATE " + kind + " TYPE Child EXTENDS Plain, Metrics"))
          .as(kind).isInstanceOf(SchemaException.class).hasMessageContaining("TIMESERIES");

      assertNothingLeftOf("Child");
      assertThat(database.getSchema().getType("Plain").getSubTypes()).as(kind).isEmpty();
    }
  }

  /**
   * A refusal only the linkage can know: the super type has an EXTERNAL property, so the new type needs a paired
   * {@code <bucket>_ext} bucket, and a bucket of that name is already the primary bucket of another type. Nothing before the
   * linkage looks at it, so it is the take-back that has to leave the schema as it was.
   */
  @Test
  void aRefusalOnlyTheLinkageCanKnowTakesTheTypeBackWithItsBuckets() {
    createExternalSuperType();
    squat("X_0_ext");

    assertThatThrownBy(() -> database.getSchema().buildDocumentType().withName("X").withSuperType("Holder").create())
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("X_0_ext");

    assertNothingLeftOf("X", "X_0_ext");
    assertThat(database.getSchema().getType("Holder").getSubTypes()).isEmpty();
    assertSquatterIntact("X_0_ext");

    reopenDatabase();

    assertNothingLeftOf("X", "X_0_ext");
    assertThat(database.getSchema().getType("Holder").getSubTypes()).isEmpty();
    assertSquatterIntact("X_0_ext");
  }

  @Test
  void theTakeBackKeepsTheBucketsTheTypeWasGivenOrFound() {
    createExternalSuperType();
    squat("Y_0_ext");
    squat("Free_ext");
    // Y_0 is the name CREATE TYPE Y would give its first bucket and it exists already: the type reuses it, so it is not the type's
    // to drop. Free is a bucket the caller hands over. Neither belongs to a type before the create
    final Bucket found = database.getSchema().createBucket("Y_0");
    final Bucket given = database.getSchema().createBucket("Free");

    assertThatThrownBy(() -> database.getSchema().buildDocumentType().withName("Y").withSuperType("Holder").create())
        .isInstanceOf(SchemaException.class);
    assertNothingLeftOf("Y", "Y_0", "Y_0_ext");
    assertThat(database.getSchema().existsBucket("Y_0")).isTrue();
    assertThat(database.getSchema().getBucketByName("Y_0").getFileId()).isEqualTo(found.getFileId());

    assertThatThrownBy(() -> database.getSchema().buildDocumentType().withName("Z").withBuckets(List.of(given))
        .withSuperType("Holder").create())
        .isInstanceOf(SchemaException.class);
    assertNothingLeftOf("Z");
    assertThat(database.getSchema().existsBucket("Free")).isTrue();
    assertThat(database.getSchema().getBucketByName("Free").getFileId()).isEqualTo(given.getFileId());

    reopenDatabase();

    assertNothingLeftOf("Y", "Y_0", "Y_0_ext");
    assertNothingLeftOf("Z");
    assertThat(database.getSchema().existsBucket("Y_0")).isTrue();
    assertThat(database.getSchema().existsBucket("Free")).isTrue();
  }

  /**
   * The link of the first super type indexed the records of the found bucket (an index file over data that is not the type's to
   * lose), and the second one is refused. The take-back drops that index file with the type, never the bucket nor its records.
   */
  @Test
  void theTakeBackKeepsTheRecordsOfAFoundBucketTheLinkIndexed() {
    database.command("sql", "CREATE DOCUMENT TYPE Indexed");
    database.command("sql", "CREATE PROPERTY Indexed.p STRING");
    database.command("sql", "CREATE INDEX ON Indexed (p) NOTUNIQUE");
    createExternalSuperType();
    squat("W_0_ext");
    database.getSchema().createBucket("W_0");
    database.transaction(() -> ((DatabaseInternal) database).createRecord(database.newDocument("Indexed").set("p", "kept"), "W_0"));
    final int indexFilesBefore = subIndexCountOf("Indexed");

    assertThatThrownBy(() -> database.getSchema().buildDocumentType().withName("W").withSuperType("Indexed").withSuperType("Holder")
        .create())
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("W_0_ext");

    assertNothingLeftOf("W", "W_0", "W_0_ext");
    assertThat(database.getSchema().existsBucket("W_0")).isTrue();
    assertThat(database.countBucket("W_0")).isEqualTo(1L);
    assertThat(subIndexCountOf("Indexed")).isEqualTo(indexFilesBefore);

    reopenDatabase();

    assertThat(database.getSchema().existsBucket("W_0")).isTrue();
    assertThat(database.countBucket("W_0")).isEqualTo(1L);
    assertThat(subIndexCountOf("Indexed")).isEqualTo(indexFilesBefore);
  }

  /**
   * The statement links its super types after it creates the type, and takes the type back when one refuses. The buckets it was
   * given (BUCKET Free) and the ones it found under its own names (C_0) were there before the statement and hold records: they
   * stay. A bucket the statement made for a name that did not exist (BUCKET Fresh) was made for the type and goes with it.
   */
  @Test
  void aRefusedStatementKeepsTheBucketsThatExistedBeforeIt() {
    createExternalSuperType();
    squat("Free_ext");
    squat("C_0_ext");
    squat("Fresh_ext");
    database.getSchema().createDocumentType("Plain");
    database.getSchema().createBucket("Free");
    database.getSchema().createBucket("C_0");
    database.transaction(() -> {
      ((DatabaseInternal) database).createRecord(database.newDocument("Plain").set("p", "free"), "Free");
      ((DatabaseInternal) database).createRecord(database.newDocument("Plain").set("p", "found"), "C_0");
    });

    assertThatThrownBy(() -> database.command("sql", "CREATE DOCUMENT TYPE B EXTENDS Holder BUCKET Free"))
        .isInstanceOf(SchemaException.class).hasMessageContaining("Free_ext");
    assertThatThrownBy(() -> database.command("sql", "CREATE DOCUMENT TYPE C EXTENDS Holder"))
        .isInstanceOf(SchemaException.class).hasMessageContaining("C_0_ext");
    assertThatThrownBy(() -> database.command("sql", "CREATE DOCUMENT TYPE D EXTENDS Holder BUCKET Fresh"))
        .isInstanceOf(SchemaException.class).hasMessageContaining("Fresh_ext");

    assertThat(database.getSchema().existsType("B")).isFalse();
    assertThat(database.getSchema().existsType("C")).isFalse();
    assertThat(database.getSchema().existsType("D")).isFalse();
    assertThat(database.getSchema().existsBucket("Free")).isTrue();
    assertThat(database.countBucket("Free")).isEqualTo(1L);
    assertThat(database.getSchema().existsBucket("C_0")).isTrue();
    assertThat(database.countBucket("C_0")).isEqualTo(1L);
    assertThat(database.getSchema().existsBucket("Fresh")).as("made for the type that was refused").isFalse();

    reopenDatabase();

    assertThat(database.countBucket("Free")).isEqualTo(1L);
    assertThat(database.countBucket("C_0")).isEqualTo(1L);
    assertThat(database.getSchema().existsType("B")).isFalse();
    assertThat(database.getSchema().existsType("C")).isFalse();
  }

  @Test
  void growingATypeOntoARefusedSecondSuperTypeLeavesItAsItWas() {
    database.getSchema().createDocumentType("Plain");
    createTimeSeriesType("Metrics");
    database.getSchema().createDocumentType("Grown");

    assertThatThrownBy(() -> database.getSchema().buildDocumentType().withName("Grown").withIgnoreIfExists(true)
        .withSuperType("Plain").withSuperType("Metrics").create())
        .isInstanceOf(SchemaException.class);

    assertThat(database.getSchema().getType("Grown").getSuperTypes()).isEmpty();
    assertThat(database.getSchema().getType("Plain").getSubTypes()).isEmpty();

    reopenDatabase();

    assertThat(database.getSchema().getType("Grown").getSuperTypes()).isEmpty();
    assertThat(database.getSchema().getType("Plain").getSubTypes()).isEmpty();
  }

  /**
   * The unlink of a super type linked earlier in the same growth is {@code removeSuperType}, which also drops the sub-indexes the
   * link propagated: the refusal comes from the second super type, after the first has put an index over the grown type's bucket.
   */
  @Test
  void growingATypeOntoARefusedSecondSuperTypeTakesTheIndexesOfTheFirstWithIt() {
    database.command("sql", "CREATE DOCUMENT TYPE Indexed");
    database.command("sql", "CREATE PROPERTY Indexed.p STRING");
    database.command("sql", "CREATE INDEX ON Indexed (p) NOTUNIQUE");
    createExternalSuperType();
    database.getSchema().createDocumentType("Grown");
    squat("Grown_0_ext");
    final int indexFilesBefore = subIndexCountOf("Indexed");

    assertThatThrownBy(() -> database.getSchema().buildDocumentType().withName("Grown").withIgnoreIfExists(true)
        .withSuperType("Indexed").withSuperType("Holder").create())
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("Grown_0_ext");

    assertThat(database.getSchema().getType("Grown").getSuperTypes()).isEmpty();
    assertThat(database.getSchema().getType("Indexed").getSubTypes()).isEmpty();
    assertThat(database.getSchema().getType("Grown").getAllIndexes(true)).isEmpty();
    assertThat(subIndexCountOf("Indexed")).isEqualTo(indexFilesBefore);

    reopenDatabase();

    assertThat(database.getSchema().getType("Grown").getSuperTypes()).isEmpty();
    assertThat(database.getSchema().getType("Grown").getAllIndexes(true)).isEmpty();
    assertThat(subIndexCountOf("Indexed")).isEqualTo(indexFilesBefore);
    // The type still takes records, and the index of the super type it never joined holds none of them
    database.transaction(() -> database.newDocument("Grown").set("p", "x").save());
    assertThat(database.query("sql", "SELECT FROM Indexed WHERE p = 'x'").stream().count()).isZero();
  }

  @Test
  void aTypeUnderValidSuperTypesIsStillCreatedWithEveryLink() {
    database.getSchema().createDocumentType("A");
    database.getSchema().createDocumentType("B");

    final DocumentType child = database.getSchema().buildDocumentType().withName("Child").withSuperType("A").withSuperType("B")
        .create();

    assertThat(child.getSuperTypes()).extracting(DocumentType::getName).containsExactlyInAnyOrder("A", "B");
    assertThat(database.getSchema().getType("A").getSubTypes()).extracting(DocumentType::getName).containsExactly("Child");

    reopenDatabase();

    assertThat(database.getSchema().getType("Child").getSuperTypes()).extracting(DocumentType::getName)
        .containsExactlyInAnyOrder("A", "B");
  }

  /** The type that was refused is not in the schema under its own name, nor through a bucket named after it. */
  private void assertNothingLeftOf(final String typeName, final String... keptBuckets) {
    final List<String> kept = List.of(keptBuckets);
    assertThat(database.getSchema().existsType(typeName)).as("type " + typeName).isFalse();
    assertThat(database.getSchema().getBuckets()).extracting(Bucket::getName).as("buckets named after " + typeName)
        .noneMatch(name -> name.startsWith(typeName + "_") && !kept.contains(name));
  }

  private void createTimeSeriesType(final String name) {
    database.command("sql", "CREATE TIMESERIES TYPE " + name + " TIMESTAMP ts TAGS (sensor STRING) FIELDS (temperature DOUBLE)");
  }

  /** The bucket indexes of every index {@code typeName} declares: one per bucket of the types the index covers. */
  private int subIndexCountOf(final String typeName) {
    int count = 0;
    for (final TypeIndex index : database.getSchema().getType(typeName).getAllIndexes(false))
      count += index.getIndexesOnBuckets().length;
    return count;
  }

  /** A super type with an EXTERNAL property: every type below it needs a paired {@code <bucket>_ext} bucket. */
  private void createExternalSuperType() {
    database.command("sql", "CREATE DOCUMENT TYPE Holder");
    database.command("sql", "CREATE PROPERTY Holder.blob STRING (EXTERNAL true)");
  }

  /** Makes {@code bucketName}, the name of a paired external bucket a type would adopt, the primary bucket of another type. */
  private void squat(final String bucketName) {
    database.command("sql", "CREATE BUCKET " + bucketName);
    database.command("sql", "CREATE DOCUMENT TYPE Squatter_" + bucketName + " BUCKET " + bucketName);
  }

  private void assertSquatterIntact(final String bucketName) {
    final String squatter = "Squatter_" + bucketName;
    assertThat(database.getSchema().existsType(squatter)).isTrue();
    assertThat(database.getSchema().getType(squatter).getBuckets(false)).extracting(Bucket::getName).containsExactly(bucketName);
  }
}
