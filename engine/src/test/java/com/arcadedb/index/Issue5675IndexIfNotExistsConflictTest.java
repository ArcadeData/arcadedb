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
import com.arcadedb.exception.DatabaseMetadataException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #5675: {@code CREATE INDEX IF NOT EXISTS} matched a pre-existing index on the
 * indexed property set alone and reported success even when the existing index did not provide what the
 * statement asked for. Tightening a {@code NOTUNIQUE} index to {@code UNIQUE} therefore did nothing while
 * telling the caller the unique constraint was in place.
 * <p>
 * The rule these tests pin down: {@code IF NOT EXISTS} is satisfied only when the existing index already
 * provides everything requested - the same index type and a uniqueness constraint at least as strong. A
 * {@code UNIQUE} index satisfies a {@code NOTUNIQUE} request (it indexes the same keys); a {@code NOTUNIQUE}
 * index does not satisfy a {@code UNIQUE} request. Anything else is a conflict reported to the caller, and
 * the existing index is never dropped implicitly.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue5675IndexIfNotExistsConflictTest extends TestHelper {

  @Override
  public void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE T");
      database.command("sql", "CREATE PROPERTY T.Scalar STRING");

      database.command("sql", "CREATE VERTEX TYPE L");
      database.command("sql", "CREATE PROPERTY L.Items LIST OF STRING");
    });
  }

  /**
   * The reported case: a NOTUNIQUE index exists, the guarded statement asks for UNIQUE. The request cannot be
   * honoured by the existing index, so it must be reported instead of answering success.
   */
  @Test
  void guardedUniqueOverNotUniqueIsReported() {
    database.command("sql", "CREATE INDEX ON T (Scalar) NOTUNIQUE");

    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX IF NOT EXISTS ON T (Scalar) UNIQUE"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("T[Scalar]")
        .hasMessageContaining("Scalar");

    // The existing index must survive the rejected request.
    assertThat(database.getSchema().existsIndex("T[Scalar]")).isTrue();
    assertThat(database.getSchema().getIndexByName("T[Scalar]").isUnique()).isFalse();

    // And it must still be usable: the rejection must not have left a half-dropped index behind.
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET Tag = 'a', Scalar = 'x'");
      database.command("sql", "INSERT INTO T SET Tag = 'b', Scalar = 'x'");
    });
    assertThat(database.countType("T", false)).isEqualTo(2L);
  }

  /**
   * Same conflict reached through the {@code BY ITEM} modifier, whose auto-derived index name only started
   * matching the canonical form after the #4879/#4881 fix - which is what turned the loud failure of 26.4.1
   * into the silent success reported here.
   */
  @Test
  void guardedUniqueOverNotUniqueByItemIsReported() {
    database.command("sql", "CREATE INDEX ON L (Items BY ITEM) NOTUNIQUE");

    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX IF NOT EXISTS ON L (Items BY ITEM) UNIQUE"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("L[Itemsbyitem]");

    assertThat(database.getSchema().getIndexByName("L[Itemsbyitem]").isUnique()).isFalse();
  }

  /**
   * A UNIQUE index already provides what a NOTUNIQUE request asks for (the same keys are indexed), so the
   * guarded statement is a no-op. It must never weaken the existing constraint by dropping and recreating it.
   */
  @Test
  void guardedNotUniqueOverUniqueKeepsTheConstraint() {
    database.command("sql", "CREATE INDEX ON T (Scalar) UNIQUE");

    final ResultSet rs = database.command("sql", "CREATE INDEX IF NOT EXISTS ON T (Scalar) NOTUNIQUE");
    assertThat(rs.next().<Boolean>getProperty("created")).isFalse();

    assertThat(database.getSchema().getIndexByName("T[Scalar]").isUnique()).isTrue();

    database.transaction(() -> database.command("sql", "INSERT INTO T SET Scalar = 'x'"));
    assertThatThrownBy(
        () -> database.transaction(() -> database.command("sql", "INSERT INTO T SET Scalar = 'x'")))
        .isInstanceOf(DuplicatedKeyException.class);
  }

  /**
   * The plain idempotent case must stay a no-op: same index type, same uniqueness.
   */
  @Test
  void guardedRepeatOfTheSameDefinitionIsANoOp() {
    database.command("sql", "CREATE INDEX ON T (Scalar) NOTUNIQUE");

    final ResultSet rs = database.command("sql", "CREATE INDEX IF NOT EXISTS ON T (Scalar) NOTUNIQUE");
    assertThat(rs.next().<Boolean>getProperty("created")).isFalse();

    assertThat(database.getSchema().getIndexByName("T[Scalar]").isUnique()).isFalse();
    assertThat(database.getSchema().getIndexByName("T[Scalar]").getType()).isEqualTo(Schema.INDEX_TYPE.LSM_TREE);
  }

  /**
   * Uniqueness is not the only part of the definition {@code IF NOT EXISTS} used to ignore: an index of a
   * different KIND on the same properties answered success too, leaving the caller with a full-text index
   * where it asked for a range one.
   */
  @Test
  void guardedIndexTypeMismatchIsReported() {
    database.command("sql", "CREATE INDEX ON T (Scalar) FULL_TEXT");

    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX IF NOT EXISTS ON T (Scalar) NOTUNIQUE"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("FULL_TEXT");

    assertThat(database.getSchema().getIndexByName("T[Scalar]").getType()).isEqualTo(Schema.INDEX_TYPE.FULL_TEXT);
  }

  /**
   * The same conflict must be reported when the statement carries a manual index name, where the name-based
   * shortcut does not fire and the request reaches the builder's property-based lookup instead.
   */
  @Test
  void guardedUniqueWithManualNameOverNotUniqueIsReported() {
    database.command("sql", "CREATE INDEX ON T (Scalar) NOTUNIQUE");

    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX MyIdx IF NOT EXISTS ON T (Scalar) UNIQUE"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("T[Scalar]");

    assertThat(database.getSchema().existsIndex("T[Scalar]")).isTrue();
    assertThat(database.getSchema().getIndexByName("T[Scalar]").isUnique()).isFalse();
    assertThat(database.getSchema().existsIndex("MyIdx")).isFalse();
  }

  /**
   * The engine API takes the same route: {@code withIgnoreIfExists(true)} used to drop the existing index and
   * rebuild it with the requested uniqueness, so a rebuild that failed on the duplicates already stored left
   * the type with no index at all.
   */
  @Test
  void builderIgnoreIfExistsNeverDropsAConflictingIndex() {
    database.command("sql", "CREATE INDEX ON T (Scalar) NOTUNIQUE");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET Scalar = 'x'");
      database.command("sql", "INSERT INTO T SET Scalar = 'x'");
    });

    assertThatThrownBy(() -> database.getSchema().buildTypeIndex("T", new String[] { "Scalar" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).withIgnoreIfExists(true).create())
        .isInstanceOf(IllegalArgumentException.class);

    assertThat(database.getSchema().existsIndex("T[Scalar]")).isTrue();
    assertThat(database.getSchema().getIndexByName("T[Scalar]").isUnique()).isFalse();

    // The surviving index must still resolve both duplicates.
    assertThat(database.query("sql", "SELECT FROM T WHERE Scalar = 'x'").stream().count()).isEqualTo(2L);
  }

  /**
   * The unguarded form keeps reporting the clash it always reported.
   */
  @Test
  void unguardedCreateOverAnExistingIndexStillFails() {
    database.command("sql", "CREATE INDEX ON T (Scalar) NOTUNIQUE");

    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX ON T (Scalar) UNIQUE"))
        .hasMessageContaining("T[Scalar]");
  }

  /**
   * A Cypher uniqueness constraint is a statement about the data, not a request to create an index if one is missing,
   * so it still upgrades a plain index on the same properties - the one caller allowed to replace an index. Neo4j
   * keeps the range index and the constraint side by side; ArcadeDB has a single index per property set and a unique
   * one indexes the same keys, so this is the equivalent end state.
   */
  @Test
  void cypherUniqueConstraintUpgradesAPlainIndex() {
    database.command("sql", "CREATE INDEX ON T (Scalar) NOTUNIQUE");

    database.command("opencypher", "CREATE CONSTRAINT IF NOT EXISTS FOR (n:T) REQUIRE n.Scalar IS UNIQUE");

    assertThat(database.getSchema().getIndexByName("T[Scalar]").isUnique()).isTrue();

    database.transaction(() -> database.command("sql", "INSERT INTO T SET Scalar = 'x'"));
    assertThatThrownBy(
        () -> database.transaction(() -> database.command("sql", "INSERT INTO T SET Scalar = 'x'")))
        .isInstanceOf(DuplicatedKeyException.class);
  }

  /**
   * The upgrade above is why an index may be dropped at all, so the case it cannot complete has to leave the previous
   * definition standing: duplicates already stored make the unique index impossible, and the type must not be left
   * without the index it had.
   */
  @Test
  void aFailedConstraintUpgradeLeavesThePlainIndexInPlace() {
    database.command("sql", "CREATE INDEX ON T (Scalar) NOTUNIQUE");

    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET Scalar = 'x'");
      database.command("sql", "INSERT INTO T SET Scalar = 'x'");
    });

    assertThatThrownBy(
        () -> database.command("opencypher", "CREATE CONSTRAINT IF NOT EXISTS FOR (n:T) REQUIRE n.Scalar IS UNIQUE"))
        .isNotNull();

    assertThat(database.getSchema().existsIndex("T[Scalar]")).isTrue();
    assertThat(database.getSchema().getIndexByName("T[Scalar]").isUnique()).isFalse();
    assertThat(database.query("sql", "SELECT FROM T WHERE Scalar = 'x'").stream().count()).isEqualTo(2L);
  }

  /**
   * The restore puts back the whole definition, page size included: it is the one attribute that is not derivable from
   * the properties, so losing it would silently re-file the index at the implementation default.
   */
  @Test
  void aFailedUpgradeRestoresThePageSizeToo() {
    database.getSchema().buildTypeIndex("T", new String[] { "Scalar" }).withType(Schema.INDEX_TYPE.LSM_TREE)
        .withUnique(false).withPageSize(16_384).create();

    final int pageSizeBefore = ((IndexInternal) database.getSchema().getIndexByName("T[Scalar]")).getPageSize();
    assertThat(pageSizeBefore).isEqualTo(16_384);

    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET Scalar = 'x'");
      database.command("sql", "INSERT INTO T SET Scalar = 'x'");
    });

    assertThatThrownBy(() -> database.getSchema().buildTypeIndex("T", new String[] { "Scalar" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).withReplaceIfIncompatible(true).create())
        .isNotNull();

    assertThat(database.getSchema().existsIndex("T[Scalar]")).isTrue();
    assertThat(((IndexInternal) database.getSchema().getIndexByName("T[Scalar]")).getPageSize()).isEqualTo(pageSizeBefore);
    assertThat(database.getSchema().getIndexByName("T[Scalar]").isUnique()).isFalse();
  }

  /**
   * The restore is faithful to the whole definition, and a manual index name is part of it: an index the caller named
   * itself must come back under that name rather than the auto-derived one. It travels on the captured
   * {@link com.arcadedb.schema.IndexMetadata#typeIndexName}, which {@code LocalSchema.createBucketIndex} copies onto
   * each bucket index and {@code LocalDocumentType.addIndexInternal} then honours when minting the TypeIndex (#4139) -
   * the same route a manual name takes on the normal creation path.
   */
  @Test
  void aFailedUpgradeRestoresAManualIndexNameAndItsPageSize() {
    database.getSchema().buildTypeIndex("T", new String[] { "Scalar" }).withType(Schema.INDEX_TYPE.LSM_TREE)
        .withUnique(false).withPageSize(16_384).withIndexName("HandPicked").create();

    assertThat(database.getSchema().existsIndex("HandPicked")).isTrue();

    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET Scalar = 'x'");
      database.command("sql", "INSERT INTO T SET Scalar = 'x'");
    });

    assertThatThrownBy(() -> database.getSchema().buildTypeIndex("T", new String[] { "Scalar" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).withReplaceIfIncompatible(true).create())
        .isNotNull();

    assertThat(database.getSchema().existsIndex("HandPicked"))
        .as("the restored index keeps the name its owner gave it, not the auto-derived one")
        .isTrue();
    assertThat(database.getSchema().getIndexByName("HandPicked").isUnique()).isFalse();
    assertThat(((IndexInternal) database.getSchema().getIndexByName("HandPicked")).getPageSize()).isEqualTo(16_384);
    assertThat(database.getSchema().existsIndex("T[Scalar]")).isFalse();
  }

  /**
   * Replacement is for the type's OWN index. An index it merely inherits belongs to the parent, and taking it away
   * there is the silent parent-index loss of issue #4083, so the explicit opt-in does not reach it.
   */
  @Test
  void replaceIfIncompatibleStillRefusesAnInheritedIndex() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Parent");
      database.command("sql", "CREATE PROPERTY Parent.Code STRING");
      database.command("sql", "CREATE INDEX ON Parent (Code) NOTUNIQUE");
      database.command("sql", "CREATE VERTEX TYPE Child EXTENDS Parent");
    });

    assertThatThrownBy(() -> database.getSchema().buildTypeIndex("Child", new String[] { "Code" })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).withReplaceIfIncompatible(true).create())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Parent");

    assertThat(database.getSchema().existsIndex("Parent[Code]")).isTrue();
    assertThat(database.getSchema().getIndexByName("Parent[Code]").isUnique()).isFalse();
  }

  /**
   * A manual index name is global, so it can already name an index on ANOTHER type. That is a different index, and the
   * guard must say so rather than answer "already exists" and leave the requested one uncreated.
   */
  @Test
  void guardedCreateWithANameTakenByAnotherTypeIsReported() {
    database.command("sql", "CREATE INDEX SharedName ON T (Scalar) NOTUNIQUE");

    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE U");
      database.command("sql", "CREATE PROPERTY U.Scalar STRING");
    });

    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX SharedName IF NOT EXISTS ON U (Scalar) NOTUNIQUE"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("SharedName");

    assertThat(database.getSchema().getIndexByName("SharedName").getTypeName()).isEqualTo("T");
    assertThat(database.getSchema().getType("U").getAllIndexes(false)).isEmpty();
  }

  /**
   * The requested index kind is read before the existing-index lookup, because deciding whether what is already there
   * covers the request needs to know what the request is. A builder with no kind is therefore refused even when an
   * index on those properties exists - where it used to hand that index back.
   */
  @Test
  void aBuilderWithoutAnIndexTypeIsRefused() {
    database.command("sql", "CREATE INDEX ON T (Scalar) NOTUNIQUE");

    assertThatThrownBy(() -> database.getSchema().buildTypeIndex("T", new String[] { "Scalar" })
        .withIgnoreIfExists(true).create())
        .isInstanceOf(DatabaseMetadataException.class)
        .hasMessageContaining("indexType");

    assertThat(database.getSchema().existsIndex("T[Scalar]")).isTrue();
  }

  /**
   * A guarded statement on a property with no index at all must still create one.
   */
  @Test
  void guardedCreateOnAFreshPropertyCreatesTheIndex() {
    assertThatNoException().isThrownBy(() -> database.command("sql", "CREATE INDEX IF NOT EXISTS ON T (Scalar) UNIQUE"));

    assertThat(database.getSchema().getIndexByName("T[Scalar]").isUnique()).isTrue();
  }
}
