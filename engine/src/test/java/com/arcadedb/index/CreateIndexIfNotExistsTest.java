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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Component;
import com.arcadedb.exception.DatabaseMetadataException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.CreateIndexStatement;
import com.arcadedb.query.sql.parser.Statement;
import com.arcadedb.schema.LSMVectorIndexMetadata;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for {@code CREATE INDEX [name] IF NOT EXISTS}: what an existing index must provide for the guard to
 * be satisfied (index kind, uniqueness, METADATA settings, name, inheritance), what is reported as a conflict instead
 * of a silent no-op, and the manual-index and cached-statement paths. Consolidated from the per-issue classes for
 * #5675, #5765, #6921 and #7228; each test keeps its issue number in its {@code @DisplayName}.
 */
public class CreateIndexIfNotExistsTest extends TestHelper {

  /**
   * Real record identities: a manual index entry is checked against the record it points at (a dangling one is
   * repaired instead of failing the write), so a fabricated RID would not exercise the unique path at all.
   */
  private RID RID_1;
  private RID RID_2;

  /** Identical text on every execution, so all of them go through ONE cached statement instance. */
  private static final String STATEMENT = "CREATE INDEX IF NOT EXISTS ON $t (uuid) UNIQUE";

  @Override
  protected void beginTest() {
    // #5675
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE T");
      database.command("sql", "CREATE PROPERTY T.Scalar STRING");

      database.command("sql", "CREATE VERTEX TYPE L");
      database.command("sql", "CREATE PROPERTY L.Items LIST OF STRING");
    });

    // #5765 index settings
    database.transaction(() -> {
      database.getSchema().createDocumentType("Doc");
      database.getSchema().getType("Doc").createProperty("embedding", Type.ARRAY_OF_FLOATS);
      database.getSchema().getType("Doc").createProperty("title", Type.STRING);
      database.getSchema().getType("Doc").createProperty("location", Type.STRING);
    });

    // #5765 manual index (its own type, so the settings fixture above keeps its Doc type untouched)
    database.transaction(() -> {
      database.getSchema().createDocumentType("ManualDoc");
      RID_1 = database.newDocument("ManualDoc").set("name", "one").save().getIdentity();
      RID_2 = database.newDocument("ManualDoc").set("name", "two").save().getIdentity();
    });

    // #6921
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE RgDoc");
      database.command("sql", "CREATE PROPERTY RgDoc.body STRING");
      database.command("sql", "INSERT INTO RgDoc SET body = 'TOKEN one'");
      database.command("sql", "INSERT INTO RgDoc SET body = 'TOKEN two'");
    });

    // #7228 super type
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Elemento");
      database.command("sql", "CREATE PROPERTY Elemento.uuid STRING");
      database.command("sql", "CREATE VERTEX TYPE ElementoNoIdentificable EXTENDS Elemento");
      database.command("sql", "CREATE VERTEX TYPE ElementoIdentificable EXTENDS Elemento");
    });

    // #7228 cached statement
    database.transaction(() -> {
      for (final String type : new String[] { "TypeA", "TypeB" }) {
        database.command("sql", "CREATE VERTEX TYPE " + type);
        database.command("sql", "CREATE PROPERTY " + type + ".uuid STRING");
      }
    });
  }

  /*
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

  /**
   * The reported case: a NOTUNIQUE index exists, the guarded statement asks for UNIQUE. The request cannot be
   * honoured by the existing index, so it must be reported instead of answering success.
   */
  @DisplayName("#5675 guarded unique over not unique is reported")
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
  @DisplayName("#5675 guarded unique over not unique by item is reported")
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
  @DisplayName("#5675 guarded not unique over unique keeps the constraint")
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
  @DisplayName("#5675 guarded repeat of the same definition is a no op")
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
  @DisplayName("#5675 guarded index type mismatch is reported")
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
  @DisplayName("#5675 guarded unique with manual name over not unique is reported")
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
  @DisplayName("#5675 builder ignore if exists never drops a conflicting index")
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
  @DisplayName("#5675 unguarded create over an existing index still fails")
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
  @DisplayName("#5675 cypher unique constraint upgrades a plain index")
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
  @DisplayName("#5675 a failed constraint upgrade leaves the plain index in place")
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
  @DisplayName("#5675 a failed upgrade restores the page size too")
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
  @DisplayName("#5675 a failed upgrade restores a manual index name and its page size")
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
  @DisplayName("#5675 replace if incompatible still refuses an inherited index")
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
  @DisplayName("#5675 guarded create with a name taken by another type is reported")
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
  @DisplayName("#5675 a builder without an index type is refused")
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
  @DisplayName("#5675 guarded create on a fresh property creates the index")
  @Test
  void guardedCreateOnAFreshPropertyCreatesTheIndex() {
    assertThatNoException().isThrownBy(() -> database.command("sql", "CREATE INDEX IF NOT EXISTS ON T (Scalar) UNIQUE"));

    assertThat(database.getSchema().getIndexByName("T[Scalar]").isUnique()).isTrue();
  }

  /*
   * Regression tests for issue #5765, item 3: {@code CREATE INDEX IF NOT EXISTS} compared the STRUCTURAL definition
   * only - the index kind and its uniqueness - so an existing index configured differently satisfied a request that
   * spelled out other settings. A guarded statement asking for 768 dimensions over a 384-dimension index was a silent
   * no-op, which is the shape of surprise issue #5675 removed one level up.
   * <p>
   * The rule these tests pin down: only the settings the {@code METADATA} clause NAMED are compared. A statement that
   * names none compares none and stays the plain no-op it has always been; a statement that names one gets an answer
   * about it, whether or not a rebuild would have been needed to change it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */

  /**
   * The reported case: the existing vector index has 384 dimensions and the guarded statement asks for 768. Every
   * vector the caller then writes is 768 long and none of them is indexed, which is exactly what the guard was
   * supposed to prevent.
   */
  @DisplayName("#5765 guarded vector dimensions mismatch is reported")
  @Test
  void guardedVectorDimensionsMismatchIsReported() {
    createVectorIndex(384, "COSINE");

    assertThatThrownBy(() -> database.command("sql",
        "CREATE INDEX IF NOT EXISTS ON Doc (embedding) LSM_VECTOR METADATA {\"dimensions\": 768}"))
        .hasMessageContaining("dimensions=384")
        .hasMessageContaining("768");

    assertThat(dimensionsOf("Doc[embedding]")).isEqualTo(384);
  }

  /**
   * Same setting, same value: the statement stays a no-op and the index is not touched.
   */
  @DisplayName("#5765 guarded matching settings stay a no op")
  @Test
  void guardedMatchingSettingsStayANoOp() {
    createVectorIndex(384, "COSINE");

    final ResultSet rs = database.command("sql",
        "CREATE INDEX IF NOT EXISTS ON Doc (embedding) LSM_VECTOR METADATA {\"dimensions\": 384, \"similarity\": \"COSINE\"}");
    assertThat(rs.next().<Boolean>getProperty("created")).isFalse();

    assertThat(dimensionsOf("Doc[embedding]")).isEqualTo(384);
  }

  /**
   * The clause is read through the index type's own reader before comparing, so a quoted number and a lower-case enum
   * name are the value they denote, not a difference.
   */
  @DisplayName("#5765 equivalent spellings are not a mismatch")
  @Test
  void equivalentSpellingsAreNotAMismatch() {
    createVectorIndex(384, "EUCLIDEAN");

    final ResultSet rs = database.command("sql",
        "CREATE INDEX IF NOT EXISTS ON Doc (embedding) LSM_VECTOR METADATA {\"dimensions\": \"384\", \"similarity\": \"euclidean\"}");
    assertThat(rs.next().<Boolean>getProperty("created")).isFalse();
  }

  /**
   * A setting other than the dimensions is compared the same way - here the similarity function, which decides what
   * "nearest" means and so silently changes every result the caller gets.
   */
  @DisplayName("#5765 guarded vector similarity mismatch is reported")
  @Test
  void guardedVectorSimilarityMismatchIsReported() {
    createVectorIndex(384, "COSINE");

    assertThatThrownBy(() -> database.command("sql",
        "CREATE INDEX IF NOT EXISTS ON Doc (embedding) LSM_VECTOR METADATA {\"dimensions\": 384, \"similarity\": \"EUCLIDEAN\"}"))
        .hasMessageContaining("similarity=COSINE");

    assertThat(similarityOf("Doc[embedding]")).isEqualTo("COSINE");
  }

  /**
   * Full-text analyzers decide how the text is tokenized, so an index built with another one answers different
   * queries. Reported, and the existing index survives.
   */
  @DisplayName("#5765 guarded full text analyzer mismatch is reported")
  @Test
  void guardedFullTextAnalyzerMismatchIsReported() {
    database.command("sql", "CREATE INDEX ON Doc (title) FULL_TEXT METADATA "
        + "{\"analyzer\": \"org.apache.lucene.analysis.core.KeywordAnalyzer\"}");

    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX IF NOT EXISTS ON Doc (title) FULL_TEXT METADATA "
        + "{\"analyzer\": \"org.apache.lucene.analysis.standard.StandardAnalyzer\"}"))
        .hasMessageContaining("analyzer=org.apache.lucene.analysis.core.KeywordAnalyzer");

    assertThat(database.getSchema().existsIndex("Doc[title]")).isTrue();
  }

  /**
   * Geospatial precision decides the cell resolution, so it changes what a query has to post-filter.
   */
  @DisplayName("#5765 guarded geospatial precision mismatch is reported")
  @Test
  void guardedGeospatialPrecisionMismatchIsReported() {
    database.command("sql", "CREATE INDEX ON Doc (location) GEOSPATIAL METADATA {\"precision\": 8}");

    assertThatThrownBy(() -> database.command("sql",
        "CREATE INDEX IF NOT EXISTS ON Doc (location) GEOSPATIAL METADATA {\"precision\": 11}"))
        .hasMessageContaining("precision=8");

    assertThat(database.getSchema().existsIndex("Doc[location]")).isTrue();
  }

  /**
   * The no-behavior-change case, and the reason the comparison is limited to the named keys: a guarded statement with
   * no {@code METADATA} names nothing, so it stays the plain no-op it has always been even over an index configured
   * with settings it says nothing about.
   */
  @DisplayName("#5765 a guarded statement without metadata stays a no op")
  @Test
  void aGuardedStatementWithoutMetadataStaysANoOp() {
    database.command("sql", "CREATE INDEX ON Doc (title) FULL_TEXT METADATA "
        + "{\"analyzer\": \"org.apache.lucene.analysis.core.KeywordAnalyzer\"}");

    final ResultSet rs = database.command("sql", "CREATE INDEX IF NOT EXISTS ON Doc (title) FULL_TEXT");
    assertThat(rs.next().<Boolean>getProperty("created")).isFalse();
  }

  /**
   * The settings are compared only once the structural definition matches: two index KINDS have unrelated key spaces,
   * so the conflict to report is the kind, not a list of settings one of them does not have.
   */
  @DisplayName("#5765 a kind mismatch is still reported as a kind mismatch")
  @Test
  void aKindMismatchIsStillReportedAsAKindMismatch() {
    database.command("sql", "CREATE INDEX ON Doc (title) NOTUNIQUE");

    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX IF NOT EXISTS ON Doc (title) FULL_TEXT METADATA "
        + "{\"analyzer\": \"org.apache.lucene.analysis.core.KeywordAnalyzer\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("LSM_TREE")
        .hasMessageContaining("FULL_TEXT")
        .hasMessageNotContaining("analyzer=");
  }

  /**
   * The same comparison asked through the OTHER entry point (issue #5781). Every case above writes the statement in
   * the auto-derived-name form, so {@code CreateIndexStatement} answers from its {@code existsIndex(name)} shortcut and
   * never reaches {@link com.arcadedb.schema.TypeIndexBuilder#create()}. Naming the index explicitly is what gets past
   * that shortcut: {@code myVectorIdx} does not exist, so the statement builds a builder, and the builder is the one
   * that finds the existing index BY PROPERTIES and compares the settings the clause named.
   */
  @DisplayName("#5765 named guarded vector dimensions mismatch is reported by the builder")
  @Test
  void namedGuardedVectorDimensionsMismatchIsReportedByTheBuilder() {
    createVectorIndex(384, "COSINE");

    assertThatThrownBy(() -> database.command("sql",
        "CREATE INDEX myVectorIdx IF NOT EXISTS ON Doc (embedding) LSM_VECTOR METADATA {\"dimensions\": 768}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("dimensions=384")
        .hasMessageContaining("768");

    assertThat(dimensionsOf("Doc[embedding]")).isEqualTo(384);
    assertThat(database.getSchema().existsIndex("myVectorIdx")).isFalse();
  }

  /**
   * Not a vector-only rule: the builder half is shared by every index type that keeps settings of its own, so a named
   * guarded full-text create over an existing auto-named index is compared on its analyzer the same way.
   */
  @DisplayName("#5765 named guarded full text analyzer mismatch is reported by the builder")
  @Test
  void namedGuardedFullTextAnalyzerMismatchIsReportedByTheBuilder() {
    database.command("sql", "CREATE INDEX ON Doc (title) FULL_TEXT METADATA "
        + "{\"analyzer\": \"org.apache.lucene.analysis.core.KeywordAnalyzer\"}");

    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX myTitleIdx IF NOT EXISTS ON Doc (title) FULL_TEXT "
        + "METADATA {\"analyzer\": \"org.apache.lucene.analysis.standard.StandardAnalyzer\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("analyzer=org.apache.lucene.analysis.core.KeywordAnalyzer");

    assertThat(database.getSchema().existsIndex("Doc[title]")).isTrue();
    assertThat(database.getSchema().existsIndex("myTitleIdx")).isFalse();
  }

  /**
   * The other direction of the builder half: settings that match make the guarded statement the no-op it asks to be.
   * The answer names the index that actually satisfied the request and reports {@code created: false}, so a caller
   * cannot read success as "my name is now on an index".
   * <p>
   * The vehicle used to be an explicit index name, which is what got the statement past {@code CreateIndexStatement}'s
   * {@code existsIndex(name)} shortcut and into the builder. A name the statement WROTE is now part of the request
   * (issue #6921) - it cannot be satisfied by an index carrying another one - so the no-op is reached here through a
   * SUBTYPE instead: {@code SubDoc[embedding]} does not exist either, and the builder finds the parent's index by
   * properties exactly the same way. The named form of this very statement is asserted right below.
   */
  @DisplayName("#5765 guarded matching settings stay a no op in the builder")
  @Test
  void guardedMatchingSettingsStayANoOpInTheBuilder() {
    createVectorIndex(384, "COSINE");
    database.command("sql", "CREATE DOCUMENT TYPE SubDoc EXTENDS Doc");

    final ResultSet rs = database.command("sql", "CREATE INDEX IF NOT EXISTS ON SubDoc (embedding) LSM_VECTOR "
        + "METADATA {\"dimensions\": 384, \"similarity\": \"COSINE\"}");
    final Result result = rs.next();
    assertThat(result.<Boolean>getProperty("created")).isFalse();
    assertThat(result.<String>getProperty("name")).isEqualTo("Doc[embedding]");
    assertThat(result.<Long>getProperty("totalIndexed")).isEqualTo(0L);

    assertThat(dimensionsOf("Doc[embedding]")).isEqualTo(384);
    assertThat(database.getSchema().existsIndex("SubDoc[embedding]")).isFalse();
  }

  /**
   * The named form of the statement above. Its settings match, so nothing in the definition is in the way and the
   * message carries no settings mismatch - the only thing that cannot be granted is the NAME, because a type holds
   * one index per property set and {@code Doc[embedding]} already occupies it (issue #6921).
   */
  @DisplayName("#5765 named guarded matching settings report the name conflict")
  @Test
  void namedGuardedMatchingSettingsReportTheNameConflict() {
    createVectorIndex(384, "COSINE");

    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX myVectorIdx IF NOT EXISTS ON Doc (embedding) "
        + "LSM_VECTOR METADATA {\"dimensions\": 384, \"similarity\": \"COSINE\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("myVectorIdx")
        .hasMessageContaining("Doc[embedding]")
        .hasMessageNotContaining("dimensions=");

    assertThat(dimensionsOf("Doc[embedding]")).isEqualTo(384);
    assertThat(database.getSchema().existsIndex("myVectorIdx")).isFalse();
  }

  /**
   * Same no-op, on the non-vector builder: a guarded full-text create whose analyzer matches leaves the existing index
   * alone and reports it under its own name.
   */
  @DisplayName("#5765 guarded matching full text settings stay a no op in the builder")
  @Test
  void guardedMatchingFullTextSettingsStayANoOpInTheBuilder() {
    database.command("sql", "CREATE INDEX ON Doc (title) FULL_TEXT METADATA "
        + "{\"analyzer\": \"org.apache.lucene.analysis.core.KeywordAnalyzer\"}");
    database.command("sql", "CREATE DOCUMENT TYPE SubDoc EXTENDS Doc");

    final ResultSet rs = database.command("sql", "CREATE INDEX IF NOT EXISTS ON SubDoc (title) FULL_TEXT "
        + "METADATA {\"analyzer\": \"org.apache.lucene.analysis.core.KeywordAnalyzer\"}");
    final Result result = rs.next();
    assertThat(result.<Boolean>getProperty("created")).isFalse();
    assertThat(result.<String>getProperty("name")).isEqualTo("Doc[title]");

    assertThat(database.getSchema().existsIndex("Doc[title]")).isTrue();
    assertThat(database.getSchema().existsIndex("SubDoc[title]")).isFalse();
  }

  /** The named form of the full-text no-op: matching settings, and the name is the only thing reported. */
  @DisplayName("#5765 named guarded matching full text settings report the name conflict")
  @Test
  void namedGuardedMatchingFullTextSettingsReportTheNameConflict() {
    database.command("sql", "CREATE INDEX ON Doc (title) FULL_TEXT METADATA "
        + "{\"analyzer\": \"org.apache.lucene.analysis.core.KeywordAnalyzer\"}");

    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX myTitleIdx IF NOT EXISTS ON Doc (title) FULL_TEXT "
        + "METADATA {\"analyzer\": \"org.apache.lucene.analysis.core.KeywordAnalyzer\"}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("myTitleIdx")
        .hasMessageContaining("Doc[title]")
        .hasMessageNotContaining("analyzer=");

    assertThat(database.getSchema().existsIndex("Doc[title]")).isTrue();
    assertThat(database.getSchema().existsIndex("myTitleIdx")).isFalse();
  }

  private void createVectorIndex(final int dimensions, final String similarity) {
    database.command("sql", "CREATE INDEX ON Doc (embedding) LSM_VECTOR METADATA {\"dimensions\": " + dimensions
        + ", \"similarity\": \"" + similarity + "\"}");
  }

  private int dimensionsOf(final String indexName) {
    return vectorMetadataOf(indexName).dimensions;
  }

  private String similarityOf(final String indexName) {
    return vectorMetadataOf(indexName).similarityFunction.name();
  }

  private LSMVectorIndexMetadata vectorMetadataOf(final String indexName) {
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName(indexName);
    assertThat(typeIndex.getType()).isEqualTo(Schema.INDEX_TYPE.LSM_VECTOR);
    return (LSMVectorIndexMetadata) typeIndex.getMetadataForNewFile();
  }

  /*
   * Regression tests for issue #5765: the manual index path.
   * <p>
   * Two defects, one on top of the other. Underneath, {@code ManualIndexBuilder} registered the new index under
   * {@code index instanceof PaginatedComponent} - a test that never matched, since {@code LSMTreeIndex} and
   * {@code HashIndex} WRAP their component - so the file stayed unknown to the schema and the creating commit
   * failed resolving it PAST the WAL append, fencing the database. Every manual index creation ended there.
   * <p>
   * Above it, the guarded branch carried the two defects issue #5675 removed from {@code TypeIndexBuilder}: a
   * request that merely differed in uniqueness DROPPED the existing index and rebuilt it, and the index kind was
   * not compared at all. A manual index is worse off than a type index there: its entries are not derived from any
   * record, so the drop destroys the only copy and no rebuild can bring them back.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */

  /**
   * The fix itself, asserted directly rather than inferred from the tests below happening to run: the index's file is
   * resolvable through the schema by its id.
   * <p>
   * That resolution IS the failure this PR is about. {@code TransactionContext.commit2ndPhase} calls
   * {@code getFileById} for every file whose page count the transaction changed, and it does so AFTER the WAL append -
   * so an unregistered file did not fail the creation, it fenced the whole database for recovery. Asserting the
   * registration pins the cause; asserting the database still commits afterwards pins the consequence, since every
   * operation on a fenced database throws.
   */
  @DisplayName("#5765 a manual index registers its file and leaves the database usable")
  @Test
  void aManualIndexRegistersItsFileAndLeavesTheDatabaseUsable() {
    final IndexInternal index = (IndexInternal) database.getSchema().buildManualIndex("manualIdx",
        new Type[] { Type.STRING }).withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();

    final Component component = index.getComponent();
    assertThat(database.getSchema().getEmbedded().getFileById(component.getFileId()))
        .as("the index's file must be resolvable by id, which is what the creating commit does past the WAL append")
        .isSameAs(component);

    // Not fenced: a fenced database refuses every operation, so a plain write that commits is the proof.
    database.transaction(() -> database.newDocument("ManualDoc").set("name", "after").save());
    assertThat(database.countType("ManualDoc", false)).isEqualTo(3L);

    // And the index itself still works after that unrelated write.
    database.transaction(() -> index.put(new Object[] { "a" }, new RID[] { RID_1 }));
    database.transaction(() -> assertThat(index.get(new Object[] { "a" }).next()).isEqualTo(RID_1));
  }

  /**
   * The floor everything else stands on: creating a manual index works at all, its entries are readable, and the
   * database is still usable afterwards.
   */
  @DisplayName("#5765 a manual index can be created and read")
  @Test
  void aManualIndexCanBeCreatedAndRead() {
    final Index index = database.getSchema().buildManualIndex("manualIdx", new Type[] { Type.STRING })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();

    database.transaction(() -> {
      index.put(new Object[] { "a" }, new RID[] { RID_1 });
      index.put(new Object[] { "b" }, new RID[] { RID_2 });
    });

    database.transaction(() -> {
      assertThat(index.get(new Object[] { "a" }).next()).isEqualTo(RID_1);
      assertThat(index.get(new Object[] { "b" }).next()).isEqualTo(RID_2);
      assertThat(index.get(new Object[] { "c" }).hasNext()).isFalse();
    });

    // A HASH manual index is the other kind that can be built without a type.
    final Index hashIndex = database.getSchema().buildManualIndex("manualHashIdx", new Type[] { Type.STRING })
        .withType(Schema.INDEX_TYPE.HASH).withUnique(true).create();

    database.transaction(() -> hashIndex.put(new Object[] { "a" }, new RID[] { RID_1 }));
    database.transaction(() -> assertThat(hashIndex.get(new Object[] { "a" }).next()).isEqualTo(RID_1));
  }

  /**
   * The uniqueness constraint of a manual index is enforced at commit. That check resolved the index's type name
   * to reach the polymorphic index covering the same properties - a manual index has neither, so it threw a
   * NullPointerException and no unique manual index could commit an entry.
   */
  @DisplayName("#5765 a unique manual index enforces its constraint")
  @Test
  void aUniqueManualIndexEnforcesItsConstraint() {
    final Index index = database.getSchema().buildManualIndex("manualIdx", new Type[] { Type.STRING })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).create();

    database.transaction(() -> index.put(new Object[] { "a" }, new RID[] { RID_1 }));

    assertThatThrownBy(() -> database.transaction(() -> index.put(new Object[] { "a" }, new RID[] { RID_2 })))
        .isInstanceOf(DuplicatedKeyException.class);

    // The refused write must leave the original entry alone.
    database.transaction(() -> assertThat(index.get(new Object[] { "a" }).next()).isEqualTo(RID_1));

    // A different key still goes in.
    database.transaction(() -> index.put(new Object[] { "b" }, new RID[] { RID_2 }));
    database.transaction(() -> assertThat(index.get(new Object[] { "b" }).next()).isEqualTo(RID_2));
  }

  /**
   * The index and its entries have to survive a close/reopen cycle: a manual index is reloaded from its file, and
   * nothing else in the schema holds a copy of what it contains.
   */
  @DisplayName("#5765 a manual index survives a reopen")
  @Test
  void aManualIndexSurvivesAReopen() {
    final Index index = database.getSchema().buildManualIndex("manualIdx", new Type[] { Type.STRING })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();

    database.transaction(() -> index.put(new Object[] { "a" }, new RID[] { RID_1 }));

    reopenDatabase();

    assertThat(database.getSchema().existsIndex("manualIdx")).isTrue();
    final Index reloaded = database.getSchema().getIndexByName("manualIdx");
    assertThat(reloaded.getType()).isEqualTo(Schema.INDEX_TYPE.LSM_TREE);
    database.transaction(() -> assertThat(reloaded.get(new Object[] { "a" }).next()).isEqualTo(RID_1));
  }

  /**
   * The reported case: a NOTUNIQUE manual index exists and the guarded request asks for UNIQUE. The existing
   * index cannot provide the constraint, so the request must be reported - and above all the entries already
   * stored in it must survive, since nothing else holds them.
   */
  @DisplayName("#5765 guarded unique over not unique keeps the index and its entries")
  @Test
  void guardedUniqueOverNotUniqueKeepsTheIndexAndItsEntries() {
    final Index existing = database.getSchema().buildManualIndex("manualIdx", new Type[] { Type.STRING })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();

    database.transaction(() -> {
      existing.put(new Object[] { "a" }, new RID[] { RID_1 });
      existing.put(new Object[] { "b" }, new RID[] { RID_2 });
    });

    assertThatThrownBy(() -> database.getSchema().buildManualIndex("manualIdx", new Type[] { Type.STRING })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).withIgnoreIfExists(true).create())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("manualIdx");

    assertThat(database.getSchema().existsIndex("manualIdx")).isTrue();
    final Index survivor = database.getSchema().getIndexByName("manualIdx");
    assertThat(survivor.isUnique()).isFalse();

    database.transaction(() -> {
      assertThat(survivor.get(new Object[] { "a" }).next()).isEqualTo(RID_1);
      assertThat(survivor.get(new Object[] { "b" }).next()).isEqualTo(RID_2);
    });
  }

  /**
   * The kind was not compared at all, so an index of one kind answered a request for another and the caller was
   * handed something it cannot use while being told the index it asked for exists.
   */
  @DisplayName("#5765 guarded different kind is reported")
  @Test
  void guardedDifferentKindIsReported() {
    database.getSchema().buildManualIndex("manualIdx", new Type[] { Type.STRING })
        .withType(Schema.INDEX_TYPE.HASH).withUnique(false).create();

    assertThatThrownBy(() -> database.getSchema().buildManualIndex("manualIdx", new Type[] { Type.STRING })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).withIgnoreIfExists(true).create())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("manualIdx")
        .hasMessageContaining("HASH");

    assertThat(database.getSchema().getIndexByName("manualIdx").getType()).isEqualTo(Schema.INDEX_TYPE.HASH);
  }

  /**
   * A UNIQUE index indexes exactly the keys a NOTUNIQUE one would, so it already provides what a NOTUNIQUE
   * request asks for: the guarded statement is a no-op that must not weaken the existing constraint.
   */
  @DisplayName("#5765 guarded not unique over unique is a no op")
  @Test
  void guardedNotUniqueOverUniqueIsANoOp() {
    final Index existing = database.getSchema().buildManualIndex("manualIdx", new Type[] { Type.STRING })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).create();

    final Index returned = database.getSchema().buildManualIndex("manualIdx", new Type[] { Type.STRING })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).withIgnoreIfExists(true).create();

    assertThat(returned).isSameAs(existing);
    assertThat(database.getSchema().getIndexByName("manualIdx").isUnique()).isTrue();
  }

  /**
   * The plain idempotent case: same kind, same uniqueness, the existing index comes back untouched with its
   * entries still in it.
   */
  @DisplayName("#5765 guarded identical request returns the existing index")
  @Test
  void guardedIdenticalRequestReturnsTheExistingIndex() {
    final Index existing = database.getSchema().buildManualIndex("manualIdx", new Type[] { Type.STRING })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();

    database.transaction(() -> existing.put(new Object[] { "a" }, new RID[] { RID_1 }));

    final Index returned = database.getSchema().buildManualIndex("manualIdx", new Type[] { Type.STRING })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).withIgnoreIfExists(true).create();

    assertThat(returned).isSameAs(existing);
    database.transaction(() -> assertThat(returned.get(new Object[] { "a" }).next()).isEqualTo(RID_1));
  }

  /**
   * An unguarded request over an existing name keeps refusing with the long-standing message, whether or not the
   * definition matches.
   */
  @DisplayName("#5765 unguarded request over an existing name is refused")
  @Test
  void unguardedRequestOverAnExistingNameIsRefused() {
    database.getSchema().buildManualIndex("manualIdx", new Type[] { Type.STRING })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();

    assertThatThrownBy(() -> database.getSchema().buildManualIndex("manualIdx", new Type[] { Type.STRING })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create())
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining("already exists");

    assertThat(database.getSchema().existsIndex("manualIdx")).isTrue();
  }

  /**
   * There is no implicit replacement on this path: a type index can be rebuilt from its records, a manual index
   * cannot, so the opt-in that upgrades a type index is refused outright at the call that asks for it.
   */
  @DisplayName("#5765 replace if incompatible is refused on a manual index")
  @Test
  void replaceIfIncompatibleIsRefusedOnAManualIndex() {
    assertThatThrownBy(() -> database.getSchema().buildManualIndex("manualIdx", new Type[] { Type.STRING })
        .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(true).withReplaceIfIncompatible(true))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("manualIdx");

    // Setting it to false is meaningless but harmless: it is already the default.
    assertThat(database.getSchema().buildManualIndex("manualIdx", new Type[] { Type.STRING })
        .withReplaceIfIncompatible(false)).isNotNull();
  }

  /**
   * An index kind that needs a type is refused by name instead of failing inside the factory with a
   * NullPointerException (FULL_TEXT, GEOSPATIAL) or a ClassCastException (the vector kinds).
   */
  @DisplayName("#5765 index kinds that need a type are refused")
  @Test
  void indexKindsThatNeedATypeAreRefused() {
    for (final Schema.INDEX_TYPE indexType : new Schema.INDEX_TYPE[] { Schema.INDEX_TYPE.FULL_TEXT,
        Schema.INDEX_TYPE.GEOSPATIAL, Schema.INDEX_TYPE.LSM_VECTOR, Schema.INDEX_TYPE.LSM_SPARSE_VECTOR }) {
      assertThatThrownBy(() -> database.getSchema().buildManualIndex("manualIdx", new Type[] { Type.STRING })
          .withType(indexType).create())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining(indexType.name());

      assertThat(database.getSchema().existsIndex("manualIdx")).isFalse();
    }
  }

  /**
   * A builder with no index type is refused by name rather than dereferencing the missing type in the factory.
   */
  @DisplayName("#5765 a missing index kind is reported")
  @Test
  void aMissingIndexKindIsReported() {
    assertThatThrownBy(
        () -> database.getSchema().buildManualIndex("manualIdx", new Type[] { Type.STRING }).withUnique(false).create())
        .isInstanceOf(DatabaseMetadataException.class)
        .hasMessageContaining("indexType");
  }

  /**
   * The index type reaches the factory: the deprecated {@code createManualIndex} overload takes one and used to
   * drop it on the floor, so every call through it failed on a null index type.
   */
  @DisplayName("#5765 deprecated create manual index honours the index type")
  @Test
  void deprecatedCreateManualIndexHonoursTheIndexType() {
    final Index index = database.getSchema()
        .createManualIndex(Schema.INDEX_TYPE.LSM_TREE, true, "manualIdx", new Type[] { Type.STRING }, 4096, null);

    assertThat(index.getType()).isEqualTo(Schema.INDEX_TYPE.LSM_TREE);
    assertThat(index.isUnique()).isTrue();
  }

  /*
   * Regression tests for issue #6921: {@code CREATE INDEX <name> IF NOT EXISTS} answered the guard with the index
   * already on those properties even when that index carries a DIFFERENT name. The requested name was never created,
   * so every later lookup through it - {@code SEARCH_INDEX('<name>', ...)}, {@code DROP INDEX <name>},
   * {@code REBUILD INDEX <name>} - failed on a name the statement had just reported on.
   * <p>
   * The rule these tests pin down: a name the statement WROTE is part of the request, not a label on it. A type keeps
   * one index per property set, so the requested name cannot be added next to the existing index either - the conflict
   * is reported, naming both indexes, instead of being answered by an index the caller cannot address. A statement that
   * wrote no name is unaffected: the auto-derived {@code typeName[properties]} form is not something the caller asked
   * for, so a guarded unnamed request stays the no-op it has always been.
   *
   * @author Roberto Franchini (r.franchini@arcadedata.com)
   */

  /**
   * The reported case verbatim: a FULL_TEXT index named {@code rgFt} exists on the property, and a guarded statement
   * asks for {@code rgFt2} on the same property. The requested name cannot be created, so the statement must say so
   * rather than answer for {@code rgFt}.
   */
  @DisplayName("#6921 guarded named request over a differently named index is reported")
  @Test
  void guardedNamedRequestOverADifferentlyNamedIndexIsReported() {
    database.command("sql", "CREATE INDEX rgFt ON RgDoc (body) FULL_TEXT");

    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX rgFt2 IF NOT EXISTS ON RgDoc (body) FULL_TEXT"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("rgFt2")
        .hasMessageContaining("rgFt")
        .hasMessageContaining("body");

    // The refusal must leave the existing index untouched and must not have created the requested name.
    assertThat(database.getSchema().existsIndex("rgFt")).isTrue();
    assertThat(database.getSchema().existsIndex("rgFt2")).isFalse();
  }

  /**
   * Why the name matters: ranking is reachable only through it. Before the fix the statement above reported on
   * {@code rgFt2} while this query failed, which is the mismatch the issue is about.
   */
  @DisplayName("#6921 the requested name is not usable after the refusal")
  @Test
  void theRequestedNameIsNotUsableAfterTheRefusal() {
    database.command("sql", "CREATE INDEX rgFt ON RgDoc (body) FULL_TEXT");

    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX rgFt2 IF NOT EXISTS ON RgDoc (body) FULL_TEXT"))
        .isInstanceOf(IllegalArgumentException.class);

    assertThatThrownBy(() -> database.query("sql", "SELECT FROM RgDoc WHERE SEARCH_INDEX('rgFt2', 'TOKEN') = true"))
        .hasMessageContaining("rgFt2");

    // ...while the index that DOES exist answers, so the refusal is about the name and nothing else.
    try (final ResultSet rs = database.query("sql", "SELECT FROM RgDoc WHERE SEARCH_INDEX('rgFt', 'TOKEN') = true")) {
      assertThat(rs.stream().count()).isEqualTo(2L);
    }
  }

  /**
   * The guard still works under the index's own name: re-running the very same statement is a no-op, so the fix
   * cannot be passing the test above by refusing every guarded request.
   */
  @DisplayName("#6921 guarded named request under its own name stays a no op")
  @Test
  void guardedNamedRequestUnderItsOwnNameStaysANoOp() {
    database.command("sql", "CREATE INDEX rgFt ON RgDoc (body) FULL_TEXT");

    try (final ResultSet rs = database.command("sql", "CREATE INDEX rgFt IF NOT EXISTS ON RgDoc (body) FULL_TEXT")) {
      final Result result = rs.next();
      assertThat(result.<Boolean>getProperty("created")).isFalse();
      assertThat(result.<String>getProperty("name")).isEqualTo("rgFt");
    }

    assertThat(database.getSchema().existsIndex("rgFt")).isTrue();
  }

  /**
   * A statement that wrote no name asked for no name, so the auto-derived form is not part of the request and the
   * existing index answers the guard exactly as before.
   */
  @DisplayName("#6921 guarded unnamed request over a named index stays a no op")
  @Test
  void guardedUnnamedRequestOverANamedIndexStaysANoOp() {
    database.command("sql", "CREATE INDEX rgFt ON RgDoc (body) FULL_TEXT");

    try (final ResultSet rs = database.command("sql", "CREATE INDEX IF NOT EXISTS ON RgDoc (body) FULL_TEXT")) {
      final Result result = rs.next();
      assertThat(result.<Boolean>getProperty("created")).isFalse();
      // Reported under the name of the index that actually satisfied it, never under a name that does not exist.
      assertThat(result.<String>getProperty("name")).isEqualTo("rgFt");
    }

    assertThat(database.getSchema().existsIndex("rgFt")).isTrue();
    assertThat(database.getSchema().existsIndex("RgDoc[body]")).isFalse();
  }

  /**
   * The mirror of the unnamed case: a named request over the auto-derived index is still a name that does not exist,
   * so it is reported too. Covers the ordinary LSM_TREE kind, to show the rule is not FULL_TEXT specific.
   */
  @DisplayName("#6921 guarded named request over an auto named index is reported")
  @Test
  void guardedNamedRequestOverAnAutoNamedIndexIsReported() {
    database.command("sql", "CREATE INDEX ON RgDoc (body) NOTUNIQUE");
    assertThat(database.getSchema().existsIndex("RgDoc[body]")).isTrue();

    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX rgIdx IF NOT EXISTS ON RgDoc (body) NOTUNIQUE"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("rgIdx")
        .hasMessageContaining("RgDoc[body]");

    assertThat(database.getSchema().existsIndex("rgIdx")).isFalse();
    assertThat(database.getSchema().existsIndex("RgDoc[body]")).isTrue();
  }

  /**
   * An index the type only INHERITS is the same story, and the message has to say where the index lives - dropping
   * it takes it away from the parent type, which is never something this statement does implicitly (issue #4083).
   */
  @DisplayName("#6921 guarded named request over an inherited index is reported")
  @Test
  void guardedNamedRequestOverAnInheritedIndexIsReported() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE RgChild EXTENDS RgDoc");
      database.command("sql", "CREATE INDEX rgParentIdx ON RgDoc (body) NOTUNIQUE");
    });

    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX rgChildIdx IF NOT EXISTS ON RgChild (body) NOTUNIQUE"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("rgChildIdx")
        .hasMessageContaining("rgParentIdx")
        .hasMessageContaining("RgDoc");

    assertThat(database.getSchema().existsIndex("rgChildIdx")).isFalse();
    assertThat(database.getSchema().existsIndex("rgParentIdx")).isTrue();
  }

  /**
   * Without the guard the statement was already refused, but with wording that offered {@code IF NOT EXISTS} as the
   * way to make it idempotent. That advice is wrong for a name conflict - the guarded form is refused too - so the
   * name conflict has to be reported as itself on both paths.
   */
  @DisplayName("#6921 unguarded named request over a differently named index names the conflict")
  @Test
  void unguardedNamedRequestOverADifferentlyNamedIndexNamesTheConflict() {
    database.command("sql", "CREATE INDEX rgFt ON RgDoc (body) FULL_TEXT");

    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX rgFt2 ON RgDoc (body) FULL_TEXT"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("rgFt2")
        .hasMessageContaining("rgFt")
        .hasMessageNotContaining("IF NOT EXISTS");

    assertThat(database.getSchema().existsIndex("rgFt2")).isFalse();
  }

  /**
   * The name is the LAST of the three things compared, so a request that gets both the definition and the name wrong
   * is still told about the definition - the harder problem, and the one the #5675 rule is about. Written with a
   * manual name on purpose: that is the case where the two answers compete.
   */
  @DisplayName("#6921 a kind conflict is still reported as a kind conflict")
  @Test
  void aKindConflictIsStillReportedAsAKindConflict() {
    database.command("sql", "CREATE INDEX rgFt ON RgDoc (body) FULL_TEXT");

    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX rgIdx IF NOT EXISTS ON RgDoc (body) UNIQUE"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("FULL_TEXT")
        .hasMessageContaining("LSM_TREE");

    // ...and the unnamed form, where nothing competes, reports the same conflict.
    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX IF NOT EXISTS ON RgDoc (body) UNIQUE"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("FULL_TEXT")
        .hasMessageContaining("LSM_TREE");
  }

  /**
   * And the requested name is created normally when the properties are free - the fix must not stand between a
   * first guarded statement and the index it asks for.
   */
  @DisplayName("#6921 guarded named request on free properties creates the name")
  @Test
  void guardedNamedRequestOnFreePropertiesCreatesTheName() {
    try (final ResultSet rs = database.command("sql", "CREATE INDEX rgFt IF NOT EXISTS ON RgDoc (body) FULL_TEXT")) {
      final Result result = rs.next();
      assertThat(result.<Boolean>getProperty("created")).isTrue();
      assertThat(result.<String>getProperty("name")).isEqualTo("rgFt");
    }

    assertThat(database.getSchema().existsIndex("rgFt")).isTrue();

    try (final ResultSet rs = database.query("sql", "SELECT FROM RgDoc WHERE SEARCH_INDEX('rgFt', 'TOKEN') = true")) {
      assertThat(rs.stream().count()).isEqualTo(2L);
    }
  }

  /*
   * Regression tests for issue #7228: {@code CREATE INDEX <name> IF NOT EXISTS ON <subType> (...)} was refused when
   * the index of that name is declared on a SUPER type of {@code <subType>}, with "an index with that name already
   * exists on type '...'. Drop it first or choose a different index name".
   * <p>
   * The refusal was wrong on its own terms. A type index is built over {@code getBuckets(true)} - the polymorphic
   * bucket list - so an index declared on {@code Elemento} already indexes every {@code ElementoNoIdentificable}
   * record; there is nothing for the statement to create and nothing the guard has to warn about. The unnamed form of
   * the very same statement had always been a no-op, because {@code TypeIndexBuilder.create()} finds the inherited
   * index through {@code getPolymorphicIndexByProperties}; only the named form, which is answered by a shortcut in
   * {@link com.arcadedb.query.sql.parser.CreateIndexStatement} before any builder exists, disagreed. That made a
   * schema script non-idempotent the moment one of its named indexes moved up the hierarchy - the one thing
   * IF NOT EXISTS is written to prevent.
   * <p>
   * The relation is checked in one direction only, which the tests below pin down: an index on a SUB type covers
   * strictly fewer buckets than a request on its parent, so it does not satisfy it and is still reported.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */

  /**
   * The reported case verbatim: the index lives on the super type and the schema script asks for it again, by name,
   * on a sub type.
   */
  @DisplayName("#7228 guarded named request over an index inherited from the super type is a no op")
  @Test
  void guardedNamedRequestOverAnIndexInheritedFromTheSuperTypeIsANoOp() {
    database.command("sql", "CREATE INDEX `Elemento_uuid` ON `Elemento` (uuid) UNIQUE");

    try (final ResultSet rs = database.command("sql",
        "CREATE INDEX `Elemento_uuid` IF NOT EXISTS ON `ElementoNoIdentificable` (uuid) UNIQUE")) {
      final Result result = rs.next();
      assertThat(result.<Boolean>getProperty("created")).isFalse();
      assertThat(result.<String>getProperty("name")).isEqualTo("Elemento_uuid");
    }

    // Exactly one index, still the one on the super type: the no-op must not have created a second.
    assertThat(database.getSchema().existsIndex("Elemento_uuid")).isTrue();
    assertThat(database.getSchema().getIndexByName("Elemento_uuid").getTypeName()).isEqualTo("Elemento");
    assertThat(database.getSchema().getType("ElementoNoIdentificable").getAllIndexes(false)).isEmpty();
  }

  /**
   * The point of answering the guard rather than refusing it: the inherited index really does constrain the sub
   * type, so the statement that was told "already exists" was told the truth.
   */
  @DisplayName("#7228 the inherited index enforces uniqueness on the sub type")
  @Test
  void theInheritedIndexEnforcesUniquenessOnTheSubType() {
    database.command("sql", "CREATE INDEX `Elemento_uuid` ON `Elemento` (uuid) UNIQUE");
    database.command("sql", "CREATE INDEX `Elemento_uuid` IF NOT EXISTS ON `ElementoNoIdentificable` (uuid) UNIQUE");

    database.transaction(() -> database.command("sql", "INSERT INTO ElementoNoIdentificable SET uuid = 'u1'"));

    assertThatThrownBy(() -> database.transaction(
        () -> database.command("sql", "INSERT INTO ElementoNoIdentificable SET uuid = 'u1'")))
        .isInstanceOf(DuplicatedKeyException.class);

    // ...and across the hierarchy, not just within the one sub type.
    assertThatThrownBy(() -> database.transaction(
        () -> database.command("sql", "INSERT INTO ElementoIdentificable SET uuid = 'u1'")))
        .isInstanceOf(DuplicatedKeyException.class);
  }

  /**
   * A whole schema script re-run end to end: every statement is guarded, so the second run must change nothing.
   */
  @DisplayName("#7228 a guarded schema script stays idempotent across the hierarchy")
  @Test
  void aGuardedSchemaScriptStaysIdempotentAcrossTheHierarchy() {
    final String script = "CREATE INDEX `Elemento_uuid` IF NOT EXISTS ON `Elemento` (uuid) UNIQUE;"
        + "CREATE INDEX `Elemento_uuid` IF NOT EXISTS ON `ElementoNoIdentificable` (uuid) UNIQUE;"
        + "CREATE INDEX `Elemento_uuid` IF NOT EXISTS ON `ElementoIdentificable` (uuid) UNIQUE;";

    database.command("sqlscript", script);
    database.command("sqlscript", script);

    assertThat(database.getSchema().getIndexes()).filteredOn(i -> "Elemento_uuid".equals(i.getName())).hasSize(1);
  }

  /**
   * The other direction is NOT symmetric: an index on a sub type indexes only that sub type's buckets, so it cannot
   * answer a request that spans the whole hierarchy. The conflict is still reported, naming both types.
   */
  @DisplayName("#7228 guarded named request over an index on a sub type is still reported")
  @Test
  void guardedNamedRequestOverAnIndexOnASubTypeIsStillReported() {
    database.command("sql", "CREATE INDEX `sub_uuid` ON `ElementoNoIdentificable` (uuid) UNIQUE");

    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX `sub_uuid` IF NOT EXISTS ON `Elemento` (uuid) UNIQUE"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("sub_uuid")
        .hasMessageContaining("Elemento")
        .hasMessageContaining("ElementoNoIdentificable");

    assertThat(database.getSchema().getIndexByName("sub_uuid").getTypeName()).isEqualTo("ElementoNoIdentificable");
  }

  /**
   * An unrelated type is still a conflict: the fix must not have turned the name check into a formality.
   */
  @DisplayName("#7228 guarded named request over an index on an unrelated type is still reported")
  @Test
  void guardedNamedRequestOverAnIndexOnAnUnrelatedTypeIsStillReported() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Otro");
      database.command("sql", "CREATE PROPERTY Otro.uuid STRING");
    });
    database.command("sql", "CREATE INDEX `otro_uuid` ON `Otro` (uuid) UNIQUE");

    assertThatThrownBy(
        () -> database.command("sql", "CREATE INDEX `otro_uuid` IF NOT EXISTS ON `ElementoNoIdentificable` (uuid) UNIQUE"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("otro_uuid")
        .hasMessageContaining("Otro");
  }

  /**
   * The inherited index has to satisfy the request on its own terms too: a NOTUNIQUE parent index does not answer a
   * request for the UNIQUE constraint, exactly as it does not when the index is the type's own (issue #5675).
   */
  @DisplayName("#7228 an inherited index of the wrong kind does not answer the guard")
  @Test
  void anInheritedIndexOfTheWrongKindDoesNotAnswerTheGuard() {
    database.command("sql", "CREATE INDEX `Elemento_uuid` ON `Elemento` (uuid) NOTUNIQUE");

    assertThatThrownBy(
        () -> database.command("sql", "CREATE INDEX `Elemento_uuid` IF NOT EXISTS ON `ElementoNoIdentificable` (uuid) UNIQUE"))
        .hasMessageContaining("Elemento_uuid");
  }

  /**
   * Different properties under the same name stay a conflict even up the hierarchy: the name matched, the request
   * did not.
   */
  @DisplayName("#7228 guarded named request on different properties of the super type is still reported")
  @Test
  void guardedNamedRequestOnDifferentPropertiesOfTheSuperTypeIsStillReported() {
    database.transaction(() -> database.command("sql", "CREATE PROPERTY Elemento.code STRING"));
    database.command("sql", "CREATE INDEX `Elemento_uuid` ON `Elemento` (uuid) UNIQUE");

    assertThatThrownBy(
        () -> database.command("sql", "CREATE INDEX `Elemento_uuid` IF NOT EXISTS ON `ElementoNoIdentificable` (code) UNIQUE"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Elemento_uuid")
        .hasMessageContaining("code");
  }

  /**
   * Without the guard the statement is not asking to be made idempotent, so the pre-existing index is still
   * reported rather than silently accepted.
   */
  @DisplayName("#7228 an unguarded named request over an inherited index is still reported")
  @Test
  void anUnguardedNamedRequestOverAnInheritedIndexIsStillReported() {
    database.command("sql", "CREATE INDEX `Elemento_uuid` ON `Elemento` (uuid) UNIQUE");

    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX `Elemento_uuid` ON `ElementoNoIdentificable` (uuid) UNIQUE"))
        .hasMessageContaining("Elemento_uuid");
  }

  /*
   * A parsed statement is a CACHED, SHARED object: {@code StatementCache.get} hands the same instance back for the same
   * text, execution after execution, and the {@code sql} engine goes through it on every command.
   * {@code CreateIndexStatement} mutates two of its own fields while it runs - a {@code $variable} type name is replaced
   * by what it resolved to, and an unnamed statement has {@code name} backfilled with the auto-derived
   * {@code typeName[properties]} form - and used to restore only the first, only on the success path.
   * <p>
   * Every {@code return} and {@code throw} before that line therefore leaked the resolved type into the shared
   * statement, the {@code IF NOT EXISTS} shortcut chief among them. The next execution of the same text then saw a type
   * name that no longer starts with {@code $}, skipped variable resolution entirely, and indexed the FIRST execution's
   * type - silently, with no error to say the variable had been ignored.
   * <p>
   * The {@code $variable} + cache combination is reached in production through
   * {@code SQLQueryEngine.command(query, configuration, parameters, variables)}, which is what
   * {@code SQLTriggerExecutor} runs a trigger body with; the tests use the same entry point. ({@code sqlscript} cannot
   * expose the defect: it has no statement cache and re-parses every execution.)
   * <p>
   * Found while reviewing the #7228 fix, which adds one more early return through the same shortcut.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */

  @DisplayName("#7228 a guarded statement over a variable type does not carry the resolved type into the next execution")
  @Test
  void aGuardedStatementOverAVariableTypeDoesNotCarryTheResolvedTypeIntoTheNextExecution() {
    // 1st: creates the index on TypeA and leaves through the success path.
    run("TypeA");
    // 2nd: same type, so the index already exists and the statement leaves through the IF NOT EXISTS shortcut - the
    // early return that used to skip the restore.
    run("TypeA");
    // 3rd: a DIFFERENT type. With the leak $t is never re-resolved and this indexes TypeA all over again.
    run("TypeB");

    assertThat(database.getSchema().getType("TypeB").getAllIndexes(false))
        .as("the third execution asked for an index on TypeB, so TypeB must have one")
        .isNotEmpty();
    assertThat(database.getSchema().existsIndex("TypeB[uuid]")).isTrue();
    assertThat(database.getSchema().getIndexByName("TypeB[uuid]").getTypeName()).isEqualTo("TypeB");
  }

  /** The constraint really landed on TypeB, so the statement did what it reported. */
  @DisplayName("#7228 the inherited constraint is enforced on the type the last execution named")
  @Test
  void theInheritedConstraintIsEnforcedOnTheTypeTheLastExecutionNamed() {
    run("TypeA");
    run("TypeA");
    run("TypeB");

    database.transaction(() -> database.command("sql", "INSERT INTO TypeB SET uuid = 'u1'"));

    assertThat(database.getSchema().getIndexByName("TypeB[uuid]").isUnique()).isTrue();
  }

  /**
   * Straight at the shared object: after any execution the statement must read back exactly as it was parsed, so the
   * next caller of the same text gets the statement they wrote and not the previous execution's residue.
   */
  @DisplayName("#7228 the cached statement is unchanged by execution")
  @Test
  void theCachedStatementIsUnchangedByExecution() {
    run("TypeA");

    final Statement cached = ((DatabaseInternal) database).getStatementCache().get(STATEMENT);
    assertThat(cached).isInstanceOf(CreateIndexStatement.class);

    final CreateIndexStatement createIndex = (CreateIndexStatement) cached;
    assertThat(createIndex.typeName.getStringValue())
        .as("the type name must still be the variable the statement was written with")
        .isEqualTo("$t");
    assertThat(createIndex.name)
        .as("the statement named no index, so no auto-derived name may survive its execution")
        .isNull();

    // ...and still true after the guarded execution that returns through the shortcut.
    run("TypeA");
    assertThat(((CreateIndexStatement) ((DatabaseInternal) database).getStatementCache().get(STATEMENT)).typeName.getStringValue())
        .isEqualTo("$t");
  }

  private void run(final String type) {
    final SQLQueryEngine engine = (SQLQueryEngine) database.getQueryEngine("sql");
    try (final ResultSet rs = engine.command(STATEMENT, database.getConfiguration(), Map.of(), Map.of("$t", type))) {
      rs.stream().forEach(r -> {
      });
    }
  }
}
