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
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.SchemaException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for #12318: {@code DROP TYPE P} re-parents P's sub types and drops P's declarations, and left an index of a sub
 * type (C[k]) keyed on a property only P declared, which {@code DROP PROPERTY P.k} refuses. The drop is refused, naming the index,
 * and nothing changes; a property the sub type still has after the drop does not count.
 */
class DropTypeIndexedInheritedPropertyTest extends TestHelper {

  @Test
  void indexOfASubTypeOnAPropertyOnlyTheDroppedTypeDeclaresRefusesTheDrop() {
    database.command("sql", "CREATE DOCUMENT TYPE P");
    database.command("sql", "CREATE DOCUMENT TYPE C EXTENDS P");
    database.command("sql", "CREATE PROPERTY P.k INTEGER");
    database.command("sql", "CREATE INDEX ON C (k) UNIQUE");
    database.transaction(() -> database.command("sql", "INSERT INTO C SET k = 1"));

    assertThatThrownBy(() -> database.command("sql", "DROP TYPE P"))//
        .isInstanceOf(SchemaException.class)//
        .hasMessageContaining("Cannot drop the type 'P'")//
        .hasMessageContaining("'k'")//
        .hasMessageContaining("'C[k]'");

    assertUnchanged();
    reopenDatabase();
    assertUnchanged();

    // the property still converts the value the index keys: the string '1' is the integer 1 already held
    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO C SET k = '1'")))//
        .isInstanceOf(DuplicatedKeyException.class);
  }

  @Test
  void indexOfATypeTwoLevelsBelowAndCompositeAndByItemIndexesRefuseTheDrop() {
    database.command("sql", "CREATE DOCUMENT TYPE P");
    database.command("sql", "CREATE PROPERTY P.k INTEGER");
    database.command("sql", "CREATE PROPERTY P.tags LIST OF STRING");
    database.command("sql", "CREATE DOCUMENT TYPE C EXTENDS P");
    database.command("sql", "CREATE PROPERTY C.s STRING");
    database.command("sql", "CREATE DOCUMENT TYPE X EXTENDS C");

    database.command("sql", "CREATE INDEX ON X (k) NOTUNIQUE");
    assertThatThrownBy(() -> database.command("sql", "DROP TYPE P")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("type 'X'").hasMessageContaining("'X[k]'");
    database.command("sql", "DROP INDEX `X[k]`");

    database.command("sql", "CREATE INDEX ON C (s, k) NOTUNIQUE");
    assertThatThrownBy(() -> database.command("sql", "DROP TYPE P")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("'C[s,k]'");
    database.command("sql", "DROP INDEX `C[s,k]`");

    database.command("sql", "CREATE INDEX ON C (tags BY ITEM) NOTUNIQUE");
    assertThatThrownBy(() -> database.command("sql", "DROP TYPE P")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("'tags'");

    assertThat(database.getSchema().existsType("P")).isTrue();
    assertThat(database.getSchema().getType("C").getSuperTypes()).extracting(DocumentType::getName).containsExactly("P");
  }

  /** A property the sub type still has after the drop: declared by itself, by another super type, or above the dropped type. */
  @Test
  void aPropertyTheSubTypeKeepsAllowsTheDrop() {
    database.command("sql", "CREATE DOCUMENT TYPE G");
    database.command("sql", "CREATE PROPERTY G.g INTEGER");
    database.command("sql", "CREATE DOCUMENT TYPE Q");
    database.command("sql", "CREATE PROPERTY Q.q INTEGER");
    database.command("sql", "CREATE DOCUMENT TYPE P EXTENDS G");
    database.command("sql", "CREATE PROPERTY P.q INTEGER");
    database.command("sql", "CREATE DOCUMENT TYPE C EXTENDS P, Q");
    database.command("sql", "CREATE PROPERTY C.own INTEGER");
    database.command("sql", "CREATE INDEX ON C (g) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON C (q) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON C (own) NOTUNIQUE");
    // P's own index goes with it, as before
    database.command("sql", "CREATE INDEX ON P (q) NOTUNIQUE");

    database.command("sql", "DROP TYPE P");

    assertThat(database.getSchema().existsType("P")).isFalse();
    assertThat(database.getSchema().getType("C").getSuperTypes()).extracting(DocumentType::getName)
        .containsExactlyInAnyOrder("G", "Q");
    assertThat(database.getSchema().existsIndex("C[g]")).isTrue();
    assertThat(database.getSchema().existsIndex("C[q]")).isTrue();
    assertThat(database.getSchema().existsIndex("C[own]")).isTrue();
    assertThat(database.getSchema().existsIndex("P[q]")).isFalse();
  }

  /**
   * The most ordinary drop: the dropped type indexes its own property and its sub types inherit both. The index is the dropped
   * type's, not the sub types', so nothing is refused, whether the schema was just built or was loaded from its file.
   */
  @Test
  void indexOfTheDroppedTypeOnItsOwnPropertyAllowsTheDrop() {
    database.command("sql", "CREATE DOCUMENT TYPE P");
    database.command("sql", "CREATE PROPERTY P.k INTEGER");
    database.command("sql", "CREATE DOCUMENT TYPE C EXTENDS P");
    database.command("sql", "CREATE DOCUMENT TYPE X EXTENDS C");
    database.command("sql", "CREATE INDEX ON P (k) UNIQUE");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO C SET k = 1");
      database.command("sql", "INSERT INTO X SET k = 2");
    });

    reopenDatabase();
    database.command("sql", "DROP TYPE P");

    final Schema schema = database.getSchema();
    assertThat(schema.existsType("P")).isFalse();
    assertThat(schema.existsIndex("P[k]")).isFalse();
    assertThat(schema.getType("C").getSuperTypes()).isEmpty();
    assertThat(schema.getType("X").getSuperTypes()).extracting(DocumentType::getName).containsExactly("C");
    assertThat(database.countType("C", true)).isEqualTo(2);
  }

  /** The refusal does not depend on the schema having been built in this session: a loaded schema answers the same. */
  @Test
  void theRefusalHoldsOnALoadedSchema() {
    database.command("sql", "CREATE DOCUMENT TYPE P");
    database.command("sql", "CREATE PROPERTY P.k INTEGER");
    database.command("sql", "CREATE DOCUMENT TYPE C EXTENDS P");
    database.command("sql", "CREATE INDEX ON C (k) UNIQUE");
    database.transaction(() -> database.command("sql", "INSERT INTO C SET k = 1"));

    reopenDatabase();

    assertThatThrownBy(() -> database.command("sql", "DROP TYPE P"))//
        .isInstanceOf(SchemaException.class)//
        .hasMessageContaining("Cannot drop the type 'P'")//
        .hasMessageContaining("'C[k]'");
    assertUnchanged();
  }

  /**
   * DROP MATERIALIZED VIEW drops the backing type after unregistering the view: the refusal comes before, so the view stays. A sub
   * type linked to the backing type before #11204 is the only way an index can stand below one.
   */
  @Test
  void dropOfAViewIsRefusedBeforeTheViewIsUnregistered() {
    database.command("sql", "CREATE DOCUMENT TYPE Sale");
    database.transaction(() -> database.newDocument("Sale").set("region", "EU").set("amount", 10).save());
    database.command("sql", "CREATE MATERIALIZED VIEW mv AS SELECT region, sum(amount) AS total FROM Sale GROUP BY region");
    linkLegacySubTypeIndexedOnBackingProperty("mv");

    assertThatThrownBy(() -> database.command("sql", "DROP MATERIALIZED VIEW mv"))//
        .isInstanceOf(SchemaException.class)//
        .hasMessageContaining("Cannot drop the type 'mv'")//
        .hasMessageContaining("'Legacy[k]'");

    assertThat(database.getSchema().existsMaterializedView("mv")).isTrue();
    assertThat(database.getSchema().existsType("mv")).isTrue();
    assertThat(database.getSchema().existsIndex("Legacy[k]")).isTrue();

    database.command("sql", "DROP INDEX `Legacy[k]`");
    database.command("sql", "DROP MATERIALIZED VIEW mv");
    assertThat(database.getSchema().existsMaterializedView("mv")).isFalse();
  }

  /** Same as {@link #dropOfAViewIsRefusedBeforeTheViewIsUnregistered()} for DROP CONTINUOUS AGGREGATE. */
  @Test
  void dropOfAContinuousAggregateIsRefusedBeforeTheAggregateIsUnregistered() {
    database.command("sql", "CREATE TIMESERIES TYPE SR TIMESTAMP ts TAGS (sid STRING) FIELDS (v DOUBLE)");
    database.command("sql", "INSERT INTO SR SET ts = 1700000000000, sid = 's1', v = 1.0");
    database.command("sql",
        "CREATE CONTINUOUS AGGREGATE ca AS SELECT sid, ts.timeBucket('1h', ts) AS hour, avg(v) AS av FROM SR GROUP BY sid, hour");
    linkLegacySubTypeIndexedOnBackingProperty("ca");

    assertThatThrownBy(() -> database.command("sql", "DROP CONTINUOUS AGGREGATE ca"))//
        .isInstanceOf(SchemaException.class)//
        .hasMessageContaining("Cannot drop the type 'ca'")//
        .hasMessageContaining("'Legacy[k]'");

    assertThat(database.getSchema().existsContinuousAggregate("ca")).isTrue();
    assertThat(database.getSchema().existsType("ca")).isTrue();

    database.command("sql", "DROP INDEX `Legacy[k]`");
    database.command("sql", "DROP CONTINUOUS AGGREGATE ca");
    assertThat(database.getSchema().existsContinuousAggregate("ca")).isFalse();
  }

  /** A schema written before #11204: Legacy extends the backing type, which alone declares k, and Legacy indexes k. */
  private void linkLegacySubTypeIndexedOnBackingProperty(final String backingTypeName) {
    final LocalDocumentType backing = (LocalDocumentType) database.getSchema().getType(backingTypeName);
    backing.createProperty("k", Type.INTEGER);
    final LocalDocumentType legacy = (LocalDocumentType) database.getSchema().createDocumentType("Legacy");
    // the re-establishing linkage of the schema load, which a database written before the check went through
    legacy.addSuperType(backing, false);
    legacy.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "k");
  }

  private void assertUnchanged() {
    final Schema schema = database.getSchema();
    assertThat(schema.existsType("P")).isTrue();
    assertThat(schema.getType("P").existsProperty("k")).isTrue();
    assertThat(schema.getType("C").getSuperTypes()).extracting(DocumentType::getName).containsExactly("P");
    assertThat(schema.existsIndex("C[k]")).isTrue();
    assertThat(database.countType("C", true)).isEqualTo(1);
  }
}
