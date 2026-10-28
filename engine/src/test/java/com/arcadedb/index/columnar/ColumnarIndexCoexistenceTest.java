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
package com.arcadedb.index.columnar;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A COLUMNAR index answers facets and a key index answers filters, so both can sit on one property (spec 7.5): each family has its
 * own slot on the type, its own default name and its own IF NOT EXISTS.
 */
class ColumnarIndexCoexistenceTest extends TestHelper {
  private static final String KEY      = "Asset[brand]";
  private static final String COLUMNAR = "Asset[brand]_columnar";

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE DOCUMENT TYPE Asset");
    database.command("sql", "CREATE PROPERTY Asset.brand STRING");
  }

  @Test
  void columnarThenKeyIndex() {
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");
    database.command("sql", "CREATE INDEX ON Asset (brand) NOTUNIQUE");
    assertBoth();
  }

  @Test
  void keyThenColumnarIndex() {
    database.command("sql", "CREATE INDEX ON Asset (brand) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");
    assertBoth();
  }

  @Test
  void plannerAndLookupStillUseTheKeyIndex() {
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");
    database.command("sql", "CREATE INDEX ON Asset (brand) NOTUNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.newDocument("Asset").set("brand", i % 2 == 0 ? "x" : "y").save();
    });

    try (final ResultSet rs = database.query("sql", "SELECT FROM Asset WHERE brand = 'x'")) {
      assertThat(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2)).contains("FETCH FROM INDEX " + KEY);
      assertThat(rs.stream().count()).isEqualTo(5);
    }
    try (final IndexCursor cursor = database.lookupByKey("Asset", new String[] { "brand" }, new Object[] { "x" })) {
      int found = 0;
      while (cursor.hasNext()) {
        cursor.next();
        found++;
      }
      assertThat(found).isEqualTo(5);
    }
  }

  @Test
  void droppingOneLeavesTheOther() {
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");
    database.command("sql", "CREATE INDEX ON Asset (brand) NOTUNIQUE");

    database.command("sql", "DROP INDEX `" + COLUMNAR + "`");
    assertThat(database.getSchema().existsIndex(COLUMNAR)).isFalse();
    assertThat(database.getSchema().getIndexByName(KEY).getType()).isEqualTo(Schema.INDEX_TYPE.LSM_TREE);
    assertThat(type().getColumnarIndex("brand")).isNull();

    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");
    database.command("sql", "DROP INDEX `" + KEY + "`");
    assertThat(database.getSchema().existsIndex(KEY)).isFalse();
    assertThat(database.getSchema().getIndexByName(COLUMNAR).getType()).isEqualTo(Schema.INDEX_TYPE.COLUMNAR);
    assertThat(type().getIndexByProperties("brand")).isNull();

    reopenDatabase();
    assertThat(database.getSchema().existsIndex(KEY)).isFalse();
    assertThat(database.getSchema().getIndexByName(COLUMNAR).getType()).isEqualTo(Schema.INDEX_TYPE.COLUMNAR);
  }

  @Test
  void bothSurviveReopen() {
    database.command("sql", "CREATE INDEX ON Asset (brand) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");

    reopenDatabase();
    assertBoth();
  }

  @Test
  void uniqueConstraintOverAColumnarPropertyBySql() {
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");
    database.command("sql", "CREATE INDEX ON Asset (brand) UNIQUE");

    database.transaction(() -> database.newDocument("Asset").set("brand", "x").save());
    assertThatThrownBy(() -> database.transaction(() -> database.newDocument("Asset").set("brand", "x").save()))
        .isInstanceOf(DuplicatedKeyException.class);
    assertThat(database.getSchema().getIndexByName(COLUMNAR).getType()).isEqualTo(Schema.INDEX_TYPE.COLUMNAR);
  }

  @Test
  void uniqueConstraintOverAColumnarPropertyByCypher() {
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE PROPERTY Person.email STRING");
    database.command("sql", "CREATE INDEX ON Person (email) COLUMNAR");
    database.command("opencypher", "CREATE CONSTRAINT person_email FOR (p:Person) REQUIRE p.email IS UNIQUE");

    database.transaction(() -> database.command("opencypher", "CREATE (:Person {email: 'a@b.c'})"));
    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher", "CREATE (:Person {email: 'a@b.c'})")))
        .isInstanceOf(DuplicatedKeyException.class);
    assertThat(database.getSchema().getIndexByName("Person[email]_columnar").getType()).isEqualTo(Schema.INDEX_TYPE.COLUMNAR);
    assertThat(database.getSchema().getType("Person").getIndexByProperties("email").isUnique()).isTrue();
  }

  @Test
  void ifNotExistsIsPerFamily() {
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");
    // a COLUMNAR index does not answer a request for a key index, and the other way round
    database.command("sql", "CREATE INDEX IF NOT EXISTS ON Asset (brand) NOTUNIQUE");
    assertBoth();

    database.command("sql", "CREATE INDEX IF NOT EXISTS ON Asset (brand) NOTUNIQUE");
    database.command("sql", "CREATE INDEX IF NOT EXISTS ON Asset (brand) COLUMNAR");
    assertBoth();

    database.getSchema().getType("Asset").getOrCreateTypeIndex(Schema.INDEX_TYPE.COLUMNAR, false, "brand");
    database.getSchema().getType("Asset").getOrCreateTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "brand");
    assertBoth();
  }

  @Test
  void aManualNameIsGloballyUnique() {
    database.command("sql", "CREATE INDEX shared ON Asset (brand) NOTUNIQUE");
    assertThatThrownBy(() -> database.command("sql", "CREATE INDEX shared ON Asset (brand) COLUMNAR"))
        .hasMessageContaining("shared");
    assertThat(database.getSchema().getIndexByName("shared").getType()).isEqualTo(Schema.INDEX_TYPE.LSM_TREE);
    assertThat(type().getColumnarIndex("brand")).isNull();
  }

  @Test
  void dropPropertyForceDropsBoth() {
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");
    database.command("sql", "CREATE INDEX ON Asset (brand) NOTUNIQUE");
    assertThat(type().getIndexesByProperties("brand")).hasSize(2);

    assertThatThrownBy(() -> database.command("sql", "DROP PROPERTY Asset.brand")).hasMessageContaining(KEY);
    database.command("sql", "DROP PROPERTY Asset.brand FORCE");
    assertThat(database.getSchema().existsIndex(KEY)).isFalse();
    assertThat(database.getSchema().existsIndex(COLUMNAR)).isFalse();
  }

  @Test
  void aSupertypeColumnarIndexIsFoundFromTheSubtype() {
    database.command("sql", "CREATE DOCUMENT TYPE Image EXTENDS Asset");
    database.command("sql", "CREATE INDEX ON Asset (brand) COLUMNAR");

    final DocumentType image = database.getSchema().getType("Image");
    assertThat(image.getColumnarIndex("brand")).isNull();
    assertThat(image.getPolymorphicColumnarIndex("brand").getName()).isEqualTo(COLUMNAR);
    assertThat(image.getPolymorphicIndexByProperties("brand")).isNull();
  }

  @Test
  void partitionedTypeWithAUniqueAndAColumnarIndexOnThePartitionKey() {
    database.command("sql", "CREATE DOCUMENT TYPE Tenant BUCKETS 4");
    database.command("sql", "CREATE PROPERTY Tenant.id STRING");
    database.command("sql", "CREATE INDEX ON Tenant (id) UNIQUE");
    database.command("sql", "ALTER TYPE Tenant BucketSelectionStrategy `partitioned('id')`");
    database.command("sql", "CREATE INDEX ON Tenant (id) COLUMNAR");
    ColumnarTestHooks.INDEX_WRITES.set(0);

    // new values: the commit-time uniqueness check reads the pruned bucket index, which must not be the COLUMNAR one
    database.transaction(() -> {
      for (int i = 0; i < 20; i++)
        database.newDocument("Tenant").set("id", "t" + i).save();
    });

    for (int i = 0; i < 20; i++) {
      try (final IndexCursor cursor = database.lookupByKey("Tenant", new String[] { "id" }, new Object[] { "t" + i })) {
        assertThat(cursor.hasNext()).as("t" + i).isTrue();
        cursor.next();
        assertThat(cursor.hasNext()).isFalse();
      }
      try (final ResultSet rs = database.query("sql", "SELECT FROM Tenant WHERE id = ?", "t" + i)) {
        assertThat(rs.stream().count()).isEqualTo(1);
      }
    }

    assertThatThrownBy(() -> database.transaction(() -> database.newDocument("Tenant").set("id", "t3").save()))
        .isInstanceOf(DuplicatedKeyException.class);

    database.transaction(() -> database.command("sql", "DELETE FROM Tenant WHERE id = 't3'"));
    try (final IndexCursor cursor = database.lookupByKey("Tenant", new String[] { "id" }, new Object[] { "t3" })) {
      assertThat(cursor.hasNext()).isFalse();
    }
    database.transaction(() -> database.newDocument("Tenant").set("id", "t3").save());
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS cnt FROM Tenant")) {
      assertThat(rs.next().<Number>getProperty("cnt").longValue()).isEqualTo(20L);
    }

    final TypeIndex unique = database.getSchema().getType("Tenant").getIndexByProperties("id");
    assertThat(unique.get(new Object[] { "t5" }).hasNext()).isTrue();
    assertThat(unique.get(new Object[] { "missing" }).hasNext()).isFalse();
    assertThat(ColumnarTestHooks.INDEX_WRITES.get()).as("no write fanned out to the COLUMNAR index").isZero();
  }

  private void assertBoth() {
    final DocumentType asset = type();
    final TypeIndex key = asset.getIndexByProperties("brand");
    final TypeIndex columnar = asset.getColumnarIndex("brand");
    assertThat(key.getName()).isEqualTo(KEY);
    assertThat(key.getType()).isEqualTo(Schema.INDEX_TYPE.LSM_TREE);
    assertThat(columnar.getName()).isEqualTo(COLUMNAR);
    assertThat(columnar.getType()).isEqualTo(Schema.INDEX_TYPE.COLUMNAR);
    assertThat(asset.getIndexesByProperties("brand")).containsExactlyInAnyOrder(key, columnar);
    assertThat(asset.getAllIndexes(false)).containsExactlyInAnyOrder(key, columnar);
    assertThat(database.getSchema().getIndexByName(KEY)).isSameAs(key);
    assertThat(database.getSchema().getIndexByName(COLUMNAR)).isSameAs(columnar);
    assertThat(key.isReadyForQueries()).isTrue();
    assertThat(columnar.isReadyForQueries()).isFalse();
    assertThat(columnar.isOnline()).isTrue();
    assertThat(List.of(key.getIndexesOnBuckets())).noneMatch(sub -> sub instanceof ColumnarIndex);
    assertThat(List.of(columnar.getIndexesOnBuckets())).allMatch(sub -> sub instanceof ColumnarIndex);
  }

  private DocumentType type() {
    return database.getSchema().getType("Asset");
  }
}
