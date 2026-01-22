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
import com.arcadedb.exception.TransactionException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NullValuesIndexTest extends TestHelper {
  private static final int    TOT       = 10;
  private static final String TYPE_NAME = "V";
  private static final int    PAGE_SIZE = 20000;

  @Test
  void nullStrategyError() {
    assertThat(database.getSchema().existsType(TYPE_NAME)).isFalse();

    final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(3).create();
    type.createProperty("id", Integer.class);
    type.createProperty("name", String.class);
    final TypeIndex indexes = database.getSchema()
        .buildTypeIndex(TYPE_NAME, new String[] { "id" })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withUnique(true)
        .withPageSize(PAGE_SIZE)
        .create();
    database.getSchema()
        .buildTypeIndex(TYPE_NAME, new String[] { "name" })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withUnique(false)
        .withPageSize(PAGE_SIZE)
        .withNullStrategy(LSMTreeIndexAbstract.NULL_STRATEGY.ERROR)
        .create();

    assertThatThrownBy(() -> database.transaction(() -> {
      for (int i = 0; i < TOT; ++i) {
        final MutableDocument v = database.newDocument(TYPE_NAME);
        v.set("id", i);
        v.set("name", "Jay");
        v.set("surname", "Miner");
        v.save();
      }

      final MutableDocument v = database.newDocument(TYPE_NAME);
      v.set("id", TOT);
      v.save();

      database.commit();
      database.begin();

      for (final IndexInternal index : indexes.getIndexesOnBuckets()) {
        assertThat(index.getStats().get("pages")).isGreaterThan(1);
      }
    })).isInstanceOf(TransactionException.class)
        .cause()
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Indexed key V[name] cannot be NULL");
  }

  @Test
  void nullStrategySkip() {
    assertThat(database.getSchema().existsType(TYPE_NAME)).isFalse();

    final DocumentType type = database.getSchema()
        .buildDocumentType()
        .withName(TYPE_NAME)
        .withTotalBuckets(3)
        .create();
    type.createProperty("id", Integer.class);
    type.createProperty("name", String.class);
    database.getSchema()
        .buildTypeIndex(TYPE_NAME, new String[] { "id" })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withUnique(true)
        .withPageSize(PAGE_SIZE)
        .create();
    database.getSchema()
        .buildTypeIndex(TYPE_NAME, new String[] { "name" })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withUnique(false)
        .withPageSize(PAGE_SIZE)
        .withNullStrategy(LSMTreeIndexAbstract.NULL_STRATEGY.SKIP)
        .create();

    database.transaction(() -> {
      for (int i = 0; i < TOT; ++i) {
        final MutableDocument v = database.newDocument(TYPE_NAME);
        v.set("id", i);
        v.set("name", "Jay");
        v.set("surname", "Miner");
        v.save();
      }

      final MutableDocument v = database.newDocument(TYPE_NAME);
      v.set("id", TOT);
      v.save();

      database.commit();
      database.begin();
    });

    database.transaction(() -> assertThat(database.countType(TYPE_NAME, true)).isEqualTo(TOT + 1));

    database.close();
    database = factory.open();

    // TRY AGAIN WITH A RE-OPEN DATABASE
    database.transaction(() -> {
      assertThat(database.getSchema().existsType(TYPE_NAME)).isTrue();

      assertThat(database.getSchema().existsType(TYPE_NAME)).isTrue();

      for (int i = TOT + 2; i < TOT + TOT; ++i) {
        final MutableDocument v = database.newDocument(TYPE_NAME);
        v.set("id", i);
        v.set("name", "Jay" + i);
        v.set("surname", "Miner");
        v.save();
      }

      final MutableDocument v = database.newDocument(TYPE_NAME);
      v.set("id", TOT + TOT + 1);
      v.save();

      database.commit();
      database.begin();
    });
    database.transaction(() -> assertThat(database.countType(TYPE_NAME, true)).isEqualTo(TOT + TOT));
  }

  @Test
  void nullStrategySkipUnique() {
    assertThat(database.getSchema().existsType(TYPE_NAME)).isFalse();

    assertThat(database.getSchema().existsType(TYPE_NAME)).isFalse();

    final VertexType type = database.getSchema().buildVertexType().withName(TYPE_NAME).withTotalBuckets(3).create();
    type.createProperty("id", Integer.class);
    type.createProperty("absent", String.class);
    database.getSchema()
        .buildTypeIndex(TYPE_NAME, new String[] { "id" })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withUnique(true)
        .withPageSize(PAGE_SIZE)
        .create();
    database.getSchema()
        .buildTypeIndex(TYPE_NAME, new String[] { "absent" })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withUnique(true)
        .withPageSize(PAGE_SIZE)
        .withNullStrategy(LSMTreeIndexAbstract.NULL_STRATEGY.SKIP)
        .create();

    database.transaction(() -> {
      for (int i = 0; i < TOT; ++i) {
        final MutableVertex v = database.newVertex(TYPE_NAME);
        v.set("id", i);
        v.set("name", "Jay");
        v.set("surname", "Miner");
        v.save();
      }

      database.commit();
      database.begin();
    });

    database.transaction(() -> assertThat(database.countType(TYPE_NAME, true)).isEqualTo(TOT));

    database.close();
    database = factory.open();

    // TRY AGAIN WITH A RE-OPEN DATABASE
    database.transaction(() -> {
      assertThat(database.getSchema().existsType(TYPE_NAME)).isTrue();

      for (int i = TOT; i < TOT + TOT; ++i) {
        final MutableVertex v = database.newVertex(TYPE_NAME);
        v.set("id", i);
        v.set("name", "Jay" + i);
        v.set("surname", "Miner");
        v.save();
      }

      database.commit();
      database.begin();
    });

    database.transaction(() -> assertThat(database.countType(TYPE_NAME, true)).isEqualTo(TOT + TOT));
  }

  @Test
  void nullStrategyIndex_basicInsertAndLookup() {
    assertThat(database.getSchema().existsType(TYPE_NAME)).isFalse();

    final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(3).create();
    type.createProperty("id", Integer.class);
    type.createProperty("name", String.class);
    database.getSchema()
        .buildTypeIndex(TYPE_NAME, new String[] { "id" })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withUnique(true)
        .withPageSize(PAGE_SIZE)
        .create();
    final TypeIndex nameIndex = database.getSchema()
        .buildTypeIndex(TYPE_NAME, new String[] { "name" })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withUnique(false)
        .withPageSize(PAGE_SIZE)
        .withNullStrategy(LSMTreeIndexAbstract.NULL_STRATEGY.INDEX)
        .create();

    database.transaction(() -> {
      // Insert documents with non-null names
      for (int i = 0; i < TOT; ++i) {
        final MutableDocument v = database.newDocument(TYPE_NAME);
        v.set("id", i);
        v.set("name", "Jay" + i);
        v.save();
      }

      // Insert documents with null names
      for (int i = TOT; i < TOT + 3; ++i) {
        final MutableDocument v = database.newDocument(TYPE_NAME);
        v.set("id", i);
        // name is null
        v.save();
      }
    });

    // Verify total count
    database.transaction(() -> assertThat(database.countType(TYPE_NAME, true)).isEqualTo(TOT + 3));

    // Verify we can look up NULL values via the index
    database.transaction(() -> {
      final IndexCursor cursor = nameIndex.get(new Object[] { null });
      int count = 0;
      while (cursor.hasNext()) {
        cursor.next();
        count++;
      }
      assertThat(count).isEqualTo(3);
    });
  }

  @Test
  void nullStrategyIndex_sqlIsNullQuery() {
    assertThat(database.getSchema().existsType(TYPE_NAME)).isFalse();

    final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(3).create();
    type.createProperty("id", Integer.class);
    type.createProperty("name", String.class);
    database.getSchema()
        .buildTypeIndex(TYPE_NAME, new String[] { "id" })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withUnique(true)
        .withPageSize(PAGE_SIZE)
        .create();
    database.getSchema()
        .buildTypeIndex(TYPE_NAME, new String[] { "name" })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withUnique(false)
        .withPageSize(PAGE_SIZE)
        .withNullStrategy(LSMTreeIndexAbstract.NULL_STRATEGY.INDEX)
        .create();

    database.transaction(() -> {
      // Insert documents with non-null names
      for (int i = 0; i < TOT; ++i) {
        final MutableDocument v = database.newDocument(TYPE_NAME);
        v.set("id", i);
        v.set("name", "Jay" + i);
        v.save();
      }

      // Insert documents with null names
      for (int i = TOT; i < TOT + 5; ++i) {
        final MutableDocument v = database.newDocument(TYPE_NAME);
        v.set("id", i);
        // name is null
        v.save();
      }
    });

    // Query using SQL IS NULL
    database.transaction(() -> {
      final var result = database.query("SQL", "SELECT FROM " + TYPE_NAME + " WHERE name IS NULL");
      int count = 0;
      while (result.hasNext()) {
        result.next();
        count++;
      }
      assertThat(count).isEqualTo(5);
    });
  }

  @Test
  void nullStrategyIndex_uniqueAllowsMultipleNulls() {
    assertThat(database.getSchema().existsType(TYPE_NAME)).isFalse();

    final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(3).create();
    type.createProperty("id", Integer.class);
    type.createProperty("name", String.class);
    database.getSchema()
        .buildTypeIndex(TYPE_NAME, new String[] { "id" })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withUnique(true)
        .withPageSize(PAGE_SIZE)
        .create();
    database.getSchema()
        .buildTypeIndex(TYPE_NAME, new String[] { "name" })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withUnique(true) // UNIQUE index
        .withPageSize(PAGE_SIZE)
        .withNullStrategy(LSMTreeIndexAbstract.NULL_STRATEGY.INDEX)
        .create();

    // Should be able to insert multiple documents with null name (SQL standard: NULL != NULL)
    database.transaction(() -> {
      for (int i = 0; i < 5; ++i) {
        final MutableDocument v = database.newDocument(TYPE_NAME);
        v.set("id", i);
        // name is null
        v.save();
      }
    });

    database.transaction(() -> assertThat(database.countType(TYPE_NAME, true)).isEqualTo(5));
  }

  @Test
  void nullStrategyIndex_persistenceReload() {
    assertThat(database.getSchema().existsType(TYPE_NAME)).isFalse();

    final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(3).create();
    type.createProperty("id", Integer.class);
    type.createProperty("name", String.class);
    database.getSchema()
        .buildTypeIndex(TYPE_NAME, new String[] { "id" })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withUnique(true)
        .withPageSize(PAGE_SIZE)
        .create();
    database.getSchema()
        .buildTypeIndex(TYPE_NAME, new String[] { "name" })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withUnique(false)
        .withPageSize(PAGE_SIZE)
        .withNullStrategy(LSMTreeIndexAbstract.NULL_STRATEGY.INDEX)
        .create();

    database.transaction(() -> {
      // Insert documents with non-null names
      for (int i = 0; i < TOT; ++i) {
        final MutableDocument v = database.newDocument(TYPE_NAME);
        v.set("id", i);
        v.set("name", "Jay" + i);
        v.save();
      }

      // Insert documents with null names
      for (int i = TOT; i < TOT + 3; ++i) {
        final MutableDocument v = database.newDocument(TYPE_NAME);
        v.set("id", i);
        // name is null
        v.save();
      }
    });

    database.close();
    database = factory.open();

    // Verify NULL_STRATEGY persisted
    database.transaction(() -> {
      final Index nameIndex = database.getSchema().getIndexByName(TYPE_NAME + "[name]");
      assertThat(nameIndex.getNullStrategy()).isEqualTo(LSMTreeIndexAbstract.NULL_STRATEGY.INDEX);
    });

    // Verify we can still query NULL values after reopen
    database.transaction(() -> {
      final var result = database.query("SQL", "SELECT FROM " + TYPE_NAME + " WHERE name IS NULL");
      int count = 0;
      while (result.hasNext()) {
        result.next();
        count++;
      }
      assertThat(count).isEqualTo(3);
    });
  }
}
