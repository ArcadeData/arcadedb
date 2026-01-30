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
package com.arcadedb.query.opencypher.optimizer.statistics;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.schema.Schema;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for StatisticsProvider class.
 */
class StatisticsProviderTest {
  private static final String DB_PATH = "./target/teststatistics";
  private Database database;
  private StatisticsProvider statisticsProvider;

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    database = new DatabaseFactory(DB_PATH).create();
    statisticsProvider = new StatisticsProvider((DatabaseInternal) database);
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
    }
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @Test
  void collectStatisticsForVertexType() {
    // Create type and insert records
    database.getSchema().getOrCreateVertexType("Person");
    database.transaction(() -> {
      for (int i = 0; i < 100; i++) {
        database.newVertex("Person").set("id", i).save();
      }
    });

    // Collect statistics
    statisticsProvider.collectStatistics(Arrays.asList("Person"));

    // Verify statistics
    final TypeStatistics stats = statisticsProvider.getTypeStatistics("Person");
    assertThat(stats).isNotNull();
    assertThat(stats.getTypeName()).isEqualTo("Person");
    assertThat(stats.getRecordCount()).isEqualTo(100);
    assertThat(stats.isVertexType()).isTrue();
  }

  @Test
  void collectStatisticsForEdgeType() {
    // Create types
    database.getSchema().getOrCreateVertexType("Person");
    database.getSchema().getOrCreateEdgeType("KNOWS");

    database.transaction(() -> {
      final var v1 = database.newVertex("Person").save();
      final var v2 = database.newVertex("Person").save();
      v1.newEdge("KNOWS", v2, true, (Object[]) null);
    });

    // Collect statistics
    statisticsProvider.collectStatistics(Arrays.asList("KNOWS"));

    // Verify statistics
    final TypeStatistics stats = statisticsProvider.getTypeStatistics("KNOWS");
    assertThat(stats).isNotNull();
    assertThat(stats.getTypeName()).isEqualTo("KNOWS");
    assertThat(stats.getRecordCount()).isEqualTo(1);
    assertThat(stats.isVertexType()).isFalse();
  }

  @Test
  void collectIndexStatistics() {
    // Create type with index
    final var personType = database.getSchema().getOrCreateVertexType("Person");
    personType.createProperty("id", Integer.class);
    personType.createProperty("name", String.class);
    personType.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
    personType.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "name");

    // Collect statistics
    statisticsProvider.collectStatistics(Arrays.asList("Person"));

    // Verify index statistics
    final List<IndexStatistics> indexes = statisticsProvider.getIndexesForType("Person");
    assertThat(indexes).hasSize(2);

    // Find id index
    final IndexStatistics idIndex = indexes.stream()
        .filter(idx -> idx.getPropertyNames().contains("id"))
        .findFirst()
        .orElse(null);
    assertThat(idIndex).isNotNull();
    assertThat(idIndex.isUnique()).isTrue();
    assertThat(idIndex.getPropertyNames()).containsExactly("id");

    // Find name index
    final IndexStatistics nameIndex = indexes.stream()
        .filter(idx -> idx.getPropertyNames().contains("name"))
        .findFirst()
        .orElse(null);
    assertThat(nameIndex).isNotNull();
    assertThat(nameIndex.isUnique()).isFalse();
    assertThat(nameIndex.getPropertyNames()).containsExactly("name");
  }

  @Test
  void findIndexForProperty() {
    // Create type with indexes
    final var personType = database.getSchema().getOrCreateVertexType("Person");
    personType.createProperty("id", Integer.class);
    personType.createProperty("name", String.class);
    personType.createProperty("age", Integer.class);
    personType.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");
    personType.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "name", "age");

    statisticsProvider.collectStatistics(Arrays.asList("Person"));

    // Find index for "id" property
    final IndexStatistics idIndex = statisticsProvider.findIndexForProperty("Person", "id");
    assertThat(idIndex).isNotNull();
    assertThat(idIndex.isUnique()).isTrue();
    assertThat(idIndex.canBeUsedForProperty("id")).isTrue();

    // Find index for "name" property (composite index)
    final IndexStatistics nameIndex = statisticsProvider.findIndexForProperty("Person", "name");
    assertThat(nameIndex).isNotNull();
    assertThat(nameIndex.canBeUsedForProperty("name")).isTrue();

    // Cannot find index for "age" (second property in composite index)
    final IndexStatistics ageIndex = statisticsProvider.findIndexForProperty("Person", "age");
    assertThat(ageIndex).isNull();
  }

  @Test
  void hasIndexForProperty() {
    // Create type with index
    final var personType = database.getSchema().getOrCreateVertexType("Person");
    personType.createProperty("email", String.class);
    personType.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "email");

    statisticsProvider.collectStatistics(Arrays.asList("Person"));

    assertThat(statisticsProvider.hasIndexForProperty("Person", "email")).isTrue();
    assertThat(statisticsProvider.hasIndexForProperty("Person", "nonexistent")).isFalse();
  }

  @Test
  void getCardinality() {
    database.getSchema().getOrCreateVertexType("Person");
    database.transaction(() -> {
      for (int i = 0; i < 50; i++) {
        database.newVertex("Person").save();
      }
    });

    statisticsProvider.collectStatistics(Arrays.asList("Person"));

    assertThat(statisticsProvider.getCardinality("Person")).isEqualTo(50);
    assertThat(statisticsProvider.getCardinality("Nonexistent")).isEqualTo(0);
  }

  @Test
  void clear() {
    database.getSchema().getOrCreateVertexType("Person");
    statisticsProvider.collectStatistics(Arrays.asList("Person"));

    assertThat(statisticsProvider.getTypeStatistics("Person")).isNotNull();

    statisticsProvider.clear();

    assertThat(statisticsProvider.getTypeStatistics("Person")).isNull();
  }

  @Test
  void preferUniqueIndexOverNonUnique() {
    // Create type with unique and non-unique indexes on different properties
    final var personType = database.getSchema().getOrCreateVertexType("Person");
    personType.createProperty("id", Integer.class);
    personType.createProperty("email", String.class);
    personType.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "email"); // Non-unique
    personType.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");     // Unique

    statisticsProvider.collectStatistics(Arrays.asList("Person"));

    // Should return unique index for id
    final IndexStatistics idIndex = statisticsProvider.findIndexForProperty("Person", "id");
    assertThat(idIndex).isNotNull();
    assertThat(idIndex.isUnique()).isTrue();

    // Should return non-unique index for email
    final IndexStatistics emailIndex = statisticsProvider.findIndexForProperty("Person", "email");
    assertThat(emailIndex).isNotNull();
    assertThat(emailIndex.isUnique()).isFalse();
  }
}
