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
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexAbstract;
import com.arcadedb.index.lsm.LSMTreeIndexMutable;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.*;


/**
 * Regression test: SELECT with partial key on composite NOTUNIQUE index
 * throws "key is composed of 1 items, while the index defined 2 items" on commit.
 *
 * Reproduces the client's exact scenario:
 * - V -> InteliBase -> InteliVertex -> BusinessObject -> Study
 * - Composite index on InteliVertex(Name, ModifiedOn)
 * - SELECT on Study with WHERE Name = 'xxx' (partial composite key)
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CompositeIndexPartialKeySelectTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      // Reproduce the client's class hierarchy: V -> InteliBase -> InteliVertex -> BusinessObject -> Study
      final VertexType inteliBase = database.getSchema().createVertexType("InteliBase");
      inteliBase.createProperty("ID", Type.STRING);
      inteliBase.createProperty("Name", Type.STRING);

      final VertexType inteliVertex = database.getSchema().createVertexType("InteliVertex");
      inteliVertex.addSuperType(inteliBase);
      inteliVertex.createProperty("CreatedOn", Type.DATETIME);
      inteliVertex.createProperty("ModifiedOn", Type.DATETIME);
      inteliVertex.createProperty("Version", Type.STRING);

      final VertexType businessObject = database.getSchema().createVertexType("BusinessObject");
      businessObject.addSuperType(inteliVertex);
      businessObject.createProperty("_isDeleted", Type.BOOLEAN);
      businessObject.createProperty("_isDisabled", Type.BOOLEAN);

      final VertexType study = database.getSchema().createVertexType("Study");
      study.addSuperType(businessObject);
      study.createProperty("StudyNumber", Type.STRING);
    });

    // Insert data BEFORE creating the index (like the client's scenario)
    database.transaction(() -> {
      for (int i = 0; i < 20; i++) {
        database.newVertex("Study")
            .set("ID", "id-" + i)
            .set("Name", "Study" + i)
            .set("CreatedOn", LocalDateTime.now())
            .set("ModifiedOn", LocalDateTime.now())
            .set("Version", "1.0")
            .set("_isDeleted", false)
            .set("_isDisabled", false)
            .set("StudyNumber", "SN-" + i)
            .save();
      }
      database.newVertex("Study")
          .set("ID", "id-special")
          .set("Name", "2025R2 Low ArcadeDB")
          .set("CreatedOn", LocalDateTime.now())
          .set("ModifiedOn", LocalDateTime.now())
          .set("Version", "1.0")
          .set("_isDeleted", false)
          .set("_isDisabled", false)
          .set("StudyNumber", "SN-special")
          .save();
    });

    // Create composite index AFTER data exists (client's scenario)
    database.transaction(() -> {
      database.getSchema().buildTypeIndex("InteliVertex", new String[] { "Name", "ModifiedOn" })
          .withType(Schema.INDEX_TYPE.LSM_TREE)
          .withUnique(false)
          .withPageSize(LSMTreeIndexAbstract.DEF_PAGE_SIZE)
          .withNullStrategy(LSMTreeIndexAbstract.NULL_STRATEGY.SKIP)
          .create();
    });
  }

  @Test
  void selectWithPartialCompositeKeyOrCondition() {
    // Client's exact query pattern (simplified)
    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT *, @rid, @type, Name, ModifiedOn FROM Study " +
              "WHERE Name = 'Study0' or Name = '2025R2 Low ArcadeDB' " +
              "AND @type = 'Study' AND _isDeleted <> true AND _isDisabled <> true LIMIT -1");

      int count = 0;
      while (result.hasNext()) {
        result.next();
        count++;
      }
      assertThat(count).isGreaterThan(0);
    });
  }

  @Test
  void selectWithSingleConditionOnCompositeIndex() {
    // Simplest case: single equality on first field of composite index
    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT * FROM Study WHERE Name = 'Study0'");

      int count = 0;
      while (result.hasNext()) {
        result.next();
        count++;
      }
      assertThat(count).isEqualTo(1);
    });
  }

  @Test
  void selectWithNullSecondFieldInCompositeIndex() {
    // Insert a record without ModifiedOn (null second index field)
    database.transaction(() -> {
      database.newVertex("Study")
          .set("ID", "id-nodate")
          .set("Name", "StudyNoDate")
          .set("_isDeleted", false)
          .set("_isDisabled", false)
          .save();
    });

    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT * FROM Study WHERE Name = 'StudyNoDate'");

      int count = 0;
      while (result.hasNext()) {
        result.next();
        count++;
      }
      assertThat(count).isEqualTo(1);
    });
  }

  @Test
  void selectViaCommandEndpoint() {
    // Simulate what happens via POST /command - command() instead of query()
    database.transaction(() -> {
      final ResultSet result = database.command("sql",
          "SELECT *, @rid, @type, Name, ModifiedOn FROM Study " +
              "WHERE Name = 'Study0' or Name = '2025R2 Low ArcadeDB' " +
              "AND @type = 'Study' AND _isDeleted <> true AND _isDisabled <> true LIMIT -1");

      int count = 0;
      while (result.hasNext()) {
        result.next();
        count++;
      }
      assertThat(count).isGreaterThan(0);
    });
  }

  @Test
  void selectWithPartialKeyAndModificationInSameTransaction() {
    // Test if a modification + select in the same tx causes issues
    database.transaction(() -> {
      // First modify a record
      database.newVertex("Study")
          .set("ID", "id-new")
          .set("Name", "NewStudy")
          .set("CreatedOn", LocalDateTime.now())
          .set("ModifiedOn", LocalDateTime.now())
          .set("_isDeleted", false)
          .set("_isDisabled", false)
          .save();

      // Then query with partial composite key
      final ResultSet result = database.query("sql",
          "SELECT * FROM Study WHERE Name = 'NewStudy'");

      int count = 0;
      while (result.hasNext()) {
        result.next();
        count++;
      }
      assertThat(count).isEqualTo(1);
    });
  }

  @Test
  void partialKeyRangeOnCompositeIndexAfterCompaction() throws Exception {
    // Regression test for: "key is composed of 1 items, while the index defined 2 items"
    // The bug occurs in LSMTreeIndexCompacted.newIterators() which calls lookupInPage
    // with purpose=1 (retrieve) that rejects partial keys on composite indexes.
    // This only manifests when the index has a compacted sub-index.

    final TypeIndex typeIndex = database.getSchema().getType("InteliVertex")
        .getIndexesByProperties("Name", "ModifiedOn").getFirst();

    // Insert enough data across many transactions to create multiple mutable index pages.
    // Each tx commit creates a new page; need 2+ pages per bucket for compaction.
    for (int batch = 0; batch < 30; batch++) {
      final int b = batch;
      database.transaction(() -> {
        for (int i = 0; i < 500; i++) {
          database.newVertex("Study")
              .set("ID", "id-batch" + b + "-" + i)
              .set("Name", "BatchStudy" + b + "_" + i)
              .set("CreatedOn", LocalDateTime.now())
              .set("ModifiedOn", LocalDateTime.now())
              .set("Version", "1.0")
              .set("_isDeleted", false)
              .set("_isDisabled", false)
              .set("StudyNumber", "SN-batch" + b + "-" + i)
              .save();
        }
      });
    }

    // Force compaction on all bucket indexes
    boolean compacted = false;
    for (final Index bucketIndex : typeIndex.getIndexesOnBuckets()) {
      if (bucketIndex instanceof LSMTreeIndex lsmIndex) {
        final LSMTreeIndexMutable mutableIdx = lsmIndex.getMutableIndex();
        if (mutableIdx.getTotalPages() >= 2) {
          lsmIndex.scheduleCompaction();
          compacted |= lsmIndex.compact();
        }
      }
    }
    assertThat(compacted).as("At least one index bucket should have been compacted").isTrue();

    // Test partial key range query directly on the index (this is what FetchFromIndexStep does
    // for composite indexes when only a prefix of the key is provided in WHERE clause).
    // Before the fix, this would throw IllegalArgumentException from lookupInPage.
    for (final Index bucketIndex : typeIndex.getIndexesOnBuckets()) {
      if (bucketIndex instanceof LSMTreeIndex lsmIndex) {
        // Partial key: only the first component of the composite key (Name, ModifiedOn)
        final Object[] partialKey = new Object[] { "Study0" };
        final IndexCursor cursor = lsmIndex.range(true, partialKey, true, partialKey, true);
        int count = 0;
        while (cursor.hasNext()) {
          cursor.next();
          count++;
        }
        // Study0 was inserted in beginTest() and goes to one specific bucket
      }
    }

    // Also test via SQL query
    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT * FROM Study WHERE Name = 'Study0'");
      int count = 0;
      while (result.hasNext()) {
        result.next();
        count++;
      }
      assertThat(count).isEqualTo(1);
    });
  }
}
