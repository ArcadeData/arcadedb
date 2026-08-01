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
package com.arcadedb.query.sql.executor;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.index.lsm.LSMTreeIndex;
import com.arcadedb.index.lsm.LSMTreeIndexCompacted;
import com.arcadedb.query.sql.parser.DeleteStatement;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #5662, item 2: {@code DeleteFromIndexStep} had no {@code close()} at all, so the cursor its {@code init()} opens was
 * never released. A {@code DELETE FROM INDEX:...} that ends before the scan does - the caller stops driving the result
 * set, or the statement fails - left an {@code LSMTreeIndexUnderlyingCompactedSeriesCursor} registered with its file,
 * and {@code LSMTreeIndex.dropRetiredCompactedIndexes} then refused to drop that file for the lifetime of the database.
 * <p>
 * The step is driven directly rather than through {@code database.command()} because the {@code CountStep} the planner
 * puts after it drains the scan to exhaustion, which releases the series cursors on its own and would make the
 * assertion hold with or without the fix.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class DeleteFromIndexStepCloseTest extends TestHelper {
  private static final String TYPE_NAME = "IndexDelete";
  private static final int    RECORDS   = 600;

  @Override
  public void beforeTest() {
    GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE.setValue(0);
  }

  @Test
  void closingTheStepReleasesTheIndexCursorItOpened() {
    final LSMTreeIndexCompacted compacted = createCompactedFixture();

    database.transaction(() -> {
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);

      final DeleteStatement statement = (DeleteStatement) ((DatabaseInternal) database).getStatementCache()
          .get("DELETE FROM INDEX:`" + bucketIndex().getName() + "` WHERE key > 1");

      final DeleteFromIndexStep step = deleteFromIndexStep(statement.createExecutionPlan(context));

      // one pull is enough to open the scan; nothing is removed yet, the entries are only positioned
      step.syncPull(context, 1);
      assertThat(compacted.countActiveCursors()).as("precondition: the scan must be holding a series cursor").isPositive();

      step.close();

      assertThat(compacted.countActiveCursors()).as("DeleteFromIndexStep.close() must release its index cursor").isZero();

      // idempotent: the plan closes every step, and a step may be closed again by a caller
      step.close();
      assertThat(compacted.countActiveCursors()).isZero();
    });
  }

  /**
   * The same guarantee on the equality path, which reaches the cursor through {@code createCursor()} rather than
   * through the range machinery.
   */
  @Test
  void anEqualityDeleteAlsoReleasesItsCursor() {
    final LSMTreeIndexCompacted compacted = createCompactedFixture();

    database.transaction(() -> {
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);

      final DeleteStatement statement = (DeleteStatement) ((DatabaseInternal) database).getStatementCache()
          .get("DELETE FROM INDEX:`" + bucketIndex().getName() + "` WHERE key >= 400");

      final DeleteFromIndexStep step = deleteFromIndexStep(statement.createExecutionPlan(context));
      step.syncPull(context, 1);
      assertThat(compacted.countActiveCursors()).as("precondition: the equality path opened a cursor too").isPositive();

      step.close();

      assertThat(compacted.countActiveCursors()).as("no cursor may be left behind by the equality path").isZero();
    });
  }

  private static DeleteFromIndexStep deleteFromIndexStep(final DeleteExecutionPlan plan) {
    for (final ExecutionStep step : plan.getSteps())
      if (step instanceof final DeleteFromIndexStep deleteFromIndex)
        return deleteFromIndex;
    throw new IllegalStateException("the plan does not contain a DeleteFromIndexStep: " + plan.prettyPrint(0, 2));
  }

  private LSMTreeIndexCompacted createCompactedFixture() {
    database.getConfiguration().setValue(GlobalConfiguration.INDEX_COMPACTION_MIN_PAGES_SCHEDULE, 0);

    final DocumentType type = database.getSchema().buildDocumentType().withName(TYPE_NAME).withTotalBuckets(1).create();
    type.createProperty("value", Integer.class);
    // a small page size so the compacted output spans several pages, i.e. a real series a cursor walks lazily
    database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "value" }).withType(Schema.INDEX_TYPE.LSM_TREE)
        .withUnique(true).withPageSize(1024).create();

    database.transaction(() -> {
      for (int i = 0; i < RECORDS; i++)
        database.newDocument(TYPE_NAME).set("value", i).save();
    });

    final LSMTreeIndex index = bucketIndex();
    try {
      if (index.scheduleCompaction())
        index.compact();
    } catch (final IOException | InterruptedException e) {
      throw new IllegalStateException("cannot compact the fixture index", e);
    }

    final LSMTreeIndexCompacted compacted = index.getMutableIndex().getSubIndex();
    assertThat(compacted).as("the fixture must produce a compacted sub-index, otherwise no series cursor is ever opened")
        .isNotNull();
    assertThat(compacted.countActiveCursors()).isZero();
    return compacted;
  }

  private LSMTreeIndex bucketIndex() {
    return (LSMTreeIndex) database.getSchema().getType(TYPE_NAME).getAllIndexes(false).iterator().next()
        .getIndexesOnBuckets()[0];
  }
}
