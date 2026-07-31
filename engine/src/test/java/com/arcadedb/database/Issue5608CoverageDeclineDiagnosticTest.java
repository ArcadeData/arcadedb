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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.exception.SchemaException;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #5608 (follow-up 4 of #5596): {@code TransactionContext.reportCoverageDecline} is a pure diagnostic, and it
 * runs from INSIDE the commit loop's {@code catch (ConcurrentModificationException e)}, one statement before
 * {@code throw e}. Anything it throws therefore replaces the conflict the caller is propagating.
 * <p>
 * That was not merely a misleading message. The diagnostic reaches {@code edgeSegmentPageKey}, whose bucket lookup
 * raises {@code SchemaException} for an unknown bucket id - the {@code bucket == null} branch below it never fires,
 * because {@code LocalSchema.getBucketById(id)} throws before returning null. A {@code SchemaException} escaping there
 * is NOT a {@code NeedRetryException}: the commit's generic {@code catch (Exception)} would have wrapped it into a
 * plain {@code TransactionException}, turning a conflict the caller would have retried into a hard failure.
 * <p>
 * The diagnostic is now total by construction, which is what this test pins: the setup is verified to be one where
 * resolving the segment's page key really does raise, and the report still returns quietly.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5608CoverageDeclineDiagnosticTest extends TestHelper {

  @Test
  void aFailingDiagnosticNeverReplacesTheConflictBeingReported() throws Exception {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Diag", 1);
      database.newDocument("Diag").set("tag", "value").save();
    });

    final LocalBucket bucket = (LocalBucket) database.getSchema().getBucketById(
        database.getSchema().getType("Diag").getBuckets(false).getFirst().getFileId());

    database.begin();
    try {
      final TransactionContext tx = ((DatabaseInternal) database).getTransaction();

      // A tracked edge append whose segment claims a bucket id that does not exist. trackEdgeAppend itself never
      // resolves the page key (that is what keeps the append hot path allocation-free), so the transaction is left
      // holding a tracked segment nobody can locate - the state the diagnostic then walks into.
      final RID unresolvableSegment = new RID(30_000, 0);
      tx.trackEdgeAppend(unresolvableSegment, new RID(30_000, 1), new RID(30_000, 2));

      // The setup is genuinely hostile, and NOT with a retryable exception: pin it, so the test cannot quietly
      // degrade into asserting that a no-op does not throw.
      assertThatThrownBy(() -> tx.poisonEdgeAppendPage(unresolvableSegment))
          .as("resolving the segment's page key must really raise").isInstanceOf(SchemaException.class);

      final MutablePage page = tx.getPageToModify(new PageId(database, bucket.getFileId(), 0), bucket.getPageSize(), false);

      final Method reportCoverageDecline = TransactionContext.class.getDeclaredMethod("reportCoverageDecline",
          MutablePage.class);
      reportCoverageDecline.setAccessible(true);

      assertThatCode(() -> reportCoverageDecline.invoke(tx, page))
          .as("the diagnostic must never alter the exception the commit loop is propagating").doesNotThrowAnyException();
    } finally {
      database.rollback();
    }

    // The database is untouched by the probe above.
    database.transaction(() -> assertThat(database.countType("Diag", false)).isEqualTo(1));
  }
}
