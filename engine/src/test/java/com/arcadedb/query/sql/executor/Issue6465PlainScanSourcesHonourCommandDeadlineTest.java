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

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.index.RangeIndex;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #6465: a WHERE-less aggregation/ORDER BY/DISTINCT over a plain scan drains the whole type
 * server-side with nothing consulting {@code arcadedb.command.timeout}. {@code ScanWithFilterStep} and
 * {@code FilterStep} (used when a WHERE exists) already guard their loop with
 * {@link WorkGuard#forCommandDeadline(CommandContext)}; the plain scan sources - reached whenever there
 * is no WHERE to build a filtered step from - carried no such guard, so a blocking consumer downstream
 * (aggregation, {@code ORDER BY}, {@code DISTINCT}) could run past the configured deadline unbounded by
 * anything but the data size.
 * <p>
 * Rather than reproducing that with a dataset large enough to outrun a real deadline under wall-clock
 * timing (flaky across machines), each test here pins an already-expired deadline directly on the
 * {@link CommandContext} - the same technique
 * {@code Issue6266CommandTimeoutCoverageTest#aPinnedDeadlineIsHonouredWhateverItsValue} uses - and drains
 * the guarded step directly, so the assertion is deterministic: the very first record the step tries to
 * hand back must instead raise a {@link TimeoutException}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6465PlainScanSourcesHonourCommandDeadlineTest {

  @Test
  void fetchFromClusterExecutionStepHonoursAnExpiredDeadline() throws Exception {
    TestHelper.executeInNewDatabase(db -> {
      final DocumentType type = db.getSchema().createDocumentType("PlainScanType");
      type.createProperty("value", Long.class);
      db.transaction(() -> {
        for (int i = 0; i < 10; i++)
          db.newDocument("PlainScanType").set("value", (long) i).save();
      });

      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(db);
      context.setCommandDeadline(System.currentTimeMillis() - 1, "an already expired deadline");

      final FetchFromClusterExecutionStep step = new FetchFromClusterExecutionStep(type.getFirstBucketId(), context);
      final ResultSet result = step.syncPull(context, 100);

      assertThatThrownBy(() -> drain(result))
          .as("a plain bucket scan with no WHERE must still stop at the command deadline")
          .isInstanceOf(TimeoutException.class)
          .hasMessageContaining("an already expired deadline");
    });
  }

  @Test
  void fetchFromIndexStepHonoursAnExpiredDeadline() throws Exception {
    TestHelper.executeInNewDatabase(db -> {
      final DocumentType type = db.getSchema().createDocumentType("PlainScanIndexType");
      type.createProperty("value", Long.class);
      final TypeIndex index = type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "value");

      db.transaction(() -> {
        for (int i = 0; i < 10; i++)
          db.newDocument("PlainScanIndexType").set("value", (long) i).save();
      });

      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(db);
      context.setCommandDeadline(System.currentTimeMillis() - 1, "an already expired deadline");

      // No condition: a full, unfiltered ascending scan of the index - the DISTINCT/ORDER BY fast path.
      final FetchFromIndexValuesStep step = new FetchFromIndexValuesStep((RangeIndex) index, true, context);
      final ResultSet result = step.syncPull(context, 100);

      assertThatThrownBy(() -> drain(result))
          .as("a full index scan with no WHERE must still stop at the command deadline")
          .isInstanceOf(TimeoutException.class)
          .hasMessageContaining("an already expired deadline");
    });
  }

  @Test
  void fetchFromRidsStepHonoursAnExpiredDeadline() throws Exception {
    TestHelper.executeInNewDatabase(db -> {
      final DocumentType type = db.getSchema().createDocumentType("PlainScanRidType");
      type.createProperty("value", Long.class);

      final List<RID> rids = new ArrayList<>();
      db.transaction(() -> {
        for (int i = 0; i < 10; i++)
          rids.add(db.newDocument("PlainScanRidType").set("value", (long) i).save().getIdentity());
      });

      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(db);
      context.setCommandDeadline(System.currentTimeMillis() - 1, "an already expired deadline");

      final FetchFromRidsStep step = new FetchFromRidsStep(rids, context);
      final ResultSet result = step.syncPull(context, 100);

      assertThatThrownBy(() -> drain(result))
          .as("a RID-list scan with no WHERE must still stop at the command deadline")
          .isInstanceOf(TimeoutException.class)
          .hasMessageContaining("an already expired deadline");
    });
  }

  /**
   * Sanity check: with no deadline configured (the default), every guarded step still returns every row -
   * the guard must cost nothing more than a comparison when disabled, never change the result.
   */
  @Test
  void unguardedScansAreUnaffectedWhenNoDeadlineIsConfigured() throws Exception {
    TestHelper.executeInNewDatabase(db -> {
      final DocumentType type = db.getSchema().createDocumentType("PlainScanNoDeadlineType");
      type.createProperty("value", Long.class);
      db.transaction(() -> {
        for (int i = 0; i < 10; i++)
          db.newDocument("PlainScanNoDeadlineType").set("value", (long) i).save();
      });

      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(db);

      final FetchFromClusterExecutionStep step = new FetchFromClusterExecutionStep(type.getFirstBucketId(), context);
      assertThat(drain(step.syncPull(context, 100))).hasSize(10);
    });
  }

  private static List<Result> drain(final ResultSet resultSet) {
    final List<Result> rows = new ArrayList<>();
    while (resultSet.hasNext())
      rows.add(resultSet.next());
    return rows;
  }
}
