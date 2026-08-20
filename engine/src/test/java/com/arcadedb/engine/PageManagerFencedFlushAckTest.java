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
package com.arcadedb.engine;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.log.WarningCapture;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Discord report (2026-08-14, "heimdall" database): {@code PageManager.flushPage()} only guarded against
 * {@code !database.isOpen()}. A database fenced for recovery (#5053) is still open, just refusing every
 * operation - so a page still queued for the async flush pipeline reached {@code getSchema()}, which threw for
 * the fenced state. That exception was not one of the ones the flush pipeline's per-page catches expected
 * ({@code DatabaseIsClosedException}, {@code DatabaseMetadataException}, {@code IOException}), so it escaped
 * uncaught, the page's WAL ack was never released, and its {@code pageIndex}/deferred-backlog entry was left
 * behind - the exact shape of the stranded-ack bug issue #6440/PR #6481 fixed for a plain close() a few hours
 * earlier that day, minus the one case that fix did not anticipate. In production this is what "No flush
 * progress for 60000 ms with N pages still pending" and a database wedged after a single fenced commit look
 * like: every OTHER queued page, unrelated to the one that triggered the fence, stops flushing too.
 * <p>
 * Reproduced deterministically via {@link PageManager#suspendFlushAndExecute}: a page committed while flushing
 * is suspended is guaranteed to still be pending (deferred, not yet written) when the database is then fenced,
 * so un-suspending exercises exactly the code path ({@code PageManagerFlushThread.resumeFlushing} ->
 * {@code PageManager.flushPage}) the report hit (#6505).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PageManagerFencedFlushAckTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.getSchema().createDocumentType("Doc");
  }

  @Test
  void fencedDatabaseDoesNotStrandAQueuedPageFlushAck() {
    final DatabaseInternal db = (DatabaseInternal) database;

    final List<String> severeLines = WarningCapture.captureSevere(() -> {
      try {
        PageManager.INSTANCE.suspendFlushAndExecute(db, () -> {
          // Committed while flushing is suspended for this database: the page is deferred, guaranteed still
          // pending when the fence lands a line below, not racing the background flush thread for it.
          db.transaction(() -> db.newDocument("Doc").set("v", 1).save());
          ((LocalDatabase) db).fenceForRecovery("test-injected post-WAL-append failure");
        });
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });

    assertThat(((LocalDatabase) db).isFencedForRecovery()).isTrue();
    assertThat(severeLines)
        .as("a page abandoned because its database was fenced must be acked silently, like the closed-database "
            + "case (#6440) - not escape as an unexpected flush-pipeline failure; got: %s", severeLines)
        .noneMatch(line -> line.contains("Unexpected error on flushing deferred page")
            || line.contains("Error on processing page flush requests"));

    database.close();
  }
}
