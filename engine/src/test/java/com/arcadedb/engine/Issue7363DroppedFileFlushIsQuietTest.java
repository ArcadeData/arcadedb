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
import com.arcadedb.log.WarningCapture;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Issue #7363: during a long ingest the async flush thread repeatedly reported, at SEVERE and at WARNI, that it
 * could not write pages to a sub-index file an index compaction had since closed:
 * <pre>
 * SEVER [PageManagerFlushThread] Error on processing page flush requests
 * java.lang.IllegalArgumentException: Cannot write page 8 because the file '...umtidx' is closed
 * WARNI [PageManagerFlushThread] Error on flushing page 'PageId(citation_graph/27/9) v=1' to disk
 * com.arcadedb.exception.DatabaseMetadataException: Cannot flush pages on disk because file '...umtidx' is closed
 * </pre>
 * Those pages are superseded by construction - the file they belong to has been replaced and deleted, so there is
 * nowhere for them to go and nothing to lose - and {@code flushPage} has always had a quiet FINE path for exactly
 * that, taken when the file has already left the {@code FileManager}. What it did not have was a way to recognise
 * the same situation one instant EARLIER, while the file is on its way out: still registered, closed or closing.
 * So the identical, harmless event was reported at the two levels an operator watches for real corruption.
 * <p>
 * The {@code IllegalArgumentException} variant was worse than noise. It is unchecked, so it escaped every per-page
 * catch in the flush loop, abandoning the rest of the batch: those pages kept their {@code pageIndex} entries and
 * their WAL acks forever, and {@code waitAllPagesOfDatabaseAreFlushed} then burned its whole budget on the next
 * close.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7363DroppedFileFlushIsQuietTest extends TestHelper {
  private static final int PAGE_SIZE = 65536;

  /**
   * The exact window the report caught: the file has been dropped (closed, deleted) but is still registered, so
   * every "does this file exist" check the flush path makes still says yes.
   */
  @Test
  void aPageOfADroppedButStillRegisteredFileIsDiscardedQuietly() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;

    final PaginatedComponentFile file = scratchFile(db, "issue7363-dropped");
    final MutablePage page = new MutablePage(new PageId(db, file.getFileId(), 0), PAGE_SIZE);
    page.writeInt(0, 0xDEAD);

    file.drop();

    assertThat(file.isDropped()).as("drop() has to say so, and say so before it closes the channel").isTrue();
    assertThat(db.getFileManager().existsFile(file.getFileId()))
        .as("dropping the file object alone leaves it registered - the window the report caught").isTrue();

    final List<WarningCapture.LogLine> lines = WarningCapture.capture(java.util.logging.Level.WARNING,
        () -> assertThatCode(() -> PageManager.INSTANCE.flushPage(page))
            .as("a page addressed to a deleted file must not fail the flush of the batch it is in")
            .doesNotThrowAnyException());

    assertThat(lines)
        .as("a superseded page is a FINE event, not a SEVERE/WARNI one on a path watched for corruption; got: %s",
            lines)
        .noneMatch(line -> line.message().contains("Cannot write page")
            || line.message().contains("Cannot flush pages on disk")
            || line.message().contains("Error on flushing page")
            || line.message().contains("Error on processing page flush requests"));
  }

  /**
   * The same page, once the file has also left the {@code FileManager}: quiet before this change too, and it has
   * to stay quiet - the two are the same event observed on either side of one non-atomic drop.
   */
  @Test
  void aPageOfAFullyUnregisteredFileStaysQuietToo() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;

    final PaginatedComponentFile file = scratchFile(db, "issue7363-unregistered");
    final MutablePage page = new MutablePage(new PageId(db, file.getFileId(), 0), PAGE_SIZE);

    db.getFileManager().dropFile(file.getFileId());
    assertThat(db.getFileManager().existsFile(file.getFileId())).isFalse();

    final List<WarningCapture.LogLine> lines = WarningCapture.capture(java.util.logging.Level.WARNING,
        () -> assertThatCode(() -> PageManager.INSTANCE.flushPage(page)).doesNotThrowAnyException());

    assertThat(lines).as("got: %s", lines)
        .noneMatch(line -> line.message().contains("Cannot write page")
            || line.message().contains("Cannot flush pages on disk"));
  }

  /**
   * A file that is merely CLOSED and not dropped is a different thing entirely - the engine writing to a live
   * component whose channel it lost - and must still be reported. Silencing that along with the dropped case would
   * trade a false alarm for a missed one.
   */
  @Test
  void aPageOfAClosedButNotDroppedFileIsStillReported() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;

    final PaginatedComponentFile file = scratchFile(db, "issue7363-closed");
    final MutablePage page = new MutablePage(new PageId(db, file.getFileId(), 0), PAGE_SIZE);

    file.close();
    assertThat(file.isDropped()).isFalse();

    assertThatCode(() -> PageManager.INSTANCE.flushPage(page))
        .as("nothing says these pages are superseded, so this failure must reach the caller")
        .hasMessageContaining("is closed");

    // Leave nothing half-open behind for the fixture's own close.
    db.getFileManager().dropFile(file.getFileId());
  }

  /**
   * A page-backed file registered with the {@code FileManager} but backed by no schema component: enough for the
   * flush path, which resolves the file by id, and detached from everything the fixture's own close touches.
   */
  private static PaginatedComponentFile scratchFile(final DatabaseInternal db, final String componentName)
      throws Exception {
    final int fileId = db.getFileManager().newFileId();
    final String path = db.getDatabasePath() + "/" + componentName + "." + fileId + "." + PAGE_SIZE + ".v0.scratch";
    return (PaginatedComponentFile) db.getFileManager()
        .getOrCreateFile(componentName, path, ComponentFile.MODE.READ_WRITE);
  }
}
