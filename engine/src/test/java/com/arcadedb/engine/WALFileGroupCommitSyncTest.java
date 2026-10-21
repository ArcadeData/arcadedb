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

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The flags a group commit leaves on a WAL file ({@link WALFile#markUnsynced}): a flush that fails keeps the writes owed WITH
 * the metadata flush they asked for, and a successful one clears the metadata request, so later data-only writes are not
 * charged full flushes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class WALFileGroupCommitSyncTest {

  @Test
  void aFailedFlushKeepsTheWritesOwedWithTheirMetadataRequest() throws Exception {
    final File directory = new File("target/databases/WALFileGroupCommitSyncTest");
    directory.mkdirs();
    final List<Boolean> flushes = new ArrayList<>();
    final AtomicBoolean failNext = new AtomicBoolean(true);
    final WALFile file = new WALFile(new File(directory, "groupcommit.wal").getAbsolutePath()) {
      @Override
      void force(final boolean metaData) throws IOException {
        flushes.add(metaData);
        if (failNext.getAndSet(false))
          throw new IOException("injected flush failure");
      }
    };
    try {
      file.markUnsynced(true);
      assertThatThrownBy(file::syncIfUnsynced).isInstanceOf(IOException.class);

      // retried with the metadata flush the write asked for
      file.syncIfUnsynced();
      assertThat(flushes).containsExactly(true, true);

      // nothing is owed any more
      file.syncIfUnsynced();
      assertThat(flushes).hasSize(2);

      // the metadata request does not outlive the writes that made it
      file.markUnsynced(false);
      file.syncIfUnsynced();
      assertThat(flushes).containsExactly(true, true, false);
    } finally {
      file.drop();
    }
  }

  /** A file whose close could not flush its owed writes must never be reported flushed afterwards. */
  @Test
  void aCloseThatCouldNotFlushLeavesTheWritesOwed() throws Exception {
    final File directory = new File("target/databases/WALFileGroupCommitSyncTest");
    directory.mkdirs();
    final AtomicBoolean fail = new AtomicBoolean(true);
    final WALFile file = new WALFile(new File(directory, "groupcommit-close.wal").getAbsolutePath()) {
      @Override
      void force(final boolean metaData) throws IOException {
        if (fail.get())
          throw new IOException("injected flush failure");
      }
    };
    try {
      file.markUnsynced(false);
      assertThatThrownBy(file::close).isInstanceOf(IOException.class);

      // a group-commit pass reaching the closed file must fail, not report the writes durable
      fail.set(false);
      assertThatThrownBy(file::syncIfUnsynced).isInstanceOf(IOException.class).hasMessageContaining("could not be flushed");
      assertThatThrownBy(file::syncIfUnsynced).isInstanceOf(IOException.class);
    } finally {
      new File(directory, "groupcommit-close.wal").delete();
    }
  }
}
