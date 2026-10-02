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

import com.arcadedb.database.BasicDatabase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for #8944: a read whose channel was closed by an interrupt reopens it and retries, and when a second
 * interrupt lands during that retry (the producer's own cancellation after another thread's interrupt closed the
 * channel) the retry must repeat instead of surfacing a closed channel to the caller.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PaginatedComponentFileReadRetryTest {

  private static final int PAGE_SIZE = 1024;
  private static final int FILE_ID   = 1;

  @TempDir
  Path tempDir;

  private FlakyReopenFile pcf;
  private BasicDatabase   db;

  /** Closes the channel right after the next N opens, as an interrupt landing between the reopen and the read does. */
  static class FlakyReopenFile extends PaginatedComponentFile {
    final AtomicInteger closeAfterOpens = new AtomicInteger();

    FlakyReopenFile(final String filePath, final MODE mode) throws FileNotFoundException {
      super(filePath, mode);
    }

    @Override
    protected void open(final String filePath, final MODE mode) throws FileNotFoundException {
      super.open(filePath, mode);
      // NULL WHILE THE SUPERCLASS CONSTRUCTOR RUNS: open() IS CALLED FROM IT, BEFORE THIS CLASS'S FIELD INITIALIZERS
      if (closeAfterOpens != null && closeAfterOpens.get() > 0 && closeAfterOpens.decrementAndGet() >= 0)
        closeChannel();
    }

    void closeChannel() {
      try {
        final Field field = PaginatedComponentFile.class.getDeclaredField("channel");
        field.setAccessible(true);
        ((FileChannel) field.get(this)).close();
      } catch (final ReflectiveOperationException | IOException e) {
        throw new IllegalStateException(e);
      }
    }
  }

  @BeforeEach
  void setUp() throws IOException {
    db = Mockito.mock(BasicDatabase.class);
    pcf = new FlakyReopenFile(tempDir.resolve("page." + FILE_ID + "." + PAGE_SIZE + ".v0.arc").toString(), ComponentFile.MODE.READ_WRITE);
  }

  @AfterEach
  void tearDown() {
    if (pcf != null)
      pcf.close();
  }

  @Test
  void readRetriesWhenTheReopenedChannelIsClosedAgain() throws Exception {
    final PageId pageId = new PageId(db, FILE_ID, 0);
    final byte[] data = new byte[PAGE_SIZE];
    Arrays.fill(data, (byte) 0x42);
    pcf.write(new MutablePage(pageId, PAGE_SIZE, data, 1, PAGE_SIZE));

    // THE FIRST READ FINDS THE CHANNEL CLOSED, AND THE FIRST TWO REOPENS ARE CLOSED AGAIN BEFORE THE RETRY READS
    pcf.closeChannel();
    pcf.closeAfterOpens.set(2);

    final CachedPage readPage = new CachedPage((PageManager) null, pageId, PAGE_SIZE);
    pcf.read(readPage);

    final ByteBuffer buf = readPage.getByteBuffer();
    buf.rewind();
    final byte[] readData = new byte[PAGE_SIZE];
    buf.get(readData);
    assertThat(readData).isEqualTo(data);
  }

  @Test
  void readGivesUpWhenTheChannelKeepsClosing() throws Exception {
    final PageId pageId = new PageId(db, FILE_ID, 0);
    pcf.write(new MutablePage(pageId, PAGE_SIZE, new byte[PAGE_SIZE], 1, PAGE_SIZE));

    pcf.closeChannel();
    pcf.closeAfterOpens.set(1_000);

    final CachedPage readPage = new CachedPage((PageManager) null, pageId, PAGE_SIZE);
    assertThatThrownBy(() -> pcf.read(readPage)).isInstanceOf(ClosedChannelException.class);
    // BOUNDED: IT DID NOT CONSUME EVERY SIMULATED CLOSE
    assertThat(pcf.closeAfterOpens.get()).isGreaterThan(900);
  }
}
