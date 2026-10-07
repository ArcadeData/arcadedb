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
import java.nio.file.Files;
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

  // #9306: EVERY OTHER CHANNEL OPERATION OF THE CLASS GETS THE SAME BOUNDED REOPEN AS read(CachedPage)

  private byte[] writeFilledPage(final int pageNumber, final byte value) throws IOException {
    final byte[] data = new byte[PAGE_SIZE];
    Arrays.fill(data, value);
    pcf.write(new MutablePage(new PageId(db, FILE_ID, pageNumber), PAGE_SIZE, data, 1, PAGE_SIZE));
    return data;
  }

  @Test
  void readPagesReopensAChannelAnInterruptClosed() throws Exception {
    final byte[] page0 = writeFilledPage(0, (byte) 0x11);
    final byte[] page1 = writeFilledPage(1, (byte) 0x22);

    // THE BUFFER IS PRE-POSITIONED, AS PageSnapshot DOES IT: THE RETRY MUST RESTART FROM THAT POSITION, NOT FROM 0
    final ByteBuffer buf = ByteBuffer.allocate(3 * PAGE_SIZE);
    buf.position(PAGE_SIZE);

    pcf.closeChannel();
    pcf.closeAfterOpens.set(2);
    pcf.readPages(0, 2, buf);

    assertThat(buf.position()).isEqualTo(3 * PAGE_SIZE);
    assertThat(Arrays.copyOfRange(buf.array(), 0, PAGE_SIZE)).containsOnly((byte) 0);
    assertThat(Arrays.copyOfRange(buf.array(), PAGE_SIZE, 2 * PAGE_SIZE)).isEqualTo(page0);
    assertThat(Arrays.copyOfRange(buf.array(), 2 * PAGE_SIZE, 3 * PAGE_SIZE)).isEqualTo(page1);
  }

  @Test
  void readPagesGivesUpWhenTheChannelKeepsClosing() throws Exception {
    writeFilledPage(0, (byte) 0x11);

    pcf.closeChannel();
    pcf.closeAfterOpens.set(1_000);

    final ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
    assertThatThrownBy(() -> pcf.readPages(0, 1, buf)).isInstanceOf(ClosedChannelException.class);
    assertThat(pcf.closeAfterOpens.get()).isGreaterThan(900);
    // THE CALLER'S LIMIT IS RESTORED ON THE FAILURE PATH TOO
    assertThat(buf.limit()).isEqualTo(PAGE_SIZE);
  }

  @Test
  void readPagesRestoresTheInterruptFlagAfterTheRetry() throws Exception {
    final byte[] page0 = writeFilledPage(0, (byte) 0x33);

    pcf.closeChannel();
    Thread.currentThread().interrupt();
    try {
      final ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
      pcf.readPages(0, 1, buf);
      assertThat(buf.array()).isEqualTo(page0);
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void readPagesRefusesToReopenAFileClosedOnPurpose() throws Exception {
    writeFilledPage(0, (byte) 0x11);
    pcf.close();

    assertThatThrownBy(() -> pcf.readPages(0, 1, ByteBuffer.allocate(PAGE_SIZE))).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void readPagesSurfacesARefusedReopenAsAFailure() throws Exception {
    writeFilledPage(0, (byte) 0x11);

    // THE CHANNEL WAS CLOSED BY AN INTERRUPT, BUT THE FILE IS GONE FROM DISK: REOPENING WOULD RE-CREATE IT (#4930)
    pcf.closeChannel();
    Files.delete(tempDir.resolve("page." + FILE_ID + "." + PAGE_SIZE + ".v0.arc"));

    assertThatThrownBy(() -> pcf.readPages(0, 1, ByteBuffer.allocate(PAGE_SIZE))).isInstanceOf(FileNotFoundException.class)
        .hasMessageContaining("no longer exists");
  }

  @Test
  void readPageReopensAChannelAnInterruptClosed() throws Exception {
    final byte[] page0 = writeFilledPage(0, (byte) 0x44);

    pcf.closeChannel();
    pcf.closeAfterOpens.set(2);
    final ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
    pcf.readPage(0, buf);

    assertThat(buf.array()).isEqualTo(page0);
  }

  @Test
  void readPageOnAClosedFileFailsLikeItsSiblings() throws Exception {
    writeFilledPage(0, (byte) 0x44);
    pcf.close();

    assertThatThrownBy(() -> pcf.readPage(0, ByteBuffer.allocate(PAGE_SIZE))).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("is closed");
  }

  @Test
  void checksumReopensAChannelAnInterruptClosed() throws Exception {
    writeFilledPage(0, (byte) 0x55);
    writeFilledPage(1, (byte) 0x66);
    final long expected = pcf.calculateChecksum();

    pcf.closeChannel();
    pcf.closeAfterOpens.set(2);
    assertThat(pcf.calculateChecksum()).isEqualTo(expected);
  }

  @Test
  void sizeReopensAChannelAnInterruptClosed() throws Exception {
    writeFilledPage(0, (byte) 0x55);
    writeFilledPage(1, (byte) 0x66);

    pcf.closeChannel();
    pcf.closeAfterOpens.set(2);
    assertThat(pcf.getSize()).isEqualTo(2L * PAGE_SIZE);

    pcf.closeChannel();
    pcf.closeAfterOpens.set(2);
    assertThat(pcf.getTotalPagesFromChannel()).isEqualTo(2L);
  }

  @Test
  void sizeAndChecksumOnAClosedFileFailLikeTheReads() throws Exception {
    writeFilledPage(0, (byte) 0x55);
    pcf.close();

    assertThatThrownBy(() -> pcf.getSize()).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("is closed");
    assertThatThrownBy(() -> pcf.getTotalPagesFromChannel()).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("is closed");
    assertThatThrownBy(() -> pcf.calculateChecksum()).isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("is closed");
  }

  @Test
  void writeRetriesWhenTheReopenedChannelIsClosedAgain() throws Exception {
    writeFilledPage(0, (byte) 0x01);

    // write USED TO REOPEN ONCE: A SECOND CLOSE LANDING ON THE REOPENED CHANNEL FAILED THE PAGE FLUSH
    pcf.closeChannel();
    pcf.closeAfterOpens.set(2);
    final byte[] data = writeFilledPage(0, (byte) 0x77);

    final ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
    pcf.readPage(0, buf);
    assertThat(buf.array()).isEqualTo(data);
  }

  @Test
  void forceRetriesWhenTheReopenedChannelIsClosedAgain() throws Exception {
    writeFilledPage(0, (byte) 0x01);

    pcf.closeChannel();
    pcf.closeAfterOpens.set(2);
    pcf.force(true);
    assertThat(pcf.isModifiedSinceLastSync()).isFalse();
  }
}
