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
import java.nio.channels.FileChannel;
import java.nio.channels.spi.AbstractInterruptibleChannel;
import java.nio.file.Path;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for the ClosedByInterruptException crash during HA snapshot deferred-flush.
 *
 * When the XNIO HTTP thread is interrupted (e.g. because the follower closed the connection
 * while a snapshot was being served), Java NIO closes the FileChannel and throws
 * ClosedByInterruptException (a subclass of ClosedChannelException). The catch block in
 * PaginatedComponentFile.write/read/force then tries to reopen and retry. Before the fix, the
 * thread's interrupted flag was still set during the retry, causing Java NIO to close the
 * freshly-reopened channel immediately and throw again, leaving the file permanently closed and
 * triggering an emergency stop.
 *
 * The fix clears the interrupted flag before the retry and restores it afterward.
 *
 * IMPORTANT - why this test uses {@link InterruptVulnerablePaginatedComponentFile}:
 * In production {@code PaginatedComponentFile.open()} installs a proxy "interruptor" via
 * {@code doNotCloseOnInterrupt()} that normally STOPS NIO from closing the channel on interrupt.
 * Under the Maven build (which runs with {@code --add-opens java.base/java.nio.channels.spi})
 * that proxy is active, so a plain PaginatedComponentFile would never reach the catch/retry block
 * this fix touches - the test would pass even against the unpatched code. To reproduce the real
 * customer condition (a JVM where the channel DOES close on interrupt) the subclass strips the
 * proxy back to NIO's default interruptor after every open(), including the internal reopen. With
 * the default interruptor in place, the retry only succeeds if the fix clears the interrupted flag
 * first; against the unpatched code these tests fail with ClosedByInterruptException.
 */
class PaginatedComponentFileInterruptedWriteTest {

  private static final int PAGE_SIZE = 1024;
  private static final int FILE_ID   = 1;

  @TempDir
  Path tempDir;

  private PaginatedComponentFile pcf;
  private BasicDatabase          db;

  /**
   * A PaginatedComponentFile that restores NIO's default close-on-interrupt behaviour by undoing the
   * {@code doNotCloseOnInterrupt()} proxy after every open(). This reproduces the production
   * environment in which the channel actually closes when the running thread is interrupted.
   */
  static class InterruptVulnerablePaginatedComponentFile extends PaginatedComponentFile {
    InterruptVulnerablePaginatedComponentFile(final String filePath, final MODE mode) throws FileNotFoundException {
      super(filePath, mode);
    }

    @Override
    protected void open(final String filePath, final MODE mode) throws FileNotFoundException {
      super.open(filePath, mode);
      restoreDefaultInterruptor(this);
    }
  }

  /**
   * Sets the channel's {@code interruptor} field back to null so NIO lazily recreates its default
   * interruptor, which closes the channel on interrupt. Best-effort: if the reflection is blocked
   * (no {@code --add-opens}), the proxy was never installed in the first place, so the channel is
   * already vulnerable and nothing needs to be done.
   */
  private static void restoreDefaultInterruptor(final PaginatedComponentFile target) {
    try {
      final Field channelField = PaginatedComponentFile.class.getDeclaredField("channel");
      channelField.setAccessible(true);
      final FileChannel channel = (FileChannel) channelField.get(target);
      if (channel == null)
        return;
      final Field interruptorField = AbstractInterruptibleChannel.class.getDeclaredField("interruptor");
      interruptorField.setAccessible(true);
      interruptorField.set(channel, null);
    } catch (final Exception ignore) {
      // proxy was not installed (no --add-opens) - channel is already vulnerable to interrupts
    }
  }

  @BeforeEach
  void setUp() throws IOException {
    db = Mockito.mock(BasicDatabase.class);
    final String filePath = tempDir.resolve("page." + FILE_ID + "." + PAGE_SIZE + ".v0.arc").toString();
    pcf = new InterruptVulnerablePaginatedComponentFile(filePath, ComponentFile.MODE.READ_WRITE);
  }

  @AfterEach
  void tearDown() {
    Thread.interrupted(); // clear any leftover interrupted flag from test
    if (pcf != null)
      pcf.close();
  }

  /**
   * A thread whose interrupted flag is set before calling write() makes NIO's default interruptor
   * close the channel and throw ClosedByInterruptException on entry - exactly the same path as a
   * mid-write interrupt. The write() retry must clear the flag first so the reopened channel is not
   * immediately re-closed, and must restore the flag afterwards. Against the unpatched code the
   * retry re-closes the channel and write() throws.
   */
  @Test
  void writeSucceedsAndRestoresInterruptFlagWhenThreadIsInterrupted() throws Exception {
    final PageId     pageId = new PageId(db, FILE_ID, 0);
    final byte[]     data   = new byte[PAGE_SIZE];
    Arrays.fill(data, (byte) 0x5A);
    final MutablePage page = new MutablePage(pageId, PAGE_SIZE, data, 1, PAGE_SIZE);

    // Set the interrupted flag before the write to trigger ClosedByInterruptException in NIO.
    Thread.currentThread().interrupt();

    // write() must succeed (after internally clearing and restoring the interrupted flag).
    pcf.write(page);

    // The interrupted flag must be restored after the write.
    assertThat(Thread.interrupted()).as("interrupted flag must be restored after write").isTrue();

    // The channel must be open and the data readable.
    final CachedPage readPage = new CachedPage((PageManager) null, pageId, PAGE_SIZE);
    pcf.read(readPage);
    final ByteBuffer buf = readPage.getByteBuffer();
    buf.rewind();
    final byte[] readData = new byte[PAGE_SIZE];
    buf.get(readData);
    assertThat(readData).isEqualTo(data);
  }

  /**
   * Same scenario for read(): interrupted flag set before the read.
   */
  @Test
  void readSucceedsAndRestoresInterruptFlagWhenThreadIsInterrupted() throws Exception {
    final PageId     pageId = new PageId(db, FILE_ID, 0);
    final byte[]     data   = new byte[PAGE_SIZE];
    Arrays.fill(data, (byte) 0x3C);
    final MutablePage writePage = new MutablePage(pageId, PAGE_SIZE, data, 1, PAGE_SIZE);
    pcf.write(writePage);

    Thread.currentThread().interrupt();

    final CachedPage readPage = new CachedPage((PageManager) null, pageId, PAGE_SIZE);
    pcf.read(readPage);

    assertThat(Thread.interrupted()).as("interrupted flag must be restored after read").isTrue();

    final ByteBuffer buf = readPage.getByteBuffer();
    buf.rewind();
    final byte[] readData = new byte[PAGE_SIZE];
    buf.get(readData);
    assertThat(readData).isEqualTo(data);
  }

  /**
   * After a successful interrupted-write, the channel must remain open for subsequent writes.
   */
  @Test
  void channelRemainsOpenAfterInterruptedWrite() throws Exception {
    final PageId pageId0 = new PageId(db, FILE_ID, 0);
    final PageId pageId1 = new PageId(db, FILE_ID, 1);

    final byte[] data0 = new byte[PAGE_SIZE];
    Arrays.fill(data0, (byte) 0xAA);
    final MutablePage page0 = new MutablePage(pageId0, PAGE_SIZE, data0, 0, PAGE_SIZE);

    Thread.currentThread().interrupt();
    pcf.write(page0);
    Thread.interrupted(); // consume restored flag

    // A second write on the same file must succeed without re-interrupt.
    final byte[] data1 = new byte[PAGE_SIZE];
    Arrays.fill(data1, (byte) 0xBB);
    final MutablePage page1 = new MutablePage(pageId1, PAGE_SIZE, data1, 0, PAGE_SIZE);
    pcf.write(page1);

    final CachedPage rp0 = new CachedPage((PageManager) null, pageId0, PAGE_SIZE);
    pcf.read(rp0);
    final CachedPage rp1 = new CachedPage((PageManager) null, pageId1, PAGE_SIZE);
    pcf.read(rp1);

    final byte[] r0 = new byte[PAGE_SIZE];
    rp0.getByteBuffer().rewind();
    rp0.getByteBuffer().get(r0);
    assertThat(r0).isEqualTo(data0);

    final byte[] r1 = new byte[PAGE_SIZE];
    rp1.getByteBuffer().rewind();
    rp1.getByteBuffer().get(r1);
    assertThat(r1).isEqualTo(data1);
  }
}
