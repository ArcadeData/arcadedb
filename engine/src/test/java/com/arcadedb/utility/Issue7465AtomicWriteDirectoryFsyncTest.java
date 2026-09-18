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
package com.arcadedb.utility;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.invocation.InvocationOnMock;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mockStatic;

/**
 * Regression test for issue #7465: {@code FileUtils.atomicWriteFile} and {@code atomicCopyFile} fsync'd the
 * temporary file and then published it with an atomic rename, but never forced the PARENT DIRECTORY. On a POSIX
 * filesystem the rename is a directory-metadata update, and fsyncing the file does not make that entry durable: the
 * guarantee both javadocs state - "a crash leaves the previous valid file untouched" - was exact for a crash of the
 * process and weaker than stated for a crash of the machine.
 * <p>
 * A genuine crash-consistency test would have to kill a process between the rename and the force, which is a
 * harness question rather than a unit test. What IS testable, and what regressed here, is whether the publish path
 * performs the directory fsync at all - so that is what these assert, by observing the one thing that fsync needs:
 * the parent directory opened as a channel. Nothing else on either path opens a DIRECTORY.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7465AtomicWriteDirectoryFsyncTest {

  @TempDir
  Path tempDir;

  @Test
  void atomicWriteForcesTheParentDirectoryAfterPublishingTheRename() throws Exception {
    final Path target = tempDir.resolve("schema.json");
    Files.writeString(target, "old");

    final int directoryOpens = countDirectoryOpens(() -> FileUtils.atomicWriteFile(target.toFile(), "new"));

    assertThat(directoryOpens)
        .as("the rename that publishes the file is directory metadata and has to be fsync'd on its own")
        .isEqualTo(isWindows() ? 0 : 1);
    assertThat(Files.readString(target)).isEqualTo("new");
  }

  @Test
  void atomicCopyForcesTheParentDirectoryToo() throws Exception {
    final Path source = tempDir.resolve("schema.json");
    final Path target = tempDir.resolve("schema.prev.json");
    Files.writeString(source, "generation-1");

    final int directoryOpens = countDirectoryOpens(() -> FileUtils.atomicCopyFile(source.toFile(), target.toFile()));

    assertThat(directoryOpens).isEqualTo(isWindows() ? 0 : 1);
    assertThat(Files.readString(target)).isEqualTo("generation-1");
    assertThat(Files.readString(source)).as("the source must never be moved aside").isEqualTo("generation-1");
  }

  @Test
  void forcingADirectorySucceedsOnEveryPlatformThatCan() {
    // Linux and macOS - where CI and every supported server deployment run - open a directory as a channel and
    // force it. Windows throws on the open instead, and the contract is that this is TOLERATED rather than fatal:
    // a durability improvement a platform cannot provide must never turn into a failed schema save on it.
    if (!isWindows())
      assertThat(FileUtils.forceDirectory(tempDir)).isTrue();
  }

  /**
   * A path that cannot be forced is refused per call and says NOTHING about the rest of the JVM. Whether this
   * platform can fsync a directory at all is decided once from the platform itself, so one uncooperative
   * directory - a network mount, an unusual permission - cannot silently turn the guarantee off for every other
   * database in the process (claude-review on PR #7855).
   */
  @Test
  void aPathThatCannotBeForcedIsRefusedWithoutDisablingTheFsyncForTheWholeJvm() throws Exception {
    final Path notADirectory = tempDir.resolve("plain.txt");
    Files.writeString(notADirectory, "x");

    assertThatCode(() -> FileUtils.forceDirectory(tempDir.resolve("absent"))).doesNotThrowAnyException();
    assertThatCode(() -> FileUtils.forceDirectory(notADirectory)).doesNotThrowAnyException();
    assertThatCode(() -> FileUtils.forceDirectory(null)).doesNotThrowAnyException();

    if (!isWindows())
      assertThat(FileUtils.forceDirectory(tempDir)).as("a real directory must still be forced afterwards").isTrue();
  }

  /**
   * And the same holds through the publish path: a directory the channel layer refuses leaves the write correct
   * and the NEXT publish, into a directory that works, still fsync'd.
   */
  @Test
  void oneRefusedDirectoryDoesNotStopTheNextPublishFromForcingItsOwn() throws Exception {
    final Path target = tempDir.resolve("schema.json");
    Files.writeString(target, "old");

    try (final MockedStatic<FileChannel> ignored = mockStatic(FileChannel.class, invocation -> {
      if (isDirectoryOpen(invocation))
        throw new IOException("this directory will not open");
      return invocation.callRealMethod();
    })) {
      FileUtils.atomicWriteFile(target.toFile(), "new");
    }

    assertThat(countDirectoryOpens(() -> FileUtils.atomicWriteFile(target.toFile(), "newer")))
        .as("one directory's refusal must not be generalised into a verdict about every other")
        .isEqualTo(isWindows() ? 0 : 1);
    assertThat(Files.readString(target)).isEqualTo("newer");
  }

  @Test
  void aPlatformThatRefusesToForceADirectoryStillPublishesTheFile() throws Exception {
    final Path target = tempDir.resolve("configuration.json");
    Files.writeString(target, "old");

    try (final MockedStatic<FileChannel> ignored = mockStatic(FileChannel.class, invocation -> {
      if (isDirectoryOpen(invocation))
        throw new IOException("this platform does not open directories");
      return invocation.callRealMethod();
    })) {
      FileUtils.atomicWriteFile(target.toFile(), "new");
    }

    assertThat(Files.readString(target)).as("a directory fsync that cannot be had is not a failed write")
        .isEqualTo("new");
  }

  /** Runs {@code publish} with {@code FileChannel.open} observed, counting the opens of {@link #tempDir} itself. */
  private int countDirectoryOpens(final ThrowingRunnable publish) throws Exception {
    final AtomicInteger opens = new AtomicInteger();
    try (final MockedStatic<FileChannel> ignored = mockStatic(FileChannel.class, invocation -> {
      if (isDirectoryOpen(invocation))
        opens.incrementAndGet();
      return invocation.callRealMethod();
    })) {
      publish.run();
    }
    return opens.get();
  }

  /**
   * {@code FileChannel.open(Path, OpenOption...)} delegates to {@code open(Path, Set, FileAttribute...)}, and the
   * static mock intercepts BOTH - so the varargs form is matched on its parameter count, or one call to it counts
   * as two.
   */
  private boolean isDirectoryOpen(final InvocationOnMock invocation) {
    return "open".equals(invocation.getMethod().getName()) && invocation.getMethod().getParameterCount() == 2
        && tempDir.equals(invocation.getArgument(0));
  }

  private static boolean isWindows() {
    return System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("win");
  }

  private interface ThrowingRunnable {
    void run() throws Exception;
  }
}
