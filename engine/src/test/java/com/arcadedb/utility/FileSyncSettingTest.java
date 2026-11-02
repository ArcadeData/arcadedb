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

import com.arcadedb.GlobalConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.SyncFailedException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code arcadedb.fileSync}: the switch that turns every fsync ArcadeDB issues into a no-op, so a test run that needs no
 * durability across a power failure does not pay a device flush per schema save, file creation and close. The unit-test
 * lanes set it to {@code false} through Surefire, which is why its default has to follow the system property across
 * {@link GlobalConfiguration#resetAll()}: {@code TestHelper} resets every setting after each test.
 * <p>
 * A forced CLOSED channel or descriptor throws, which is what tells a skipped fsync from one that ran without having to
 * count calls.
 */
class FileSyncSettingTest {
  private static final String PROPERTY = "arcadedb.fileSync";

  @TempDir
  Path dir;

  @AfterEach
  void restoreSetting() {
    GlobalConfiguration.FILE_SYNC.reset();
  }

  @Test
  void theDefaultFollowsTheSystemPropertyAcrossAReset() {
    final String previous = System.getProperty(PROPERTY);
    try {
      System.clearProperty(PROPERTY);
      GlobalConfiguration.FILE_SYNC.reset();
      assertThat(GlobalConfiguration.FILE_SYNC.getValueAsBoolean()).as("fsync is on unless the process turns it off").isTrue();

      System.setProperty(PROPERTY, "false");
      GlobalConfiguration.FILE_SYNC.reset();
      assertThat(GlobalConfiguration.FILE_SYNC.getValueAsBoolean()).as("the reset after a test keeps what the test JVM asked for")
          .isFalse();

      System.setProperty(PROPERTY, "FALSE");
      GlobalConfiguration.FILE_SYNC.reset();
      assertThat(GlobalConfiguration.FILE_SYNC.getValueAsBoolean()).isFalse();

      // only an explicit "false" turns durability off: a typo keeps the fsync
      System.setProperty(PROPERTY, "flase");
      GlobalConfiguration.FILE_SYNC.reset();
      assertThat(GlobalConfiguration.FILE_SYNC.getValueAsBoolean()).isTrue();
    } finally {
      if (previous != null)
        System.setProperty(PROPERTY, previous);
      else
        System.clearProperty(PROPERTY);
    }
  }

  @Test
  void aChannelIsForcedOnlyWhileFileSyncIsOn() throws IOException {
    final FileChannel channel = FileChannel.open(dir.resolve("data"), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    channel.close();

    GlobalConfiguration.FILE_SYNC.setValue(false);
    assertThatCode(() -> FileUtils.force(channel, true)).as("no fsync is attempted").doesNotThrowAnyException();

    GlobalConfiguration.FILE_SYNC.setValue(true);
    assertThatThrownBy(() -> FileUtils.force(channel, true)).isInstanceOf(ClosedChannelException.class);
  }

  @Test
  void aDescriptorIsSyncedOnlyWhileFileSyncIsOn() throws IOException {
    final FileDescriptor fd;
    try (final FileOutputStream out = new FileOutputStream(dir.resolve("data").toFile())) {
      fd = out.getFD();
    }

    GlobalConfiguration.FILE_SYNC.setValue(false);
    assertThatCode(() -> FileUtils.sync(fd)).as("no fsync is attempted").doesNotThrowAnyException();

    GlobalConfiguration.FILE_SYNC.setValue(true);
    assertThatThrownBy(() -> FileUtils.sync(fd)).isInstanceOf(SyncFailedException.class);
  }

  @Test
  void aDirectoryIsForcedOnlyWhileFileSyncIsOn() {
    GlobalConfiguration.FILE_SYNC.setValue(false);
    assertThat(FileUtils.forceDirectory(dir)).isFalse();

    GlobalConfiguration.FILE_SYNC.setValue(true);
    // Windows cannot fsync a directory at all (see FileUtils.forceDirectory)
    assertThat(FileUtils.forceDirectory(dir)).isEqualTo(!System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("win"));
  }
}
