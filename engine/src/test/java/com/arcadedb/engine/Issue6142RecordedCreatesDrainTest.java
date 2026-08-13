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
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6142: the creates of a recording session are carried forward as a consumable queue, not re-derived from the
 * cumulative log on every read.
 * <p>
 * The HA schema-instalment producer asks "what was created since my last instalment?" once per instalment. It used to
 * answer by walking the whole of {@code getRecordedChanges()} and filtering out what it had already shipped, which is
 * O(instalments x file changes): harmless for an index rebuild, which records one or two file changes however many
 * instalments its WAL volume produces, and quadratic for a DDL that creates many files through the same buffered
 * path. An index into the cumulative list is NOT the alternative - {@code dropFile} removes the cancelled create from
 * the middle of it - so the split is carried forward instead.
 * <p>
 * What these tests pin is that the queue tells the same story as the cumulative log about every file id, because the
 * consumer of one is the compensation for the other: a create the queue forgets is a file the followers are told to
 * create and nobody ever retires.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6142RecordedCreatesDrainTest {
  private static final Set<String> FILE_EXT = Set.of(LocalBucket.BUCKET_EXT);

  @Test
  void aDrainHandsOverOnlyWhatWasCreatedSinceThePreviousOne(@TempDir final Path dir) throws Exception {
    final FileManager fileManager = newFileManager(dir);
    assertThat(fileManager.startRecordingChanges()).isTrue();
    try {
      createFile(fileManager, dir, 0, "first");
      createFile(fileManager, dir, 1, "second");

      assertThat(fileManager.drainRecordedCreates())
          .as("the first instalment must announce both files, in creation order")
          .containsExactly(Map.entry(0, "first.0.65536.v1.bucket"), Map.entry(1, "second.1.65536.v1.bucket"));

      assertThat(fileManager.drainRecordedCreates())
          .as("nothing was created since, so the next instalment announces nothing - the whole point of draining "
              + "rather than re-reading the cumulative list").isEmpty();

      createFile(fileManager, dir, 2, "third");

      assertThat(fileManager.drainRecordedCreates())
          .as("only what appeared since the previous drain: re-announcing an already shipped file would make the "
              + "follower create over a file that is being written into")
          .containsExactly(Map.entry(2, "third.2.65536.v1.bucket"));

      assertThat(fileManager.getRecordedChanges())
          .as("draining must not consume the CUMULATIVE log: the session's final entry is built from it").hasSize(3);
    } finally {
      fileManager.stopRecordingChanges();
      fileManager.close();
    }
  }

  /**
   * The drop-cancels-create rule, which the cumulative log has always had and the queue has to mirror. A session that
   * creates a file and drops it again says nothing about it in its final entry, so a queue that still offered the
   * create would have an instalment announce a file that this node no longer has and no later entry mentions.
   */
  @Test
  void aCreateCancelledByADropIsNeverHandedOver(@TempDir final Path dir) throws Exception {
    final FileManager fileManager = newFileManager(dir);
    assertThat(fileManager.startRecordingChanges()).isTrue();
    try {
      createFile(fileManager, dir, 0, "kept");
      createFile(fileManager, dir, 1, "dropped");
      fileManager.dropFile(1);

      assertThat(fileManager.drainRecordedCreates())
          .as("the drop cancelled the create, exactly as it does in the cumulative log")
          .containsExactly(Map.entry(0, "kept.0.65536.v1.bucket"));
      assertThat(fileManager.getRecordedChanges()).hasSize(1);
    } finally {
      fileManager.stopRecordingChanges();
      fileManager.close();
    }
  }

  /**
   * A file dropped AFTER its create was drained is not the queue's business, and must not resurface as a create: the
   * instalment already announced it, so retiring it is the job of the shipper's own bookkeeping (which is what
   * {@code RaftReplicatedDatabase.reconcileInstalmentFiles} does with it).
   */
  @Test
  void aDropAfterTheDrainDoesNotReviveTheCreate(@TempDir final Path dir) throws Exception {
    final FileManager fileManager = newFileManager(dir);
    assertThat(fileManager.startRecordingChanges()).isTrue();
    try {
      createFile(fileManager, dir, 0, "shipped");
      assertThat(fileManager.drainRecordedCreates()).containsExactly(Map.entry(0, "shipped.0.65536.v1.bucket"));

      fileManager.dropFile(0);

      assertThat(fileManager.drainRecordedCreates()).as("a drop is not a create and never enters the queue").isEmpty();
    } finally {
      fileManager.stopRecordingChanges();
      fileManager.close();
    }
  }

  /**
   * Issue #4083 on the incremental path: {@code PaginatedComponent.removeTempSuffix} renames a file AFTER its create
   * was recorded, and an instalment announcing the pre-rename name would have the follower create the file under a
   * name the schema JSON does not use.
   */
  @Test
  void aRenameReachesTheQueueToo(@TempDir final Path dir) throws Exception {
    final FileManager fileManager = newFileManager(dir);
    assertThat(fileManager.startRecordingChanges()).isTrue();
    try {
      final ComponentFile file = createFile(fileManager, dir, 0, "temp_renamed");
      ((PaginatedComponentFile) file).rename("renamed.0.65536.v1." + LocalBucket.BUCKET_EXT);
      fileManager.refreshRecordedFileName(file);

      assertThat(fileManager.drainRecordedCreates())
          .as("the queue must carry the current name, like the cumulative log does")
          .containsExactly(Map.entry(0, "renamed.0.65536.v1.bucket"));
    } finally {
      fileManager.stopRecordingChanges();
      fileManager.close();
    }
  }

  /** Outside a session there is nothing to collect into, and the drain must say so rather than fail. */
  @Test
  void drainingWithNoSessionOpenIsEmpty(@TempDir final Path dir) {
    final FileManager fileManager = newFileManager(dir);
    try {
      assertThat(fileManager.drainRecordedCreates()).isEmpty();
    } finally {
      fileManager.close();
    }
  }

  // ---------------------------------------------------------------------------------------------------------------

  private static FileManager newFileManager(final Path dir) {
    return new FileManager(dir.toFile().getAbsolutePath(), ComponentFile.MODE.READ_WRITE, FILE_EXT);
  }

  private static ComponentFile createFile(final FileManager fileManager, final Path dir, final int fileId,
      final String name) throws Exception {
    return fileManager.getOrCreateFile(fileId,
        dir.resolve(name + "." + fileId + ".65536.v1." + LocalBucket.BUCKET_EXT).toFile().getAbsolutePath());
  }
}
