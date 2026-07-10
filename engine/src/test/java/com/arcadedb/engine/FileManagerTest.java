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

import com.arcadedb.index.lsm.LSMTreeIndexCompacted;
import com.arcadedb.index.lsm.LSMTreeIndexMutable;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

/**
 * @author carlos-rodrigues@8x8.com
 */
@EnabledOnOs({OS.LINUX, OS.MAC})
public class FileManagerTest {
  public static final Set<String> FILE_EXT = Set.of(Dictionary.DICT_EXT,
      LocalBucket.BUCKET_EXT, LSMTreeIndexMutable.NOTUNIQUE_INDEX_EXT, LSMTreeIndexMutable.UNIQUE_INDEX_EXT,
      LSMTreeIndexCompacted.NOTUNIQUE_INDEX_EXT, LSMTreeIndexCompacted.UNIQUE_INDEX_EXT);

  @Test
  void construtor_failure_noPermissionsDirectory(@TempDir Path dir) throws Exception {
    // arrange

    Set<PosixFilePermission> noPerms = EnumSet.noneOf(PosixFilePermission.class);
    Files.setPosixFilePermissions(dir, noPerms);

    // act and assert
    assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> {
      new FileManager(dir.toFile().getAbsolutePath(), ComponentFile.MODE.READ_WRITE, FILE_EXT);
    });

    // reset permissions to allow cleanup
    Set<PosixFilePermission> restorePerms = PosixFilePermissions.fromString("rwx------");
    Files.setPosixFilePermissions(dir, restorePerms);
  }

  @Test
  void construtor_failure_parentDirectoryWithNoPermissions(@TempDir Path dir) throws Exception {
    // arrange
    Set<PosixFilePermission> noPerms = EnumSet.noneOf(PosixFilePermission.class);
    Files.setPosixFilePermissions(dir, noPerms);

    // act and assert
    assertThatExceptionOfType(IllegalArgumentException.class).isThrownBy(() -> {
      new FileManager(dir.toFile().getAbsolutePath() + "/child", ComponentFile.MODE.READ_WRITE, FILE_EXT);
    });

    // cleanup
    Set<PosixFilePermission> restorePerms = PosixFilePermissions.fromString("rwx------");
    Files.setPosixFilePermissions(dir, restorePerms);
  }

  @Test
  void construtor_success_emptyDirectory(@TempDir Path dir) throws Exception {
    // arrange
    // act
    FileManager fileManager = new FileManager(dir.toFile().getAbsolutePath(), ComponentFile.MODE.READ_WRITE, FILE_EXT);

    // assert
    assertThat(fileManager.getFiles().isEmpty()).isTrue();
  }

  @Test
  void construtor_success_noDirectory() throws Exception {
    // arrange
    Path dir = Path.of(System.getProperty("java.io.tmpdir"), "nonExistentDir");

    // act
    FileManager fileManager = new FileManager(dir.toFile().getAbsolutePath(), ComponentFile.MODE.READ_WRITE, FILE_EXT);

    // assert
    assertThat(fileManager.getFiles().isEmpty()).isTrue();
    // cleanup
    Files.deleteIfExists(dir);
  }

  @Test
  void getFiles_concurrentWithNewFileId_noConcurrentModificationException(@TempDir Path dir) throws Exception {
    // Regression test for #4371: concurrent newFileId() + getFiles() iteration must not throw CME.
    // newFileId() adds to the backing list under its own lock; getFiles() must return a snapshot
    // so that iterating the returned list is safe even while writers add slots concurrently.
    final FileManager fileManager = new FileManager(dir.toFile().getAbsolutePath(), ComponentFile.MODE.READ_WRITE, Set.of());
    final int threadCount = 8;
    final int iterationsPerThread = 500;
    final AtomicReference<Throwable> caughtError = new AtomicReference<>();
    final CountDownLatch startLatch = new CountDownLatch(1);

    final List<Thread> writers = new ArrayList<>();
    for (int i = 0; i < threadCount; i++) {
      writers.add(new Thread(() -> {
        try {
          startLatch.await();
          for (int j = 0; j < iterationsPerThread; j++)
            fileManager.newFileId();
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }));
    }

    final List<Thread> readers = new ArrayList<>();
    for (int i = 0; i < threadCount; i++) {
      readers.add(new Thread(() -> {
        try {
          startLatch.await();
          for (int j = 0; j < iterationsPerThread; j++) {
            // iterate the full snapshot returned by getFiles() - must not throw CME
            for (final ComponentFile ignored : fileManager.getFiles()) {
              // just traverse
            }
          }
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        } catch (final Throwable t) {
          caughtError.compareAndSet(null, t);
        }
      }));
    }

    writers.forEach(Thread::start);
    readers.forEach(Thread::start);
    startLatch.countDown();

    for (final Thread t : writers)
      t.join(10_000);
    for (final Thread t : readers)
      t.join(10_000);

    assertThat(caughtError.get())
        .as("concurrent getFiles() iteration must not throw ConcurrentModificationException")
        .isNull();
    assertThat(fileManager.getFiles()).hasSize(threadCount * iterationsPerThread);
  }
}
