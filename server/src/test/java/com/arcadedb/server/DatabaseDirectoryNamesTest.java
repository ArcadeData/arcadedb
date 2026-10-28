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
package com.arcadedb.server;

import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

/**
 * Issue #10567: the in-memory listing of the database directory {@code isDatabaseOfServer} answers from. A change the server makes
 * is seen at once through {@link DatabaseDirectoryNames#invalidate()}, also when the invalidation lands while a listing that began
 * before it is still running: that listing must not be taken for the current one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class DatabaseDirectoryNamesTest {
  private static final String MARKER = "schema.json";

  private File directory;

  @BeforeEach
  void setUp() {
    directory = new File("./target/databaseDirectoryNamesTest");
    FileUtils.deleteRecursively(directory);
    assertThat(directory.mkdirs()).isTrue();
  }

  @AfterEach
  void tearDown() {
    FileUtils.deleteRecursively(directory);
  }

  @Test
  void aDirectoryIsADatabaseOnlyWhenItHoldsOneAndReservedNamesNeverAre() throws IOException {
    database("a");
    assertThat(new File(directory, "plain").mkdirs()).isTrue();
    database(".raft");

    final DatabaseDirectoryNames names = new DatabaseDirectoryNames(directory, holdsMarker(), name -> name.startsWith("."));
    assertThat(names.contains("a")).isTrue();
    assertThat(names.contains("plain")).as("a directory that holds no database").isFalse();
    assertThat(names.contains(".raft")).as("a reserved name").isFalse();
    assertThat(names.contains("missing")).isFalse();
  }

  @Test
  void aChangeTheServerMakesIsSeenAtOnce() throws IOException {
    final DatabaseDirectoryNames names = new DatabaseDirectoryNames(directory, holdsMarker(), name -> false);
    assertThat(names.contains("b")).isFalse();

    database("b");
    names.invalidate();
    assertThat(names.contains("b")).isTrue();

    FileUtils.deleteRecursively(new File(directory, "b"));
    names.invalidate();
    assertThat(names.contains("b")).isFalse();
  }

  @Test
  void anInvalidationDuringAListingIsNotLostToIt() throws IOException {
    database("a");
    final AtomicBoolean invalidateOnce = new AtomicBoolean(true);
    final DatabaseDirectoryNames[] holder = new DatabaseDirectoryNames[1];
    // while the first listing runs, the server creates "c" and invalidates: the listing already in progress did not see "c"
    final Predicate<File> predicate = dir -> {
      if (invalidateOnce.compareAndSet(true, false)) {
        try {
          database("c");
        } catch (final IOException e) {
          throw new RuntimeException(e);
        }
        holder[0].invalidate();
      }
      return new File(dir, MARKER).exists();
    };
    holder[0] = new DatabaseDirectoryNames(directory, predicate, name -> false);

    holder[0].contains("a");
    assertThat(holder[0].contains("c")).as("the listing that began before the invalidation is not the current one").isTrue();
  }

  /**
   * A listing that fails (an I/O error, here a directory that cannot be read) answers the last names that were listed, not "no
   * database", and is listed again once the directory can be read, although its modification time did not move since.
   */
  @Test
  void aListingThatFailsKeepsTheLastNamesAndIsRetried() throws IOException {
    database("a");
    final DatabaseDirectoryNames names = new DatabaseDirectoryNames(directory, holdsMarker(), name -> false);
    assertThat(names.contains("a")).isTrue();
    database("b");

    assertThat(directory.setReadable(false)).isTrue();
    try {
      assumeFalse(directory.list() != null, "the directory can still be listed (running as a privileged user)");
      names.invalidate();
      assertThat(names.contains("a")).as("the last listing keeps answering").isTrue();
      assertThat(names.contains("b")).isFalse();
    } finally {
      assertThat(directory.setReadable(true)).isTrue();
    }

    await().atMost(10, TimeUnit.SECONDS).pollInterval(100, TimeUnit.MILLISECONDS).until(() -> names.contains("b"));
  }

  private Predicate<File> holdsMarker() {
    return dir -> new File(dir, MARKER).exists();
  }

  private void database(final String name) throws IOException {
    final File dir = new File(directory, name);
    assertThat(dir.mkdirs()).isTrue();
    assertThat(new File(dir, MARKER).createNewFile()).isTrue();
  }
}
