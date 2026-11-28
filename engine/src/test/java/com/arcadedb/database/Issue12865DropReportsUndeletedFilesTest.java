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
package com.arcadedb.database;

import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * {@code LocalDatabase.drop()} closed the instance, ran the recursive delete, which only logs a WARNING for what it cannot remove,
 * and returned normally. A drop that left the files behind (an entry owned by another user, a read-only entry, a volume that refuses
 * the unlink, a file held open on Windows) was reported as done while the database was still on disk: {@code create()} of the path
 * was refused as "already exists" and {@code open()} brought the dropped data back. The drop now fails when files are left.
 * <p>
 * A read-only directory stands for every cause: POSIX only, and skipped where the permission is not enforced (running as root).
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
@DisabledOnOs(OS.WINDOWS)
class Issue12865DropReportsUndeletedFilesTest {
  private static final String PARENT = "target/databases/Issue12865DropReportsUndeletedFiles";
  private static final String PATH   = PARENT + "/db";

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(PARENT));
    try (final Database db = new DatabaseFactory(PATH).create()) {
      db.getSchema().createDocumentType("T");
      db.transaction(() -> db.newDocument("T").set("k", 1).save());
    }
  }

  @AfterEach
  void tearDown() {
    new File(PARENT).setExecutable(true);
    new File(PARENT).setWritable(true);
    new File(PATH).setReadable(true);
    new File(PATH).setWritable(true);
    final Database leftOpen = DatabaseFactory.getActiveDatabaseInstance(PATH);
    if (leftOpen != null && leftOpen.isOpen())
      leftOpen.drop();
    FileUtils.deleteRecursively(new File(PARENT));
  }

  @Test
  @Timeout(60)
  void aDropThatCannotDeleteTheFilesFailsAndReleasesThePath() {
    final DatabaseFactory factory = new DatabaseFactory(PATH);
    final Database db = factory.open();
    final File directory = new File(PATH);

    directory.setWritable(false);
    try {
      assumeTrue(!directory.canWrite(), "the directory cannot be made read-only here (running as root?)");

      assertThatThrownBy(db::drop).isInstanceOf(DatabaseOperationException.class)
          .hasMessageContaining("was closed but its files could not be deleted").hasMessageContaining("schema.json");
    } finally {
      directory.setWritable(true);
    }

    // THE INSTANCE IS CLOSED AND THE PATH IS RELEASED, WHATEVER BECAME OF THE DELETE
    assertThat(db.isOpen()).isFalse();
    assertThat(DatabaseFactory.getActiveDatabaseInstance(PATH)).isNull();

    // WHAT THE DROP COULD NOT DELETE IS STILL A DATABASE, AND THE CALLER WAS TOLD SO: IT OPENS AGAIN, AND A DROP THAT CAN DELETE IT
    // REMOVES IT
    assertThat(factory.exists()).isTrue();
    final Database reopened = new DatabaseFactory(PATH).open();
    // READ THE RECORD ITSELF: countType() CAN ANSWER FROM THE CACHED BUCKET COUNTER
    try (final ResultSet rows = reopened.query("sql", "SELECT k FROM T")) {
      assertThat(rows.hasNext()).isTrue();
      assertThat(rows.next().<Integer>getProperty("k")).isEqualTo(1);
      assertThat(rows.hasNext()).isFalse();
    }
    reopened.drop();

    assertThat(directory).doesNotExist();
    assertThat(DatabaseFactory.getActiveDatabaseInstance(PATH)).isNull();
  }

  @Test
  @Timeout(60)
  void aDirectoryLeftBehindWhoseContentCannotBeListedIsAFailedDrop() {
    final Database db = new DatabaseFactory(PATH).open();
    final File directory = new File(PATH);

    // NEITHER READABLE NOR WRITABLE: THE DELETE CANNOT LIST NOR REMOVE ANYTHING, AND list() ANSWERS NULL, WHICH IS NOT "EMPTY"
    directory.setReadable(false);
    directory.setWritable(false);
    try {
      assumeTrue(!directory.canRead() && !directory.canWrite(), "the directory cannot be made unreadable here (running as root?)");

      assertThatThrownBy(db::drop).isInstanceOf(DatabaseOperationException.class)
          .hasMessageContaining("was closed but its files could not be deleted").hasMessageContaining("cannot be inspected");
    } finally {
      directory.setReadable(true);
      directory.setWritable(true);
    }

    assertThat(DatabaseFactory.getActiveDatabaseInstance(PATH)).isNull();
    assertThat(new DatabaseFactory(PATH).exists()).isTrue();
  }

  @Test
  @Timeout(60)
  void aDirectoryHiddenByAnAncestorWithoutSearchPermissionIsAFailedDrop() {
    final Database db = new DatabaseFactory(PATH).open();
    final File parent = new File(PARENT);

    // THE PARENT DENIES SEARCH: File.exists() ANSWERS FALSE FOR THE DATABASE DIRECTORY, THE DELETE SKIPS IT, AND THE FILES STAY
    parent.setExecutable(false);
    try {
      assumeTrue(!new File(PATH).exists(), "search permission is not enforced here (running as root?)");

      assertThatThrownBy(db::drop).isInstanceOf(DatabaseOperationException.class)
          .hasMessageContaining("was closed but its files could not be deleted").hasMessageContaining("cannot be inspected");
    } finally {
      parent.setExecutable(true);
    }

    assertThat(DatabaseFactory.getActiveDatabaseInstance(PATH)).isNull();
    assertThat(new DatabaseFactory(PATH).exists()).isTrue();
  }

  @Test
  @Timeout(60)
  void anEmptyDirectoryLeftBehindIsNotAFailedDrop() {
    final Database db = new DatabaseFactory(PATH).open();
    final File parent = new File(PARENT);
    final File directory = new File(PATH);

    // THE FILES CAN BE DELETED BUT NOT THE DIRECTORY ITSELF, AS FOR A MOUNT POINT: NO DATABASE IS LEFT, SO THE DROP SUCCEEDS
    parent.setWritable(false);
    try {
      assumeTrue(!parent.canWrite(), "the directory cannot be made read-only here (running as root?)");

      db.drop();
    } finally {
      parent.setWritable(true);
    }

    assertThat(directory).isDirectory();
    assertThat(directory.list()).isEmpty();
    assertThat(DatabaseFactory.getActiveDatabaseInstance(PATH)).isNull();
    assertThat(new DatabaseFactory(PATH).exists()).isFalse();

    try (final Database recreated = new DatabaseFactory(PATH).create()) {
      assertThat(recreated.getSchema().existsType("T")).isFalse();
    }
  }
}
