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

import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12025: the engine's list of the databases whose last close left their committed pages to the next open's recovery
 * without forcing them. The Raft snapshot checkpoint forces the data files of the OPEN databases only, on the premise that a
 * closed one was forced when it closed, and a crash-equivalent close (a fence, a wedged flush, a failed fsync) breaks it: its
 * pages may be in the OS cache only, since the WAL of a replicated commit is not forced (#11746) and a follower writes none.
 * The list is what lets the checkpoint see such a database.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class Issue12025CloseLeftPagesToRecoveryTest {

  @TempDir
  Path tempDir;

  private DatabaseFactory factory;
  private Path            dbPath;

  @BeforeEach
  void setUp() {
    dbPath = tempDir.resolve("closeLeftPages").toAbsolutePath().normalize();
    factory = new DatabaseFactory(dbPath.toString());
  }

  @AfterEach
  void tearDown() {
    LocalDatabase.TEST_AFTER_MARKED_CLOSED_HOOK = null;
    if (DatabaseFactory.getActiveDatabaseInstance(dbPath.toString()) instanceof Database open && open.isOpen())
      open.drop();
    else if (factory.exists())
      factory.open().drop();
    factory.close();
  }

  @Test
  void aCleanCloseIsNotListed() {
    final Database db = createWithData();
    db.close();

    assertThat(listed()).as("a clean close forced every file, so nothing of the database is at risk").isEmpty();
  }

  @Test
  void aFencedCloseIsListedUntilANewerInstanceForcesTheFiles() {
    final LocalDatabase db = (LocalDatabase) createWithData();
    db.fenceForRecovery("test: issue #12025");
    db.close();

    assertThat(new File(dbPath.toFile(), "database.lck")).as("the fixture: the close was crash-equivalent").exists();
    assertThat(listed()).as("a close that left its pages to the recovery is listed").containsExactly(dbPath);

    final LocalDatabase reopened = (LocalDatabase) factory.open();
    assertThat(listed()).as("opening the database is not enough: its recovered files are not forced yet").containsExactly(dbPath);

    assertThat(reopened.getPageManager().flushAndSyncDatabase(reopened, 5_000L)).isTrue();
    assertThat(listed()).as("the newer instance forced every file the recovery left unsynced").isEmpty();
    assertThat(reopened.query("sql", "SELECT count(*) AS c FROM Doc").next().<Long>getProperty("c")).isEqualTo(10L);
  }

  @Test
  void aFencedCloseIsUnlistedByTheCleanCloseOfTheNextInstance() {
    final LocalDatabase db = (LocalDatabase) createWithData();
    db.fenceForRecovery("test: issue #12025");
    db.close();
    assertThat(listed()).containsExactly(dbPath);

    factory.open().close();

    assertThat(listed()).as("the next instance recovered the pages and its clean close forced them").isEmpty();
  }

  @Test
  void aFencedCloseIsUnlistedByTheDropOfTheDatabase() {
    final LocalDatabase db = (LocalDatabase) createWithData();
    db.fenceForRecovery("test: issue #12025");
    db.close();
    assertThat(listed()).containsExactly(dbPath);

    factory.open().drop();

    assertThat(listed()).as("a dropped database has nothing left to lose").isEmpty();
  }

  @Test
  void aFencedCloseIsForgottenOnceItsDirectoryIsGone() {
    final LocalDatabase db = (LocalDatabase) createWithData();
    db.fenceForRecovery("test: issue #12025");
    db.close();
    assertThat(listed()).containsExactly(dbPath);

    FileUtils.deleteRecursively(dbPath.toFile());

    assertThat(listed()).as("a path with no database left is not reported").isEmpty();
  }

  /**
   * A checkpoint that skips the instance because it already reports itself closed must find it listed, whichever way the
   * close is going to end: the close decides only afterwards whether it forced the files.
   */
  @Test
  void aCloseInProgressIsListedFromTheMomentTheInstanceReportsItselfClosed() {
    final LocalDatabase db = (LocalDatabase) createWithData();
    final List<Boolean> seen = new ArrayList<>();
    LocalDatabase.TEST_AFTER_MARKED_CLOSED_HOOK = () -> {
      seen.add(db.isOpen());
      seen.add(listed().contains(dbPath));
    };

    db.close();

    assertThat(seen).as("closed, and listed at the same time").containsExactly(false, true);
    assertThat(listed()).as("the close was clean in the end, so it unlisted itself").isEmpty();
  }

  /** A sync of the instance that is itself closing, or of the one that listed the path, does not unlist it. */
  @Test
  void aSyncOfTheInstanceThatListedThePathDoesNotUnlistIt() {
    final LocalDatabase db = (LocalDatabase) createWithData();
    DatabaseFactory.markClosingWithPagesAtRisk(db);

    assertThat(db.getPageManager().flushAndSyncDatabase(db, 5_000L)).isTrue();

    assertThat(listed()).as("only a NEWER instance, whose open ran the recovery, answers for a listed close").containsExactly(dbPath);
    db.close();
    assertThat(listed()).isEmpty();
  }

  private Database createWithData() {
    final Database db = factory.create();
    db.getSchema().createDocumentType("Doc");
    db.transaction(() -> {
      for (int i = 0; i < 10; i++)
        db.newDocument("Doc").set("v", i).save();
    });
    return db;
  }

  private List<Path> listed() {
    final List<Path> result = new ArrayList<>();
    for (final Path p : DatabaseFactory.getDatabasesClosedWithPagesLeftToRecovery())
      if (p.equals(dbPath))
        result.add(p);
    return result;
  }
}
