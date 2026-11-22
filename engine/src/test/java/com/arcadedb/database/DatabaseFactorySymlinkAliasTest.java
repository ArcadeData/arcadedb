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
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The registry of open databases was keyed by the absolute, normalised path, which does not see through a symbolic link: a second
 * factory reaching an open database by the link passed the "already in use" check, built an instance over the same files and was
 * refused by the file lock afterwards. Its refusal closed the lock file it had opened, and on Linux and macOS closing ANY descriptor
 * of a file releases ALL the locks the process holds on it: the winner kept its in-process lock but no other process was kept out
 * of the database any more, which is the one protection {@code database.lck} exists for.
 */
class DatabaseFactorySymlinkAliasTest {
  private static final String PATH         = "target/databases/DatabaseFactorySymlinkAliasTest";
  private static final String ALIAS        = "target/databases/DatabaseFactorySymlinkAliasTestLink";
  private static final String PARENT_ALIAS = "target/DatabaseFactorySymlinkAliasTestParentLink";
  private static final String NEW_DATABASE = "DatabaseFactorySymlinkAliasTestNew";
  private static final String OTHER        = "target/databases/DatabaseFactorySymlinkAliasTestOther";

  @BeforeEach
  void setUp() throws IOException {
    cleanUp();
    try (final Database db = new DatabaseFactory(PATH).create()) {
      db.command("sql", "CREATE DOCUMENT TYPE T");
    }
    try {
      Files.createSymbolicLink(Path.of(ALIAS).toAbsolutePath(), Path.of(PATH).toAbsolutePath());
    } catch (final UnsupportedOperationException | IOException | SecurityException e) {
      assumeTrue(false, "symbolic links are not available here: " + e);
    }
  }

  @AfterEach
  void tearDown() throws IOException {
    cleanUp();
  }

  private static void cleanUp() throws IOException {
    final Database leftOpen = DatabaseFactory.getActiveDatabaseInstance(PATH);
    if (leftOpen != null && leftOpen.isOpen())
      leftOpen.close();
    Files.deleteIfExists(Path.of(ALIAS).toAbsolutePath());
    Files.deleteIfExists(Path.of(PARENT_ALIAS).toAbsolutePath());
    FileUtils.deleteRecursively(new File(PATH));
    FileUtils.deleteRecursively(new File(OTHER));
  }

  /**
   * A caller waits on the monitor of the key its path resolved to when it started. If the directory behind the path changes while
   * it waits (here the link is pointed at another database; a failed create taking back the directory it made is the other way),
   * it must take the monitor of the key the path resolves to NOW, the one every later caller of that directory takes.
   */
  @Test
  @Timeout(60)
  void aCallerWhosePathResolvesElsewhereOnceItGetsTheMonitorTakesTheMonitorOfTheNewKey() throws Exception {
    try (final Database other = new DatabaseFactory(OTHER).create()) {
      other.command("sql", "CREATE DOCUMENT TYPE O");
    }

    final CountDownLatch inFirstOpen = new CountDownLatch(1);
    final CountDownLatch releaseFirstOpen = new CountDownLatch(1);
    final DatabaseFactory first = new DatabaseFactory(PATH);
    first.registerCallback(DatabaseInternal.CALLBACK_EVENT.DB_AFTER_OPEN, () -> {
      inFirstOpen.countDown();
      releaseFirstOpen.await();
      return null;
    });
    final AtomicReference<Database> firstOpened = new AtomicReference<>();
    final Thread firstThread = new Thread(() -> firstOpened.set(first.open()), "first-open");

    final AtomicBoolean heldTheMonitorOfTheNewKey = new AtomicBoolean();
    final DatabaseFactory second = new DatabaseFactory(ALIAS);
    second.registerCallback(DatabaseInternal.CALLBACK_EVENT.DB_AFTER_OPEN, () -> {
      heldTheMonitorOfTheNewKey.set(DatabaseFactory.isPathLockInUse(OTHER));
      return null;
    });
    final AtomicReference<Database> secondOpened = new AtomicReference<>();
    final Thread secondThread = new Thread(() -> secondOpened.set(second.open()), "second-open");

    firstThread.start();
    try {
      assertThat(inFirstOpen.await(30, TimeUnit.SECONDS)).isTrue();
      // THE LINK STILL LEADS TO PATH: THE SECOND OPEN WAITS ON THE MONITOR OF PATH, HELD BY THE FIRST
      secondThread.start();
      while (secondThread.getState() != Thread.State.BLOCKED && secondThread.getState() != Thread.State.TERMINATED)
        Thread.onSpinWait();
      assertThat(secondThread.getState()).isEqualTo(Thread.State.BLOCKED);

      Files.delete(Path.of(ALIAS).toAbsolutePath());
      Files.createSymbolicLink(Path.of(ALIAS).toAbsolutePath(), Path.of(OTHER).toAbsolutePath());
    } finally {
      releaseFirstOpen.countDown();
      firstThread.join(30_000);
      secondThread.join(30_000);
    }

    try {
      assertThat(firstOpened.get()).isNotNull();
      assertThat(secondOpened.get()).as("the link now leads to another database, which nobody has open").isNotNull();
      assertThat(secondOpened.get().getSchema().existsType("O")).isTrue();
      assertThat(heldTheMonitorOfTheNewKey.get()).as("opened under the monitor of the directory the link leads to now").isTrue();
    } finally {
      if (firstOpened.get() != null)
        firstOpened.get().close();
      if (secondOpened.get() != null)
        secondOpened.get().close();
    }
  }

  @Test
  @Timeout(60)
  void anOpenThroughALinkOfAnOpenDatabaseIsRefusedAsActiveBeforeAnythingIsBuilt() {
    try (final Database db = new DatabaseFactory(PATH).open()) {
      assertThatThrownBy(() -> new DatabaseFactory(ALIAS).open()).isInstanceOf(DatabaseOperationException.class)
          .hasMessageContaining("Found active instance");
      assertThatThrownBy(() -> new DatabaseFactory(ALIAS).create()).isInstanceOf(DatabaseOperationException.class)
          .hasMessageContaining("Found active instance");

      assertThat(DatabaseFactory.getActiveDatabaseInstance(ALIAS)).isSameAs(db);
      assertThat(DatabaseFactory.getActiveDatabaseInstance(PATH)).isSameAs(db);
      db.transaction(() -> db.newDocument("T").set("i", 1).save());
      assertThat(db.countType("T", false)).isEqualTo(1);
    }
    assertThat(DatabaseFactory.getActiveDatabaseInstance(ALIAS)).isNull();
    assertThat(DatabaseFactory.getActiveDatabaseInstance(PATH)).isNull();
  }

  /**
   * A {@code create} computes its key before its directory exists. When a PARENT of that directory is reached through a link
   * (or is {@code /tmp} on macOS, a link to {@code /private/tmp}), the key it took must still be the one an open of the same
   * directory computes once it exists, or the two take different monitors and the open builds an instance over the files the
   * create is still writing.
   */
  @Test
  @Timeout(60)
  void aDatabaseNotCreatedYetHasTheKeyItHasOnceCreatedEvenThroughALinkOfItsParent() throws IOException {
    final Path parentLink = Path.of(PARENT_ALIAS).toAbsolutePath();
    final String direct = Path.of(PATH).getParent().resolve(NEW_DATABASE).toString();
    final String viaLink = Path.of(PARENT_ALIAS).resolve(NEW_DATABASE).toString();
    FileUtils.deleteRecursively(new File(direct));
    Files.createSymbolicLink(parentLink, Path.of(PATH).getParent().toAbsolutePath());
    try {
      final Path keyBeforeCreate = DatabaseFactory.getRegistryKey(viaLink);
      assertThat(DatabaseFactory.getRegistryKey(direct)).isEqualTo(keyBeforeCreate);

      try (final Database db = new DatabaseFactory(viaLink).create()) {
        assertThat(DatabaseFactory.getRegistryKey(direct)).isEqualTo(keyBeforeCreate);
        assertThat(DatabaseFactory.getRegistryKey(viaLink)).isEqualTo(keyBeforeCreate);
        assertThat(DatabaseFactory.getActiveDatabaseInstance(direct)).isSameAs(db);
        assertThatThrownBy(() -> new DatabaseFactory(direct).open()).isInstanceOf(DatabaseOperationException.class)
            .hasMessageContaining("Found active instance");
      }
      assertThat(DatabaseFactory.getActiveDatabaseInstance(direct)).isNull();
    } finally {
      final Database leftOpen = DatabaseFactory.getActiveDatabaseInstance(direct);
      if (leftOpen != null && leftOpen.isOpen())
        leftOpen.close();
      Files.deleteIfExists(parentLink);
      FileUtils.deleteRecursively(new File(direct));
    }
  }

  @Test
  @Timeout(120)
  void aRefusedOpenThroughALinkLeavesTheDatabaseLockedAgainstOtherProcesses() throws Exception {
    try (final Database db = new DatabaseFactory(PATH).open()) {
      assertThatThrownBy(() -> new DatabaseFactory(ALIAS).open()).isInstanceOf(DatabaseOperationException.class);

      // ANOTHER PROCESS: THE LOCK OF THIS JVM ON database.lck IS WHAT KEEPS IT OUT, IF IT WAS RELEASED THE PROBE TAKES IT AND EXITS WITH 3
      final String lockFile = new File(PATH, "database.lck").getAbsolutePath();
      final Process probe = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(), "-cp",
          System.getProperty("java.class.path"), LockProbe.class.getName(), lockFile).redirectErrorStream(true).start();
      assertThat(probe.waitFor(60, TimeUnit.SECONDS)).isTrue();
      assertThat(probe.exitValue()).as("the other process found database.lck %s", new String(probe.getInputStream().readAllBytes()))
          .isEqualTo(LockProbe.LOCKED);
      assertThat(db.isOpen()).isTrue();
    }
  }

  /** Runs in its own JVM: tries the lock on the file named by the argument and says whether it was held by someone else. */
  public static final class LockProbe {
    static final int LOCKED   = 0;
    static final int ACQUIRED = 3;

    public static void main(final String[] args) throws IOException {
      try (final FileChannel channel = FileChannel.open(Path.of(args[0]), StandardOpenOption.READ, StandardOpenOption.WRITE)) {
        final FileLock lock = channel.tryLock();
        System.out.println(lock == null ? "locked by another process" : "ACQUIRED: nobody holds the lock");
        System.exit(lock == null ? LOCKED : ACQUIRED);
      }
    }
  }
}
