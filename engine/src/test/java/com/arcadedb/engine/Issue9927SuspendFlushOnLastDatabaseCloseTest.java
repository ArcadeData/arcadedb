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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.utility.CallableNoReturn;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Regression test for issue #9927: {@code PageManager.suspendFlushAndExecute} re-read the {@code flushThread} field in
 * its {@code finally}. When the callback closed or dropped the LAST open database of the JVM, the page-manager
 * lifecycle refcount went 1 -> 0, {@code shutdown()} nulled {@code flushThread}, and releasing the suspension threw
 * {@code NullPointerException: Cannot invoke "PageManagerFlushThread.setSuspended(...)" because "this.flushThread" is
 * null}. Both release sites are driven: the first suspender's own {@code finally}, and {@code runAlreadySuspended} for
 * a nested suspender that joined the active window.
 * <p>
 * Deliberately NOT a {@code TestHelper}: its database would hold a lifecycle reference and keep the page manager
 * alive, which is exactly what #9881's test does to steer clear of this bug. Here the database under test must be the
 * only reference, so each test checks the JVM-wide refcount before starting and skips (rather than passing for the
 * wrong reason) if a database leaked by an earlier test class is still holding one.
 */
class Issue9927SuspendFlushOnLastDatabaseCloseTest {
  private static final String DB_NAME    = "Issue9927SuspendFlushOnLastDatabaseCloseTest";
  private static final String DB_PATH    = "target/databases/" + DB_NAME;
  private static final String AFTER_PATH = DB_PATH + "-after";

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
    FileUtils.deleteRecursively(new File(AFTER_PATH));
    assumeThat(PageManager.INSTANCE.getLifecycleRefCount())
        .as("another database is open in this JVM, so closing the one under test cannot shut the page manager down")
        .isZero();
  }

  @AfterEach
  void tearDown() {
    // A failed assertion can leave the database under test (or the AFTER_PATH one) open. Its lifecycle reference
    // would then make every later test of this class skip on the refcount assumption, so close it here.
    for (final Database db : new ArrayList<>(DatabaseFactory.getActiveDatabaseInstances()))
      if (db.getDatabasePath().contains(DB_NAME))
        db.close();
    FileUtils.deleteRecursively(new File(DB_PATH));
    FileUtils.deleteRecursively(new File(AFTER_PATH));
  }

  @Test
  void droppingTheLastOpenDatabaseInsideTheSuspensionDoesNotThrow() throws Exception {
    final DatabaseInternal db = createDatabase();
    final PageManagerFlushThread suspendedOn = PageManager.INSTANCE.getFlushThread();

    runSuspended(db, () -> {
      db.drop();
      assertPageManagerShutDown();
    });

    assertThat(db.isOpen()).isFalse();
    assertThat(new File(DB_PATH)).doesNotExist();
    // The fix releases on the thread that shut down, which is safe only because the close made that thread forget the
    // database's suspension count: pin that coupling, so a refactor of setSuspended cannot quietly break it.
    assertThat(suspendedOn.isSuspended(db)).isFalse();
    assertThat(suspendedOn.setSuspended(db, false)).as("an extra release on the dead thread must be a no-op").isFalse();
    assertPageManagerUsableAgain();
  }

  @Test
  void closingTheLastOpenDatabaseInsideTheSuspensionDoesNotThrow() throws Exception {
    final DatabaseInternal db = createDatabase();

    runSuspended(db, () -> {
      db.close();
      assertPageManagerShutDown();
    });

    assertThat(db.isOpen()).isFalse();
    assertPageManagerUsableAgain();
  }

  @Test
  void droppingTheLastOpenDatabaseInsideANestedSuspensionDoesNotThrow() throws Exception {
    final DatabaseInternal db = createDatabase();
    final AtomicBoolean nestedRan = new AtomicBoolean();

    // The inner call joins the outer window (tryJoinActiveSuspension), so it releases through runAlreadySuspended;
    // once it returns, the outer call releases through its own finally - both after the page manager shut down.
    runSuspended(db, () -> runSuspended(db, () -> {
      db.drop();
      assertPageManagerShutDown();
      nestedRan.set(true);
    }));

    assertThat(nestedRan.get()).isTrue();
    assertThat(db.isOpen()).isFalse();
    assertPageManagerUsableAgain();
  }

  private static DatabaseInternal createDatabase() {
    final DatabaseInternal db = (DatabaseInternal) new DatabaseFactory(DB_PATH).create();
    db.getSchema().createDocumentType("Doc");
    db.transaction(() -> db.newDocument("Doc").set("id", 0).save());
    assertThat(PageManager.INSTANCE.getLifecycleRefCount()).as("the database under test holds the only reference")
        .isEqualTo(1);
    return db;
  }

  /** Proves the scenario reached the 1 -> 0 transition the bug needs, so a pass is not a pass for the wrong reason. */
  private static void assertPageManagerShutDown() {
    assertThat(PageManager.INSTANCE.getLifecycleRefCount()).isZero();
    assertThat(PageManager.INSTANCE.getFlushThread()).as("the last release must have shut the flush thread down")
        .isNull();
  }

  /** The suspension released against a dead flush thread must leave nothing behind that breaks the next database. */
  private static void assertPageManagerUsableAgain() throws Exception {
    try (final Database db = new DatabaseFactory(AFTER_PATH).create()) {
      db.getSchema().createDocumentType("Doc");
      db.transaction(() -> db.newDocument("Doc").set("id", 1).save());
      assertThat(db.countType("Doc", false)).isEqualTo(1);
      final PageManager pageManager = ((DatabaseInternal) db).getPageManager();
      pageManager.suspendFlushAndExecute(db, () -> {
      });
      assertThat(pageManager.isPageFlushingSuspended(db)).isFalse();
    }
    new DatabaseFactory(AFTER_PATH).open().drop();
    assertThat(PageManager.INSTANCE.getLifecycleRefCount()).isZero();
  }

  /**
   * Runs {@code body} inside a real flush suspension. {@code suspendFlushAndExecute} logs and swallows whatever its
   * callback throws, so a failed assertion is carried out and rethrown here instead of passing the test silently. What
   * the release of the suspension throws AFTER the callback - the #9927 NPE - propagates straight out.
   */
  private static void runSuspended(final DatabaseInternal db, final CallableNoReturn body) throws Exception {
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final AtomicBoolean ran = new AtomicBoolean();
    db.getPageManager().suspendFlushAndExecute(db, () -> {
      try {
        body.call();
        ran.set(true);
      } catch (final Throwable t) {
        failure.set(t);
      }
    });
    if (failure.get() instanceof Error error)
      throw error;
    if (failure.get() instanceof Exception exception)
      throw exception;
    assertThat(ran.get()).as("the suspended body must have run to completion").isTrue();
  }
}
