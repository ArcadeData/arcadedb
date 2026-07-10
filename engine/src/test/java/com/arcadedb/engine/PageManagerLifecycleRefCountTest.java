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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * #4927: the JVM-wide PageManager lifecycle is refcounted instead of keyed on the racy
 * ACTIVE_INSTANCES.isEmpty() check-then-act. These tests pin the lifecycle contract deterministically; the
 * original interleavings (a close nulling the flush thread under a mid-flight open on another thread, or two
 * opens double-starting it) are pure races, so the contract that makes them impossible is what is tested.
 */
@ResourceLock("PageManager.INSTANCE")
class PageManagerLifecycleRefCountTest {

  private static final String DB_A = "target/databases/PageManagerLifecycleRefCountTestA";
  private static final String DB_B = "target/databases/PageManagerLifecycleRefCountTestB";

  @BeforeEach
  void normalizeGlobalState() {
    // #5070 review: PageManager.INSTANCE is process-global. Force-reset the refcount to a known zero
    // baseline so these assertions are not order-dependent on another test leaking an open database (or a
    // kill() without the paired close()) in the same surefire fork. The class-level @ResourceLock declares
    // the singleton dependency: this force-teardown must never run concurrently with another class holding
    // an open database (safe under the current sequential surefire config).
    PageManager.INSTANCE.close();
  }

  @AfterEach
  void cleanup() {
    for (final String path : new String[] { DB_A, DB_B }) {
      final DatabaseFactory factory = new DatabaseFactory(path);
      try {
        if (factory.exists())
          factory.open().drop();
      } catch (final Exception e) {
        // removed on disk below
      }
      factory.close();
      FileUtils.deleteRecursively(new File(path));
    }
  }

  @Test
  void configureWithoutOpenDatabasesDoesNotStartAFlushThread() {
    // The GlobalConfiguration PROFILE setter calls configure() at configuration time, possibly before any
    // database exists. The old code then STARTED a flush thread for zero databases - the same
    // lifecycle-divorced-from-database-count defect class as the #4927 races - and leaked it until the next
    // lifecycle transition. With the refcount, configure() with no references is a no-op.
    PageManager.INSTANCE.configure();
    assertThat(PageManager.INSTANCE.getFlushThread())
        .as("configure() with no open database must not start a flush thread (#4927)")
        .isNull();
  }

  @Test
  void closingOneDatabaseNeverTearsDownTheManagerForOthers() {
    final DatabaseFactory factoryA = new DatabaseFactory(DB_A);
    try (final DatabaseFactory factoryB = new DatabaseFactory(DB_B)) {
      final Database dbA = factoryA.create();
      final Database dbB = factoryB.create();

      dbA.drop();

      assertThat(PageManager.INSTANCE.getFlushThread())
          .as("the flush thread must survive while any database is open").isNotNull();
      assertThat(PageManager.INSTANCE.getFlushThread().isAlive()).isTrue();

      // The surviving database still works end to end (cache misses, flush scheduling).
      dbB.getSchema().createDocumentType("Doc");
      dbB.transaction(() -> dbB.newDocument("Doc").set("v", 1).save());
      assertThat(dbB.query("sql", "SELECT count(*) as c FROM Doc").next().<Long>getProperty("c")).isEqualTo(1L);

      dbB.drop();
      assertThat(PageManager.INSTANCE.getFlushThread())
          .as("the LAST release tears the manager down").isNull();
    } finally {
      factoryA.close();
    }
  }

  @Test
  void concurrentOpenCloseHammerNeverLosesTheFlushThread() throws Exception {
    // The original race shape: one thread repeatedly opening/closing database A while another opens B,
    // writes and closes. With the old isEmpty() heuristics this interleaving could null the flush thread
    // under the open database (NPE on the first cache miss) or double-start it. The refcount makes both
    // impossible; this hammer asserts no thread observes a broken lifecycle.
    final int iterations = 30;
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final CountDownLatch start = new CountDownLatch(1);

    final Thread tA = new Thread(() -> {
      try {
        start.await();
        final DatabaseFactory factory = new DatabaseFactory(DB_A);
        for (int i = 0; i < iterations; i++) {
          final Database db = factory.exists() ? factory.open() : factory.create();
          db.close();
        }
        factory.close();
      } catch (final Throwable e) {
        failure.compareAndSet(null, e);
      }
    }, "hammer-A");

    final Thread tB = new Thread(() -> {
      try {
        start.await();
        final DatabaseFactory factory = new DatabaseFactory(DB_B);
        for (int i = 0; i < iterations; i++) {
          final Database db = factory.exists() ? factory.open() : factory.create();
          if (!db.getSchema().existsType("Doc"))
            db.getSchema().createDocumentType("Doc");
          final int it = i;
          db.transaction(() -> db.newDocument("Doc").set("i", it).save());
          db.close();
        }
        factory.close();
      } catch (final Throwable e) {
        failure.compareAndSet(null, e);
      }
    }, "hammer-B");

    tA.start();
    tB.start();
    start.countDown();
    tA.join(120_000);
    tB.join(120_000);

    assertThat(failure.get()).as("no thread may observe a torn-down PageManager while its database is open").isNull();
    assertThat(PageManager.INSTANCE.getFlushThread()).as("all references released: manager torn down").isNull();
  }

  @Test
  void redundantDoubleCloseDoesNotStealAnotherDatabasesReference() {
    // #5070 review (the acute regression): close() is documented-idempotent (the lambda early-returns on
    // !open), but the lifecycle release ran unconditionally - a defensive double-close consumed a SECOND
    // reference and tore the flush thread down under the other open database.
    final DatabaseFactory factoryA = new DatabaseFactory(DB_A);
    try (final DatabaseFactory factoryB = new DatabaseFactory(DB_B)) {
      final Database dbA = factoryA.create();
      final Database dbB = factoryB.create();

      dbA.close();
      dbA.close(); // redundant, must be a full no-op

      assertThat(PageManager.INSTANCE.getFlushThread())
          .as("a redundant double-close must not consume another database's lifecycle reference (#5070)")
          .isNotNull();

      dbB.getSchema().createDocumentType("Doc");
      dbB.transaction(() -> dbB.newDocument("Doc").set("v", 1).save());
      dbB.close();

      assertThat(PageManager.INSTANCE.getFlushThread()).isNull();
    } finally {
      factoryA.close();
    }
  }

  @Test
  void startupFailureRollsBackTheAcquiredReference() {
    // #5070 review: startup() can throw (negative MAX_PAGE_RAM) AFTER the refcount increment; without the
    // rollback the counter wedged at 1 with no flush thread and every later open ran against a dead manager.
    final Object previous = GlobalConfiguration.MAX_PAGE_RAM.getValue();
    GlobalConfiguration.MAX_PAGE_RAM.setValue(-2);
    try {
      try (final DatabaseFactory factory = new DatabaseFactory(DB_A)) {
        assertThatThrownBy(factory::create)
            .as("misconfigured startup must fail the open").hasMessageContaining("configuration is invalid");
      }
    } finally {
      GlobalConfiguration.MAX_PAGE_RAM.setValue(previous);
    }

    // The failed acquire left no residue: a healthy open now starts the manager normally.
    try (final DatabaseFactory factory = new DatabaseFactory(DB_A)) {
      final Database db = factory.create();
      assertThat(PageManager.INSTANCE.getFlushThread())
          .as("the wedged-counter state must not survive a startup failure (#5070)").isNotNull();
      db.getSchema().createDocumentType("Doc");
      db.transaction(() -> db.newDocument("Doc").set("v", 1).save());
      db.drop();
    }
  }

  @Test
  void liveConfigureIsRefusedInsteadOfSwappingTheFlushThreadUnderReaders() {
    // #5070 review round 2: a live shutdown+startup swap in configure() raced the hot paths, which read
    // flushThread/readCache without the lifecycle lock - a concurrent flush could NPE on the transient
    // null. A profile change while databases are open is now refused loudly; the thread survives untouched.
    try (final DatabaseFactory factory = new DatabaseFactory(DB_A)) {
      final Database db = factory.create();
      final PageManagerFlushThread before = PageManager.INSTANCE.getFlushThread();
      assertThat(before).isNotNull();

      PageManager.INSTANCE.configure();

      assertThat(PageManager.INSTANCE.getFlushThread())
          .as("configure() with open databases must not swap the live flush thread (#5070)")
          .isSameAs(before);
      assertThat(before.isAlive()).isTrue();
      db.drop();
    }
  }
}
