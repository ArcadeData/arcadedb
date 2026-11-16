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
package com.arcadedb.server.ha.raft;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12251: on the leader, a transaction with nothing to replicate ran its after-commit and after-completion callbacks inside
 * the database read lock of its commit. Every TIMESERIES insert into a type with a continuous aggregate ends that way (the samples
 * commit in a transaction of their own) and owes the aggregate a refresh, whose own commit waits for any schema session in
 * progress. The session of a late sample, opened by another insert, waits for the write lock that read lock holds off: the two
 * waited on each other until the quorum timeout, and with several writers every insert took 10 to 60 s.
 * <p>
 * The callbacks now run once the commit has released the read lock: a schema change made while they run is not held off by them.
 */
class Issue12251ReadOnlyCommitCallbacksOutsideReadLockTest extends BaseRaftHATest {

  @Override
  protected int getServerCount() {
    return 2;
  }

  @Test
  void aSchemaChangeMadeWhileTheCallbacksOfAReadOnlyCommitRunIsNotHeldOffByThem() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("leader elected").isGreaterThanOrEqualTo(0);
    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    final AtomicBoolean callbackRan = new AtomicBoolean();
    final AtomicBoolean schemaChangeEndedWhileTheCallbackRan = new AtomicBoolean();
    final AtomicReference<Throwable> schemaChangeFailure = new AtomicReference<>();
    final Thread[] schemaChange = new Thread[1];

    // nothing written: the commit takes the read-only arm, as the transaction of a TIMESERIES insert does
    leaderDb.transaction(() -> ((DatabaseInternal) leaderDb).getTransaction()
        .addAfterCompletionCallbackIfAbsent("issue12251", () -> {
          callbackRan.set(true);
          // the schema session of another client, as the late-sample record of a concurrent insert opens it
          schemaChange[0] = new Thread(() -> {
            try {
              leaderDb.command("sql", "CREATE DOCUMENT TYPE Issue12251").close();
            } catch (final Throwable t) {
              schemaChangeFailure.set(t);
            }
          }, "schema-change-12251");
          schemaChange[0].start();
          try {
            schemaChange[0].join(20_000);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          schemaChangeEndedWhileTheCallbackRan.set(!schemaChange[0].isAlive());
        }));

    try {
      assertThat(callbackRan.get()).as("the read-only commit ran its callback").isTrue();
      assertThat(schemaChangeEndedWhileTheCallbackRan.get())
          .as("the schema change completed while the callback of the read-only commit was running").isTrue();
      assertThat(schemaChangeFailure.get()).isNull();
      assertThat(leaderDb.getSchema().existsType("Issue12251")).isTrue();
    } finally {
      if (schemaChange[0] != null)
        try {
          schemaChange[0].join(60_000);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
    }
  }

  /** The after-commit callbacks (the ones a materialized view registers) run outside the read lock too. */
  @Test
  void aSchemaChangeMadeWhileTheAfterCommitCallbackOfAReadOnlyCommitRunsIsNotHeldOffByIt() {
    final int leaderIndex = findLeaderIndex();
    assertThat(leaderIndex).as("leader elected").isGreaterThanOrEqualTo(0);
    final Database leaderDb = getServerDatabase(leaderIndex, getDatabaseName());

    final AtomicBoolean callbackRan = new AtomicBoolean();
    final AtomicBoolean schemaChangeEndedWhileTheCallbackRan = new AtomicBoolean();
    final AtomicReference<Throwable> schemaChangeFailure = new AtomicReference<>();
    final Thread[] schemaChange = new Thread[1];

    leaderDb.transaction(() -> ((DatabaseInternal) leaderDb).getTransaction().addAfterCommitCallback(() -> {
      callbackRan.set(true);
      schemaChange[0] = new Thread(() -> {
        try {
          leaderDb.command("sql", "CREATE DOCUMENT TYPE Issue12251AfterCommit").close();
        } catch (final Throwable t) {
          schemaChangeFailure.set(t);
        }
      }, "schema-change-12251-after-commit");
      schemaChange[0].start();
      try {
        schemaChange[0].join(20_000);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      schemaChangeEndedWhileTheCallbackRan.set(!schemaChange[0].isAlive());
    }));

    try {
      assertThat(callbackRan.get()).as("the read-only commit ran its after-commit callback").isTrue();
      assertThat(schemaChangeEndedWhileTheCallbackRan.get())
          .as("the schema change completed while the after-commit callback of the read-only commit was running").isTrue();
      assertThat(schemaChangeFailure.get()).isNull();
      assertThat(leaderDb.getSchema().existsType("Issue12251AfterCommit")).isTrue();
    } finally {
      if (schemaChange[0] != null)
        try {
          schemaChange[0].join(60_000);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
    }
  }
}
