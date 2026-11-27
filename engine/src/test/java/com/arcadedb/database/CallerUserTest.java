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

import com.arcadedb.TestHelper;
import com.arcadedb.security.SecurityDatabaseUser;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * @author lvca (l.garulli@arcadedata.com)
 */
class CallerUserTest extends TestHelper {

  @Test
  void aTaskRunsAsTheUserBoundToTheThreadThatHandedItOver() throws Exception {
    final SecurityDatabaseUser caller = user("caller");
    bind(caller);
    try {
      final Callable<String> task = CallerUser.bind((DatabaseInternal) database, () -> userNameOnThisThread());
      assertThat(onOtherThread(task)).isEqualTo("caller");
    } finally {
      bind(null);
    }
  }

  @Test
  void aTaskOfACallerWithoutUserRunsWithoutUser() throws Exception {
    final ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      // THE THREAD HAS A USER LEFT BY SOMETHING ELSE: A COMMAND WITH NO USER MUST NOT RUN AS THAT USER
      pool.submit(() -> {
        DatabaseContext.INSTANCE.init((DatabaseInternal) database).setCurrentUser(user("left-over"));
        return null;
      }).get();

      final Callable<String> task = CallerUser.bind((DatabaseInternal) database, () -> userNameOnThisThread());
      assertThat(pool.submit(task).get()).isNull();
    } finally {
      pool.submit(() -> DatabaseContext.INSTANCE.removeContext(database.getDatabasePath())).get();
      pool.shutdown();
    }
  }

  @Test
  void theThreadGetsBackTheUserItHadWhateverTheTaskDid() throws Exception {
    final ExecutorService pool = Executors.newSingleThreadExecutor();
    final SecurityDatabaseUser previous = user("previous");
    bind(user("caller"));
    try {
      pool.submit(() -> {
        DatabaseContext.INSTANCE.init((DatabaseInternal) database).setCurrentUser(previous);
        return null;
      }).get();

      assertThat(pool.submit(CallerUser.bind((DatabaseInternal) database, () -> userNameOnThisThread())).get()).isEqualTo("caller");
      assertThat(pool.submit(() -> userNameOnThisThread()).get()).isEqualTo("previous");

      final Callable<Object> failing = CallerUser.bind((DatabaseInternal) database, () -> {
        throw new IllegalStateException("the task failed");
      });
      assertThatThrownBy(() -> pool.submit(failing).get()).isInstanceOf(ExecutionException.class)
          .hasCauseInstanceOf(IllegalStateException.class);
      assertThat(pool.submit(() -> userNameOnThisThread()).get()).isEqualTo("previous");
    } finally {
      bind(null);
      pool.submit(() -> DatabaseContext.INSTANCE.removeContext(database.getDatabasePath())).get();
      pool.shutdown();
    }
  }

  @Test
  void aTaskRunByTheThreadThatHandedItOverKeepsItsTransaction() throws Exception {
    final SecurityDatabaseUser caller = user("caller");
    bind(caller);
    database.begin();
    try {
      final Callable<Boolean> task = CallerUser.bind((DatabaseInternal) database, () -> database.isTransactionActive());
      assertThat(task.call()).as("a pool that runs the task on the submitting thread must not roll its transaction back").isTrue();
      assertThat(userNameOnThisThread()).isEqualTo("caller");
    } finally {
      database.rollback();
      bind(null);
    }
  }

  private String userNameOnThisThread() {
    return database.getCurrentUserName();
  }

  private <T> T onOtherThread(final Callable<T> task) throws Exception {
    final ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      return pool.submit(task).get();
    } finally {
      pool.submit(() -> DatabaseContext.INSTANCE.removeContext(database.getDatabasePath())).get();
      pool.shutdown();
    }
  }

  private void bind(final SecurityDatabaseUser user) {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(user);
  }

  private static SecurityDatabaseUser user(final String name) {
    return new SecurityDatabaseUser() {
      @Override
      public String getName() {
        return name;
      }

      @Override
      public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
        return true;
      }

      @Override
      public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
        return true;
      }

      @Override
      public long getResultSetLimit() {
        return -1L;
      }

      @Override
      public long getReadTimeout() {
        return -1L;
      }
    };
  }
}
