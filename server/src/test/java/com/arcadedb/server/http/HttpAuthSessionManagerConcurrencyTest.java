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
package com.arcadedb.server.http;

import com.arcadedb.server.security.ServerSecurityUser;

import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Concurrency regression for issue #5033: the authenticated-session map must only be read and mutated
 * under the read/write lock. Exercises {@code createSession} / {@code getActiveSessionCount} /
 * {@code checkSessionsValidity} / {@code getActiveSessions} from many threads to prove none of the
 * lock-free reads corrupt the underlying {@code HashMap}.
 */
class HttpAuthSessionManagerConcurrencyTest {

  private ServerSecurityUser createMockUser(final String username) {
    final ServerSecurityUser user = mock(ServerSecurityUser.class);
    when(user.getName()).thenReturn(username);
    when(user.getAuthorizedDatabases()).thenReturn(Set.of());
    return user;
  }

  @Test
  void concurrentReadsAndWritesDoNotThrow() throws Exception {
    final HttpAuthSessionManager manager = new HttpAuthSessionManager(30_000L);
    try {
      final ServerSecurityUser user = createMockUser("stress");

      final int writers = 8;
      final int readers = 8;
      final int opsPerThread = 5_000;
      final CountDownLatch start = new CountDownLatch(1);
      final CountDownLatch done = new CountDownLatch(writers + readers);
      final AtomicReference<Throwable> failure = new AtomicReference<>();

      for (int w = 0; w < writers; w++) {
        new Thread(() -> {
          try {
            start.await();
            for (int i = 0; i < opsPerThread; i++) {
              final HttpAuthSession session = manager.createSession(user);
              manager.removeSession(session.getToken());
            }
          } catch (final Throwable e) {
            failure.compareAndSet(null, e);
          } finally {
            done.countDown();
          }
        }, "auth-writer-" + w).start();
      }

      for (int r = 0; r < readers; r++) {
        new Thread(() -> {
          try {
            start.await();
            for (int i = 0; i < opsPerThread; i++) {
              manager.getActiveSessionCount();
              manager.getActiveSessions();
              manager.checkSessionsValidity();
            }
          } catch (final Throwable e) {
            failure.compareAndSet(null, e);
          } finally {
            done.countDown();
          }
        }, "auth-reader-" + r).start();
      }

      start.countDown();
      assertThat(done.await(60, TimeUnit.SECONDS)).as("all threads finished").isTrue();
      assertThat(failure.get()).as("no exception from concurrent session access").isNull();
    } finally {
      manager.close();
    }

    // close() must clear the map under the write lock; the count reads zero afterwards.
    assertThat(manager.getActiveSessionCount()).isEqualTo(0);
  }
}
