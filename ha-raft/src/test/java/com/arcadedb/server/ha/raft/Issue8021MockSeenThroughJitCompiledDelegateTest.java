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

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.LocalDatabase;
import com.arcadedb.utility.SubclassMocks;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.http.HttpClient;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/**
 * Issue #8021: a mock handed to {@code RaftReplicatedDatabase} after the JIT compiled its delegation against the real
 * {@code LocalDatabase} (see {@link SubclassMocks} for the mechanism). Warms the delegate up on a real database, then
 * reads a mock's name back through it. It can only fail on a JIT that keeps stale code (Graal), and only while no
 * inline mock has retransformed {@code LocalDatabase} earlier in the fork; it cannot fail falsely.
 */
class Issue8021MockSeenThroughJitCompiledDelegateTest {
  private static final String MOCKED_NAME = "issue8021-mocked";

  @TempDir
  Path tempDir;

  @Test
  void aSubclassMockIsAnsweredThroughADelegateTheJitCompiledAgainstTheRealClass() throws InterruptedException {
    try (final HttpClient httpClient = HttpClient.newHttpClient();
        final DatabaseFactory factory = new DatabaseFactory(tempDir.resolve("db").toString())) {
      final LocalDatabase real = (LocalDatabase) factory.create();
      try {
        final RaftReplicatedDatabase warm = new RaftReplicatedDatabase(null, real, null, httpClient);
        long chars = 0;
        // Enough calls for every tier to compile the delegate, with pauses that let the background compiler finish.
        for (int round = 0; round < 10; round++) {
          for (int i = 0; i < 100_000; i++)
            chars += nameThrough(warm).length();
          Thread.sleep(20);
        }
        // Consuming the result keeps the warm-up loop from being eliminated as dead code.
        assertThat(chars).isPositive();
      } finally {
        real.drop();
      }

      final LocalDatabase proxied = SubclassMocks.mock(LocalDatabase.class);
      when(proxied.getName()).thenReturn(MOCKED_NAME);
      final RaftReplicatedDatabase database = new RaftReplicatedDatabase(null, proxied, null, httpClient);

      assertThat(proxied.getName()).isEqualTo(MOCKED_NAME);
      assertThat(nameThrough(database)).as("the compiled delegate must reach the stub, not the mock's null field").isEqualTo(MOCKED_NAME);
    }
  }

  /** The call site the JIT compiles: the same delegation the commit path makes when it builds a LocalCommit. */
  private static String nameThrough(final RaftReplicatedDatabase database) {
    return database.getName();
  }
}
