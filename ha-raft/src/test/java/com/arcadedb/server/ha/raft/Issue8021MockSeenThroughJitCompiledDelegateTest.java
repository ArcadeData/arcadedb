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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

/**
 * Issue #8021: {@code Issue6965LocalCommitHandshakeTest} failed with an NPE in {@code LocalCommitRegistry.register}
 * whenever a real-server test ran before it in the same fork, because {@code RaftReplicatedDatabase.getName()} - JIT
 * compiled by then, with {@code LocalDatabase.getName()} inlined - kept reading the mocked database's null
 * {@code name} field after Mockito's inline mock maker had retransformed {@code LocalDatabase}. See
 * {@link SubclassMocks} for the mechanism.
 * <p>
 * This reproduces the shape in one class: warm the delegate up on a real database until the JIT has compiled it, then
 * hand it a mock and read the name back through it. With an inline mock it answers null on the Graal JIT; with a
 * subclass mock it answers the stub on every JIT. The test can only fail on a JIT that keeps stale code, and only
 * while {@code LocalDatabase} has not yet been retransformed by an inline mock earlier in the fork - on any other run
 * it passes, it cannot fail falsely.
 */
class Issue8021MockSeenThroughJitCompiledDelegateTest {
  private static final String MOCKED_NAME = "issue8021-mocked";

  @TempDir
  Path tempDir;

  @Test
  void aSubclassMockIsAnsweredThroughADelegateTheJitCompiledAgainstTheRealClass() throws InterruptedException {
    final LocalDatabase real = (LocalDatabase) new DatabaseFactory(tempDir.resolve("db").toString()).create();
    try {
      final RaftReplicatedDatabase warm = new RaftReplicatedDatabase(null, real, null);
      long chars = 0;
      // Enough calls for every tier to compile the delegate, with pauses that let the background compiler finish.
      for (int round = 0; round < 20; round++) {
        for (int i = 0; i < 200_000; i++)
          chars += nameThrough(warm).length();
        Thread.sleep(20);
      }
      assertThat(chars).isPositive();
    } finally {
      real.drop();
    }

    final LocalDatabase proxied = SubclassMocks.mock(LocalDatabase.class);
    when(proxied.getName()).thenReturn(MOCKED_NAME);
    final RaftReplicatedDatabase database = new RaftReplicatedDatabase(null, proxied, null);

    assertThat(proxied.getName()).isEqualTo(MOCKED_NAME);
    assertThat(nameThrough(database)).as("the compiled delegate must reach the stub, not the mock's null field").isEqualTo(MOCKED_NAME);
  }

  /** The call site the JIT compiles: the same delegation the commit path makes when it builds a LocalCommit. */
  private static String nameThrough(final RaftReplicatedDatabase database) {
    return database.getName();
  }
}
