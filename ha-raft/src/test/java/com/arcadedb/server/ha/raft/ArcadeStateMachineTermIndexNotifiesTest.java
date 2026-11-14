/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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

import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11742: Ratis applies its own metadata and configuration entries through
 * {@link ArcadeStateMachine#notifyTermIndexUpdated}, which moved the applied index without calling
 * {@link RaftHAServer#notifyApplied()}. The bookmark a write is answered with already counts the metadata entry Ratis appends
 * after it, so a follower read waiting for that bookmark was woken only by its next one-second recheck: a quiet cluster answered
 * every read-your-writes read a whole number of seconds after it started. As for #5846, the notification is checked directly.
 */
class ArcadeStateMachineTermIndexNotifiesTest {

  @Test
  void anEntryRatisAppliesItselfWakesTheApplyWaiters(@TempDir final Path tempDir) throws Exception {
    final RaftStorage raftStorage = RaftStorage.newBuilder().setDirectory(tempDir.toFile())
        .setOption(RaftStorage.StartupOption.FORMAT).build();
    final ArcadeStateMachine sm = new ArcadeStateMachine();
    final FakeRaftHAServer raft = FakeRaftHAServer.detached();
    try {
      sm.initialize(stubServer(), RaftGroupId.valueOf(UUID.randomUUID()), raftStorage);
      sm.setRaftHAServer(raft);

      sm.notifyTermIndexUpdated(1L, 1L);

      assertThat(sm.getLastAppliedTermIndex().getIndex()).as("the applied index the waiters compare with").isEqualTo(1L);
      assertThat(raft.calls("notifyApplied")).hasSize(1);
    } finally {
      sm.close();
      raftStorage.close();
    }
  }

  /** {@code BaseStateMachine.initialize()} only needs a non-null {@code getId()}. */
  private static RaftServer stubServer() {
    return (RaftServer) Proxy.newProxyInstance(ArcadeStateMachineTermIndexNotifiesTest.class.getClassLoader(),
        new Class<?>[] { RaftServer.class }, (proxy, method, args) -> {
          if ("getId".equals(method.getName()))
            return RaftPeerId.valueOf("test-peer");
          if ("close".equals(method.getName()) || "start".equals(method.getName()))
            return null;
          throw new UnsupportedOperationException("Stub: " + method.getName());
        });
  }
}
