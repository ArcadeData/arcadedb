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

import com.arcadedb.serializer.json.JSONObject;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5890 (code review follow-up): closing {@code PostVerifyDatabaseHandler}'s
 * {@code peerQueryExecutor} in {@code RaftHAPlugin.stopService()} means a request already fanning out to
 * peers can now race a concurrent shutdown. {@code CompletableFuture.supplyAsync(...)} submitting to an
 * already-shut-down pool throws {@link java.util.concurrent.RejectedExecutionException} synchronously -
 * a failure mode that could not previously occur, since the pool was never shut down at all.
 * {@link PostVerifyDatabaseHandler#submitPeerQuery} must degrade that into a normal per-peer ERROR result
 * instead of letting the exception abort the whole request.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class PostVerifyDatabaseHandlerRejectedExecutionTest {

  @Test
  void submitPeerQueryDegradesGracefullyWhenThePoolIsAlreadyShutDown() {
    final PostVerifyDatabaseHandler handler = new PostVerifyDatabaseHandler(null, new RaftHAPlugin());
    handler.close(); // shuts down peerQueryExecutor, simulating a concurrent stopService()

    final RaftPeer peer = RaftPeer.newBuilder().setId(RaftPeerId.valueOf("peer1")).build();
    // raftHAServer/localChecksums/user are never dereferenced: the pool rejects the submission before
    // queryPeer's lambda body ever runs, so null is safe here.
    final CompletableFuture<JSONObject> future =
        handler.submitPeerQuery(null, peer, "mydb", null, null, false);

    final JSONObject result = future.join();
    assertThat(result.getString("status", null)).isEqualTo("ERROR");
    assertThat(result.getString("peerId", null)).isEqualTo("peer1");
  }
}
