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

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.TestServerHelper;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11196: the three optional TX_ENTRY sections (tx-prepared-at-index, tx-fulltext-corpus-deltas, fixed-width-float-arrays)
 * are emitted by any node, so a registry that has not been filled yet must not read as "a peer does not support it". A peer that
 * was never asked is a third state: it is asked once, then the answer decides. A peer that answered no, or could not be reached,
 * is still a "no" and is not probed again on the commit path.
 */
class Issue11196ColdRegistryTxEntryCapabilityTest {

  private static final String PEER = "localhost_2435";

  @Test
  void aColdRegistryIsAskedOnceInsteadOfReadingAsNo() {
    final RaftHAServer raft = newDetachedServer();
    final AtomicInteger probes = new AtomicInteger();
    raft.setCapabilityProber((peerId, http, https, token) -> {
      probes.incrementAndGet();
      return new PeerCapabilityQuery.Advertisement(PEER, "26.11.1", PeerCapabilities.LOCAL);
    });

    assertThat(raft.getPeerCapabilityRegistry().freshAdvertisementOf(PEER)).as("cold registry").isNull();

    assertThat(raft.canStateTxPreparedAt()).as("tx-prepared-at-index").isTrue();
    assertThat(raft.canStateFullTextCorpusDeltas()).as("tx-fulltext-corpus-deltas").isTrue();
    assertThat(raft.canWriteFixedWidthFloatArrays()).as("fixed-width-float-arrays").isTrue();
    assertThat(probes.get()).as("one round filled the registry for all three").isEqualTo(1);
  }

  @Test
  void aPeerThatAnsweredNoIsNotProbedAgainOnTheCommitPath() {
    final RaftHAServer raft = newDetachedServer();
    final AtomicInteger probes = new AtomicInteger();
    raft.setCapabilityProber((peerId, http, https, token) -> {
      probes.incrementAndGet();
      return new PeerCapabilityQuery.Advertisement(PEER, "26.9.1", Set.of(PeerCapabilities.SCHEMA_DELTA));
    });

    assertThat(raft.canStateTxPreparedAt()).isFalse();
    raft.invalidateTxPreparedAtCapability();
    assertThat(raft.canStateTxPreparedAt()).isFalse();
    assertThat(raft.canStateFullTextCorpusDeltas()).isFalse();

    assertThat(probes.get()).as("a real 'no' is a settled answer, only the first read asked").isEqualTo(1);
  }

  @Test
  void anUnreachablePeerIsAskedOnceAndStaysANo() {
    final RaftHAServer raft = newDetachedServer();
    final AtomicInteger probes = new AtomicInteger();
    raft.setCapabilityProber((peerId, http, https, token) -> {
      probes.incrementAndGet();
      throw new IOException("connection refused");
    });

    assertThat(raft.canStateTxPreparedAt()).isFalse();
    raft.invalidateTxPreparedAtCapability();
    assertThat(raft.canStateTxPreparedAt()).isFalse();

    assertThat(probes.get()).as("a failed probe is recorded, so a down peer does not cost a round per commit").isEqualTo(1);
  }

  @Test
  void theWarningSaysWhyEachPeerIsMissing() {
    final RaftHAServer raft = newDetachedServer();
    assertThat(raft.explainMissingCapability(PeerCapabilities.TX_PREPARED_AT_INDEX))
        .as("never asked").contains(PEER).containsIgnoringCase("not been asked");

    raft.setCapabilityProber((peerId, http, https, token) ->
        new PeerCapabilityQuery.Advertisement(PEER, "26.9.1", Set.of(PeerCapabilities.SCHEMA_DELTA)));
    raft.refreshPeerCapabilities();
    assertThat(raft.explainMissingCapability(PeerCapabilities.TX_PREPARED_AT_INDEX))
        .as("answered without it").contains(PEER).containsIgnoringCase("older build");

    raft.setCapabilityProber((peerId, http, https, token) -> {
      throw new IOException("connection refused");
    });
    raft.refreshPeerCapabilities();
    assertThat(raft.explainMissingCapability(PeerCapabilities.TX_PREPARED_AT_INDEX))
        .as("unreachable").contains(PEER).contains("connection refused");
  }

  private static RaftHAServer newDetachedServer() {
    final ContextConfiguration config = new ContextConfiguration();
    config.setValue(GlobalConfiguration.HA_SERVER_LIST, "localhost:2434:2480,localhost:2435:2481");

    final ArcadeDBServer arcadeServer = TestServerHelper.unstartedServer("ArcadeDB_0");
    return new RaftHAServer(arcadeServer, config);
  }
}
