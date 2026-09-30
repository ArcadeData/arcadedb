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

import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link LeaderDatabaseQuery} endpoint/scheme selection (issue #4727) and response parsing (issue
 * #8360). Locks in the fix that the database-list RPC honors SSL: it must prefer the peer's HTTPS endpoint when
 * SSL is enabled, so the feature is not silently disabled on SSL-only clusters (the StatefulSet/empty-node
 * deployments it targets). No network is involved - this exercises the pure scheme-selection and JSON-parsing
 * logic.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LeaderDatabaseQueryTest {

  @Test
  void plainHttpWhenSslDisabled() {
    final LeaderDatabaseQuery.Endpoint e = LeaderDatabaseQuery.chooseEndpoint("host:2480", "host:2490", false);
    assertThat(e).isNotNull();
    assertThat(e.https()).isFalse();
    assertThat(e.url()).isEqualTo("http://host:2480/api/v1/cluster/bootstrap-state");
  }

  @Test
  void prefersHttpsWhenSslEnabledAndHttpsAddressKnown() {
    final LeaderDatabaseQuery.Endpoint e = LeaderDatabaseQuery.chooseEndpoint("host:2480", "host:2490", true);
    assertThat(e).isNotNull();
    assertThat(e.https()).isTrue();
    assertThat(e.url()).isEqualTo("https://host:2490/api/v1/cluster/bootstrap-state");
  }

  @Test
  void fallsBackToHttpWhenSslEnabledButNoHttpsAddress() {
    final LeaderDatabaseQuery.Endpoint e = LeaderDatabaseQuery.chooseEndpoint("host:2480", null, true);
    assertThat(e).isNotNull();
    assertThat(e.https()).isFalse();
    assertThat(e.url()).isEqualTo("http://host:2480/api/v1/cluster/bootstrap-state");
  }

  @Test
  void nullWhenNoUsableAddress() {
    assertThat(LeaderDatabaseQuery.chooseEndpoint(null, null, false)).isNull();
    // SSL on but only a null https and null http -> still nothing usable.
    assertThat(LeaderDatabaseQuery.chooseEndpoint(null, null, true)).isNull();
  }

  // ---- response parsing (issue #8360) ----

  @Test
  void parsesDatabasesAndSnapshotTermIndexWhenPresent() throws Exception {
    final LeaderDatabaseQuery.BootstrapState state = LeaderDatabaseQuery.parseBody("""
        {"databases":[{"name":"heimdall","fingerprint":"abc","lastTxId":42}],"snapshotTerm":7,"snapshotIndex":39707283}""",
        null, URL);

    assertThat(state.databases()).containsExactly(new LeaderDatabaseQuery.DatabaseInfo("heimdall", 42L));
    assertThat(state.snapshotTermIndex()).isEqualTo(TermIndex.valueOf(7L, 39707283L));
  }

  @Test
  void snapshotTermIndexIsNullWhenThePeerHasNoSnapshotYet() throws Exception {
    final LeaderDatabaseQuery.BootstrapState state = LeaderDatabaseQuery.parseBody(
        """
        {"databases":[]}""", null, URL);

    assertThat(state.databases()).isEmpty();
    assertThat(state.snapshotTermIndex())
        .as("no snapshot yet must parse as null, not as a fabricated (0, -1)/(0, 0) boundary")
        .isNull();
  }

  @Test
  void snapshotTermDefaultsToZeroWhenOmittedButIndexIsPresent() throws Exception {
    // Defensive: a well-formed peer never sends an index without its term, but the parser must not throw on it.
    final LeaderDatabaseQuery.BootstrapState state = LeaderDatabaseQuery.parseBody(
        """
        {"databases":[],"snapshotIndex":100}""", null, URL);

    assertThat(state.snapshotTermIndex()).isEqualTo(TermIndex.valueOf(0L, 100L));
  }

  // ---- the answer must be written by the peer it was meant for (issue #8658) ----

  private static final String URL = "http://leader:2480/api/v1/cluster/bootstrap-state";

  @Test
  void anAnswerWrittenByTheExpectedPeerIsAccepted() throws Exception {
    final LeaderDatabaseQuery.BootstrapState state = LeaderDatabaseQuery.parseBody(
        """
        {"peerId":"arcade-1","databases":[{"name":"db","fingerprint":"f","lastTxId":1}],"snapshotTerm":2,"snapshotIndex":9}""",
        "arcade-1", URL);

    assertThat(state.databases()).hasSize(1);
    assertThat(state.snapshotTermIndex()).isEqualTo(TermIndex.valueOf(2L, 9L));
  }

  @Test
  void anAnswerWrittenByAnotherNodeIsRefused() {
    assertThatThrownBy(() -> LeaderDatabaseQuery.parseBody(
        """
        {"peerId":"arcade-2","databases":[{"name":"db","fingerprint":"f","lastTxId":1}]}""", "arcade-1", URL))
        .isInstanceOf(LeaderDatabaseQuery.WrongPeerAnsweredException.class)
        .hasMessageContaining("arcade-1")
        .hasMessageContaining("arcade-2");
  }

  @Test
  void anAnswerThatNamesNoPeerIsRefused() {
    assertThatThrownBy(() -> LeaderDatabaseQuery.parseBody("""
        {"databases":[]}""", "arcade-1", URL))
        .isInstanceOf(LeaderDatabaseQuery.WrongPeerAnsweredException.class)
        .hasMessageContaining("names no peer");
  }

  @Test
  void noExpectedPeerAcceptsAnyAnswer() throws Exception {
    assertThat(LeaderDatabaseQuery.parseBody("""
        {"peerId":"whoever","databases":[]}""", null, URL).databases()).isEmpty();
  }
}
