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
package com.arcadedb.postgres;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.server.ha.raft.BaseRaftHATest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;

import static com.arcadedb.postgres.PostgresWireMessages.WireMessage;
import static com.arcadedb.postgres.PostgresWireMessages.messageTypesOf;
import static com.arcadedb.postgres.PostgresWireMessages.readUntilReadyForQuery;
import static com.arcadedb.postgres.PostgresWireMessages.readyForQueryStatusOf;
import static com.arcadedb.postgres.PostgresWireMessages.sendSimpleQuery;
import static com.arcadedb.postgres.PostgresWireProtocolTestBase.readMessage;
import static com.arcadedb.postgres.PostgresWireProtocolTestBase.readMessageOfType;
import static com.arcadedb.postgres.PostgresWireProtocolTestBase.sendPasswordMessage;
import static com.arcadedb.postgres.PostgresWireProtocolTestBase.sendStartupMessage;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11483 on an HA follower: the statements of one simple Query message run in a single implicit transaction, and a
 * follower forwards a write to the leader as a request of its own unless the transaction it runs in is the client's
 * (issue #10040). Left unmarked, the implicit transaction of a message let a write commit on the leader before a later
 * statement of the same message failed, and the rollback that followed found nothing to undo.
 */
@Tag("slow")
class Issue11483MultiStatementFollowerIT extends BaseRaftHATest {
  private static final String TYPE = "Issue11483Ha";

  // One Postgres port per server, drawn free from the same ledger as the Raft ports
  private final int[] postgresPorts = allocateFixturePorts(3);

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    final int index = serverIndexOf(config);
    config.setValue(GlobalConfiguration.SERVER_PLUGINS.getKey(), "Postgres:com.arcadedb.postgres.PostgresProtocolPlugin");
    config.setValue(GlobalConfiguration.POSTGRES_PORT.getKey(), String.valueOf(postgresPorts[index]));
  }

  @Override
  protected int getServerCount() {
    return 3;
  }

  @Test
  @Timeout(180)
  void aFailureLaterInTheMessageUndoesAWriteSentToAFollower() throws Exception {
    final int follower = prepareFollower(requireLeaderIndex());

    try (final Socket socket = connect(follower)) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      sendSimpleQuery(out, "INSERT INTO " + TYPE + " SET name = 'undone'; SET x");
      final List<WireMessage> response = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(response)).contains('E');
      assertThat(readyForQueryStatusOf(response)).isEqualTo('I');
    }

    waitForAllServers();
    for (int i = 0; i < getServerCount(); i++)
      assertThat(count(i, "undone")).as("server %d must hold no row of the failed message", i).isZero();
  }

  @Test
  @Timeout(180)
  void aMessageThatSucceedsOnAFollowerIsCommittedEverywhere() throws Exception {
    final int follower = prepareFollower(requireLeaderIndex());

    try (final Socket socket = connect(follower)) {
      final DataOutputStream out = new DataOutputStream(socket.getOutputStream());
      final DataInputStream in = new DataInputStream(socket.getInputStream());
      authenticate(out, in);

      sendSimpleQuery(out, "INSERT INTO " + TYPE + " SET name = 'kept'; INSERT INTO " + TYPE + " SET name = 'kept'");
      final List<WireMessage> response = readUntilReadyForQuery(in);
      assertThat(messageTypesOf(response)).doesNotContain('E');
      assertThat(readyForQueryStatusOf(response)).isEqualTo('I');
    }

    waitForAllServers();
    for (int i = 0; i < getServerCount(); i++)
      assertThat(count(i, "kept")).as("server %d must hold both rows of the message", i).isEqualTo(2L);
  }

  private long count(final int serverIndex, final String name) {
    final Database db = getServerDatabase(serverIndex, getDatabaseName());
    try (final ResultSet rs = db.query("sql", "SELECT count(@rid) AS c FROM " + TYPE + " WHERE name = ?", name)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  /** Creates the target type through the leader and waits for the follower to see it, then returns the follower. */
  private int prepareFollower(final int leader) {
    final Database leaderDb = getServerDatabase(leader, getDatabaseName());
    leaderDb.command("sql", "CREATE DOCUMENT TYPE " + TYPE + " IF NOT EXISTS");
    waitForAllServers();
    return firstFollowerOf(leader);
  }

  private Socket connect(final int serverIndex) throws Exception {
    final Socket socket = new Socket();
    socket.connect(new InetSocketAddress("localhost", postgresPorts[serverIndex]), 2000);
    return socket;
  }

  private void authenticate(final DataOutputStream out, final DataInputStream in) throws Exception {
    sendStartupMessage(out, "root", getDatabaseName());
    readMessage(in); // AuthenticationCleartextPassword
    sendPasswordMessage(out, DEFAULT_PASSWORD_FOR_TESTS);
    readMessageOfType(in, 'Z'); // drain AuthenticationOk/BackendKeyData/ParameterStatus.../ReadyForQuery
  }
}
