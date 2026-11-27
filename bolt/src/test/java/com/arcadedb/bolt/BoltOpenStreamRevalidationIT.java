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
package com.arcadedb.bolt;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.bolt.message.BoltMessage;
import com.arcadedb.query.sql.executor.QueryAdmissionGate;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.security.ServerSecurity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The rows of a result stream are produced as they are pulled, so a PULL or a DISCARD is checked against the user the
 * connection stands for NOW, as a RUN is: a user deleted, re-passworded or stripped of the database grant after the statement
 * started gets no further row, the streams and the transaction of the connection are released with the refusal, and a user
 * nothing happened to keeps reading its streams.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
public class BoltOpenStreamRevalidationIT extends BaseBoltServerTest {
  private static final String USER         = "boltStreamUser";
  private static final String PASSWORD     = "boltStreamPassword1";
  private static final String STREAM_QUERY = "UNWIND range(1, 5) AS x RETURN x";

  /** What happens to the user while a stream of its connection is open, and the code the connection is then refused with. */
  enum Change {
    DELETED(BoltErrorCodes.AUTHENTICATION_ERROR) {
      @Override
      void apply(final ServerSecurity security, final String database) {
        security.dropUser(USER);
      }
    },
    PASSWORD_ROTATED(BoltErrorCodes.AUTHENTICATION_ERROR) {
      @Override
      void apply(final ServerSecurity security, final String database) {
        security.updateUser(new JSONObject().put("name", USER).put("password", security.encodePassword("another-Password-2"))
            .put("databases", new JSONObject().put(database, new JSONArray().put("admin"))));
      }
    },
    GRANT_REMOVED(BoltErrorCodes.FORBIDDEN_ERROR) {
      @Override
      void apply(final ServerSecurity security, final String database) {
        // the stored hash is kept, so only the grant changes: a re-salted hash would read as a password rotation
        security.updateUser(new JSONObject().put("name", USER).put("password", security.getUser(USER).getPassword())
            .put("databases", new JSONObject()));
      }
    };

    final String code;

    Change(final String code) {
      this.code = code;
    }

    abstract void apply(ServerSecurity security, String database);
  }

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Bolt:com.arcadedb.bolt.BoltProtocolPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    final ServerSecurity security = getServer(0).getSecurity();
    if (security.getUser(USER) != null)
      security.dropUser(USER);
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @ParameterizedTest
  @EnumSource(Change.class)
  void aPullAfterTheUserChangedIsRefused(final Change change) throws Exception {
    createUser();

    try (final BoltWireConnection bolt = connectAsUser()) {
      openStream(bolt);
      bolt.pull(1, -1);
      final BoltWireConnection.Summary first = bolt.readSummary();
      assertThat(first.signature()).isEqualTo(BoltMessage.SUCCESS);
      assertThat(first.records()).as("the row pulled before the change").hasSize(1);
      assertThat(first.metadata()).containsEntry("has_more", true);

      change.apply(getServer(0).getSecurity(), getDatabaseName());

      bolt.pull(-1, -1);
      assertRefused(bolt.readSummary(), change);
      assertThat(QueryAdmissionGate.getInstance().getRunning()).as("the refusal releases the stream").isZero();

      bolt.sendNoFields(BoltMessage.RESET);
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
      assertRunRefused(bolt);
    }
  }

  @ParameterizedTest
  @EnumSource(Change.class)
  void aStreamNeverPulledIsRefusedToo(final Change change) throws Exception {
    createUser();

    try (final BoltWireConnection bolt = connectAsUser()) {
      openStream(bolt);

      change.apply(getServer(0).getSecurity(), getDatabaseName());

      bolt.pull(-1, -1);
      assertRefused(bolt.readSummary(), change);
      assertThat(QueryAdmissionGate.getInstance().getRunning()).isZero();
    }
  }

  @ParameterizedTest
  @EnumSource(Change.class)
  void aDiscardAfterTheUserChangedIsRefused(final Change change) throws Exception {
    createUser();

    try (final BoltWireConnection bolt = connectAsUser()) {
      openStream(bolt);
      bolt.pull(1, -1);
      assertThat(bolt.readSummary().metadata()).containsEntry("has_more", true);

      change.apply(getServer(0).getSecurity(), getDatabaseName());

      bolt.discard(-1, -1);
      assertRefused(bolt.readSummary(), change);
      assertThat(QueryAdmissionGate.getInstance().getRunning()).as("the refusal releases the stream").isZero();
    }
  }

  @ParameterizedTest
  @EnumSource(Change.class)
  void everyStreamOfATransactionIsRefusedAndReleased(final Change change) throws Exception {
    createUser();

    try (final BoltWireConnection bolt = connectAsUser()) {
      bolt.begin(getDatabaseName());
      bolt.run(STREAM_QUERY);
      assertThat(bolt.readSummary().metadata()).containsEntry("qid", 0L);
      bolt.run(STREAM_QUERY);
      assertThat(bolt.readSummary().metadata()).containsEntry("qid", 1L);

      bolt.pull(1, 0);
      assertThat(bolt.readSummary().records()).hasSize(1);

      change.apply(getServer(0).getSecurity(), getDatabaseName());

      // The second stream was never pulled, the first one only in part: both are refused, and both are released at once
      bolt.pull(-1, 1);
      assertRefused(bolt.readSummary(), change);
      assertThat(QueryAdmissionGate.getInstance().getRunning()).as("both streams are released").isZero();

      bolt.pull(-1, 0);
      assertThat(bolt.readSummary().signature()).as("the connection serves nothing until it is reset").isEqualTo(BoltMessage.IGNORED);

      bolt.sendNoFields(BoltMessage.RESET);
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
    }
  }

  @ParameterizedTest
  @EnumSource(Change.class)
  void aSystemStreamIsRefusedToo(final Change change) throws Exception {
    createUser();

    try (final BoltWireConnection bolt = connectAsUser()) {
      bolt.run("SHOW DATABASES");
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);

      change.apply(getServer(0).getSecurity(), getDatabaseName());

      bolt.pull(-1, -1);
      assertRefused(bolt.readSummary(), change);
    }
  }

  @Test
  void aUserNothingHappenedToKeepsReadingItsStreams() throws Exception {
    createUser();

    try (final BoltWireConnection bolt = connectAsUser()) {
      openStream(bolt);
      bolt.pull(2, -1);
      final BoltWireConnection.Summary first = bolt.readSummary();
      assertThat(first.records()).hasSize(2);
      assertThat(first.metadata()).containsEntry("has_more", true);

      // A change that leaves the grant in place: the stream goes on
      final ServerSecurity security = getServer(0).getSecurity();
      security.updateUser(new JSONObject().put("name", USER).put("password", security.getUser(USER).getPassword())
          .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put("admin"))));

      bolt.pull(1, -1);
      final BoltWireConnection.Summary second = bolt.readSummary();
      assertThat(second.signature()).isEqualTo(BoltMessage.SUCCESS);
      assertThat(second.records()).hasSize(1);

      bolt.discard(-1, -1);
      final BoltWireConnection.Summary rest = bolt.readSummary();
      assertThat(rest.signature()).isEqualTo(BoltMessage.SUCCESS);
      assertThat(rest.metadata()).containsEntry("has_more", false);
      assertThat(QueryAdmissionGate.getInstance().getRunning()).isZero();
    }
  }

  private void createUser() {
    final ServerSecurity security = getServer(0).getSecurity();
    security.createUser(new JSONObject().put("name", USER).put("password", security.encodePassword(PASSWORD))
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put("admin"))));
  }

  /** A connection authenticated as the user, through a second LOGON of the one the fixture opens as root. */
  private BoltWireConnection connectAsUser() throws Exception {
    final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName());
    bolt.logon(USER, PASSWORD);
    return bolt;
  }

  private static void openStream(final BoltWireConnection bolt) throws Exception {
    bolt.run(STREAM_QUERY);
    assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
  }

  private static void assertRefused(final BoltWireConnection.Summary summary, final Change change) {
    assertThat(summary.signature()).as("the answer to a request of a user that changed (%s)", change).isEqualTo(BoltMessage.FAILURE);
    assertThat(summary.code()).as(change.name()).isEqualTo(change.code);
    assertThat(summary.records()).as("no row follows the change").isEmpty();
  }

  /** The connection, reset, serves its user no statement: the refusal does not wear off with the RESET. */
  private static void assertRunRefused(final BoltWireConnection bolt) throws Exception {
    bolt.run(STREAM_QUERY);
    final BoltWireConnection.Summary summary = bolt.readSummary();
    assertThat(summary.signature()).isEqualTo(BoltMessage.FAILURE);
    assertThat(summary.records()).isEmpty();
  }
}
