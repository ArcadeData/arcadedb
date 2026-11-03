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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A LOGON is accepted from READY as well, which is what lets a pooled connection re-authenticate without a LOGOFF. A LOGON
 * that FAILS there must leave no identity in force: it used to keep the previous principal bound, so the RESET a pool sends
 * before reusing a connection brought it back to READY serving the user the failed attempt was switching away from.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class BoltFailedLogonIT extends BaseBoltServerTest {

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Bolt:com.arcadedb.bolt.BoltProtocolPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  void aRefusedLogonFromReadyLeavesNoPrincipalBound() throws Exception {
    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      assertRunSucceeds(bolt);

      bolt.sendMap(BoltMessage.LOGON, Map.of("scheme", "basic", "principal", "root", "credentials", "not-the-password"));
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.FAILURE);

      assertResetDoesNotRestoreTheSession(bolt);

      // and a LOGON that succeeds binds the identity again
      bolt.logon();
      assertRunSucceeds(bolt);
    }
  }

  @Test
  void aLogonWithoutCredentialsFromReadyLeavesNoPrincipalBound() throws Exception {
    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      assertRunSucceeds(bolt);

      bolt.sendMap(BoltMessage.LOGON, Map.of("scheme", "basic", "principal", "root"));
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.FAILURE);

      assertResetDoesNotRestoreTheSession(bolt);
    }
  }

  @Test
  void aLogonAsAnUnknownUserFromReadyLeavesNoPrincipalBound() throws Exception {
    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      bolt.sendMap(BoltMessage.LOGON, Map.of("scheme", "basic", "principal", "nobody-here", "credentials", "whatever"));
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.FAILURE);

      assertResetDoesNotRestoreTheSession(bolt);
    }
  }

  /** RESET answers SUCCESS and leaves the connection waiting for a LOGON: a query is refused, not run as the old user. */
  private static void assertResetDoesNotRestoreTheSession(final BoltWireConnection bolt) throws Exception {
    bolt.sendNoFields(BoltMessage.RESET);
    assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);

    bolt.run("RETURN 1 AS one");
    final BoltWireConnection.Summary run = bolt.readSummary();
    assertThat(run.signature()).as("a query after the refused LOGON must not run as the previous principal")
        .isEqualTo(BoltMessage.FAILURE);

    bolt.sendNoFields(BoltMessage.RESET);
    assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
  }

  private static void assertRunSucceeds(final BoltWireConnection bolt) throws Exception {
    bolt.run("RETURN 1 AS one");
    assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
    bolt.pull(-1, -1);
    assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
  }
}
