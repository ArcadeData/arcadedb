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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.security.ServerSecurity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The GQL session parameters ({@code SESSION SET}) belong to the identity that set them. A connection that is logged off, or
 * logged on again as another user, starts with an empty session: the parameters of the previous identity must not be visible to
 * the next one. A plain RESET keeps them, as it always did, because it is connection housekeeping that leaves the identity alone.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class BoltSessionStateOnReauthenticationIT extends BaseBoltServerTest {
  private static final String SECOND_USER     = "boltSessionSecondUser";
  private static final String SECOND_PASSWORD = "boltSessionSecondPassword1";
  private static final String PARAMETER       = "carriedValue";

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("Bolt:com.arcadedb.bolt.BoltProtocolPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    if (getServer(0) != null && getServer(0).isStarted()) {
      final ServerSecurity security = getServer(0).getSecurity();
      if (security.getUser(SECOND_USER) != null)
        security.dropUser(SECOND_USER);
    }
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  void logoffThenLogonAsAnotherUserStartsWithAnEmptySession() throws Exception {
    createSecondUser();
    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      setSessionParameter(bolt);
      assertParameterIsBound(bolt);

      bolt.sendNoFields(BoltMessage.LOGOFF);
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
      bolt.logon(SECOND_USER, SECOND_PASSWORD);

      assertParameterIsUnbound(bolt);
    }
  }

  @Test
  void logonFromReadyAsAnotherUserStartsWithAnEmptySession() throws Exception {
    createSecondUser();
    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      setSessionParameter(bolt);
      assertParameterIsBound(bolt);

      bolt.logon(SECOND_USER, SECOND_PASSWORD);

      assertParameterIsUnbound(bolt);
    }
  }

  @Test
  void logoffThenLogonAsTheSameUserStartsWithAnEmptySession() throws Exception {
    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      setSessionParameter(bolt);
      assertParameterIsBound(bolt);

      bolt.sendNoFields(BoltMessage.LOGOFF);
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
      bolt.logon();

      assertParameterIsUnbound(bolt);
    }
  }

  @Test
  void aRefusedLogonFromReadyDropsTheSessionParameters() throws Exception {
    createSecondUser();
    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      setSessionParameter(bolt);
      assertParameterIsBound(bolt);

      bolt.sendMap(BoltMessage.LOGON, Map.of("scheme", "basic", "principal", SECOND_USER, "credentials", "not-the-password"));
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.FAILURE);
      bolt.sendNoFields(BoltMessage.RESET);
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);

      bolt.logon(SECOND_USER, SECOND_PASSWORD);

      assertParameterIsUnbound(bolt);
    }
  }

  @Test
  void aPlainResetKeepsTheSessionParametersOfTheSameIdentity() throws Exception {
    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      setSessionParameter(bolt);

      bolt.sendNoFields(BoltMessage.RESET);
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);

      assertParameterIsBound(bolt);
    }
  }

  private void createSecondUser() {
    final ServerSecurity security = getServer(0).getSecurity();
    security.createUser(new JSONObject().put("name", SECOND_USER).put("password", security.encodePassword(SECOND_PASSWORD))
        .put("databases", new JSONObject().put(getDatabaseName(), new JSONArray().put("admin"))));
  }

  private static void setSessionParameter(final BoltWireConnection bolt) throws Exception {
    final BoltWireConnection.Summary set = runAndPull(bolt, "SESSION SET $" + PARAMETER + " = 4242");
    assertThat(set.signature()).isEqualTo(BoltMessage.SUCCESS);
  }

  private static void assertParameterIsBound(final BoltWireConnection bolt) throws Exception {
    final BoltWireConnection.Summary read = runAndPull(bolt, "RETURN $" + PARAMETER + " AS value");
    assertThat(read.signature()).isEqualTo(BoltMessage.SUCCESS);
    assertThat(read.records()).hasSize(1);
    assertThat(((List<?>) read.records().get(0)).get(0)).isEqualTo(4242L);
  }

  /** The parameter is unbound: the query is refused as it is for any parameter nothing supplied, and no value comes back. */
  private static void assertParameterIsUnbound(final BoltWireConnection bolt) throws Exception {
    final BoltWireConnection.Summary read = runAndPull(bolt, "RETURN $" + PARAMETER + " AS value");
    assertThat(read.signature()).as("the parameter of the previous identity must not be visible").isEqualTo(BoltMessage.FAILURE);
    assertThat(read.records()).isEmpty();
    assertThat(read.message()).contains(PARAMETER);
  }

  /** Runs {@code query} and pulls all of its rows; answers the summary that ended the exchange, a FAILURE of either step included. */
  private static BoltWireConnection.Summary runAndPull(final BoltWireConnection bolt, final String query) throws Exception {
    bolt.run(query);
    final BoltWireConnection.Summary run = bolt.readSummary();
    if (run.signature() != BoltMessage.SUCCESS) {
      resetAfterFailure(bolt);
      return run;
    }
    bolt.pull(-1, -1);
    final BoltWireConnection.Summary pull = bolt.readSummary();
    if (pull.signature() != BoltMessage.SUCCESS)
      resetAfterFailure(bolt);
    return pull;
  }

  /** RESET is the way out of FAILED, and it keeps the identity and the session parameters, so the next step is not disturbed. */
  private static void resetAfterFailure(final BoltWireConnection bolt) throws Exception {
    bolt.sendNoFields(BoltMessage.RESET);
    assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
  }
}
