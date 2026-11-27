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
import com.arcadedb.bolt.packstream.PackStreamReader;
import com.arcadedb.bolt.structure.BoltTemporalStructure;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A RUN read off the socket before the connection has authenticated is answered by the state machine, whatever its parameters
 * hold, and the connection goes on to serve a client that then authenticates and runs the same parameters.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class BoltRunBeforeAuthenticationIT extends BaseBoltServerTest {
  private static final byte SIG_LOCAL_TIME = 0x74; // 't'

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
  void aRunBeforeAuthenticationIsRefusedWhateverItsParametersHold() throws Exception {
    try (final BoltWireConnection bolt = BoltWireConnection.beforeHello(getServerBoltPort())) {
      bolt.run("RETURN $t AS t", parametersHolding(new BoltTemporalStructure(SIG_LOCAL_TIME, -1L)), Map.of());
      final BoltWireConnection.Summary refused = bolt.readSummary();
      assertThat(refused.signature()).isEqualTo(BoltMessage.FAILURE);
      assertThat(refused.code()).isEqualTo(BoltException.PROTOCOL_ERROR);
      assertThat(refused.message()).contains("RUN not expected in state: AUTHENTICATION");

      // the connection is FAILED now: what follows is ignored, parameters or not
      bolt.run("RETURN $t AS t", parametersHolding(new BoltTemporalStructure(SIG_LOCAL_TIME, -1L)), Map.of());
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.IGNORED);

      // and RESET brings it back to a connection that can authenticate
      bolt.sendNoFields(BoltMessage.RESET);
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);
      bolt.hello();
      bolt.logon();
    }
  }

  @Test
  void theTemporalParametersOfAnAcceptedRunAreStillBound() throws Exception {
    final long oneHour = 3_600_000_000_000L;
    try (final BoltWireConnection bolt = new BoltWireConnection(getServerBoltPort(), getDatabaseName())) {
      bolt.run("RETURN $t AS t", parametersHolding(new BoltTemporalStructure(SIG_LOCAL_TIME, oneHour)), Map.of());
      assertThat(bolt.readSummary().signature()).isEqualTo(BoltMessage.SUCCESS);

      bolt.pull(-1, -1);
      final BoltWireConnection.Summary pulled = bolt.readSummary();
      assertThat(pulled.signature()).isEqualTo(BoltMessage.SUCCESS);
      assertThat(pulled.records()).hasSize(1);

      final Object value = ((List<?>) pulled.records().get(0)).get(0);
      assertThat(value).as("a local time that was bound comes back as one").isInstanceOf(PackStreamReader.StructureValue.class);
      final PackStreamReader.StructureValue structure = (PackStreamReader.StructureValue) value;
      assertThat(structure.getSignature()).isEqualTo(SIG_LOCAL_TIME);
      assertThat(structure.getField(0)).isEqualTo(oneHour);
    }
  }

  /** The parameter map holding {@code value} directly and, below ten lists, in a list. */
  private static Map<String, Object> parametersHolding(final BoltTemporalStructure value) {
    List<Object> nested = new ArrayList<>(List.of(value));
    for (int i = 0; i < 10; i++)
      nested = new ArrayList<>(List.of(nested));

    final Map<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("t", value);
    parameters.put("nested", nested);
    return parameters;
  }
}
