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
package com.arcadedb.mcp;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The tools/call request log is written before the tool runs, so a secret passed as an argument reaches the log
 * whatever the tool's own response redaction does. These tests pin that the value written to a secret setting is
 * masked there, and that ordinary arguments still appear so the log can still explain what a call did.
 */
class MCPDispatcherArgumentLoggingTest {

  private static final String SECRET = "s3cr3t-cluster-token-should-never-leak";

  @Test
  void theValueWrittenToASecretSettingIsMaskedInTheRequestLog() {
    final JSONObject args = new JSONObject()
        .put("key", "arcadedb.ha.clusterToken")
        .put("value", SECRET);

    final String logged = MCPDispatcher.formatArgs("set_server_setting", args);

    assertThat(logged).doesNotContain(SECRET);
    assertThat(logged).contains("value=\"*****\"");
    // The key itself is not secret and is what makes the log line useful.
    assertThat(logged).contains("arcadedb.ha.clusterToken");
  }

  @Test
  void aPasswordSettingIsMaskedTheSameWay() {
    final JSONObject args = new JSONObject()
        .put("key", "arcadedb.server.rootPassword")
        .put("value", SECRET);

    assertThat(MCPDispatcher.formatArgs("set_server_setting", args)).doesNotContain(SECRET);
  }

  @Test
  void anOrdinarySettingValueIsStillLogged() {
    final JSONObject args = new JSONObject()
        .put("key", "arcadedb.asyncWorkerThreads")
        .put("value", "8");

    // Masking every value would leave the log unable to explain what a call changed.
    assertThat(MCPDispatcher.formatArgs("set_server_setting", args)).contains("value=\"8\"");
  }

  @Test
  void anArgumentNamedValueOnAnotherToolIsNotMasked() {
    final JSONObject args = new JSONObject()
        .put("key", "arcadedb.ha.clusterToken")
        .put("value", "ordinary-payload");

    // The mask is keyed on the tool as well as the setting, so it cannot silently blank an unrelated
    // argument that happens to share the name.
    assertThat(MCPDispatcher.formatArgs("upsert_entity", args)).contains("value=\"ordinary-payload\"");
  }

  @Test
  void anUnresolvableSettingKeyDoesNotMask() {
    final JSONObject args = new JSONObject()
        .put("key", "arcadedb.no.such.setting")
        .put("value", "plain");

    // An unknown key is rejected by the tool before it changes anything, so its value is not a secret.
    assertThat(MCPDispatcher.formatArgs("set_server_setting", args)).contains("value=\"plain\"");
  }

  @Test
  void aSettingKeyOfAnotherJsonTypeDoesNotRaise() {
    final JSONObject args = new JSONObject()
        .put("key", new JSONObject())
        .put("value", SECRET);

    // This line is logged before the tool runs and above the handler's try, so a raise here escapes the dispatcher
    // and reaches the transport as a bodiless HTTP 500 rather than a JSON-RPC envelope.
    assertThat(MCPDispatcher.formatArgs("set_server_setting", args)).isNotNull();
  }

  @Test
  void aSecretSettingKeyGivenAsASingleElementArrayIsStillMasked() {
    final JSONObject args = new JSONObject()
        .put("key", new JSONArray().put("arcadedb.server.rootPassword"))
        .put("value", SECRET);

    // Gson coerces a single-element array to that element's string form, so the tool resolves this key and does
    // write the secret. Guarding the read must not narrow it to a plain string, or the value reaches the log
    // unmasked while the setting is still applied.
    assertThat(MCPDispatcher.formatArgs("set_server_setting", args)).doesNotContain(SECRET);
  }
}
