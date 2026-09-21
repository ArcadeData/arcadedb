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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8038. The {@code tools/call} request log masked the {@code value} argument of
 * {@code set_server_setting} on {@link GlobalConfiguration#isHidden()} alone, while the settings READERS had
 * already moved onto {@link GlobalConfiguration#publishableValue(Object)}. {@code isHidden()} is {@code false}
 * for {@code arcadedb.server.defaultDatabases}, because the credential sits inside the value rather than being
 * the whole value, so the passwords the getters conceal were written to the server log in clear by the line
 * that describes the call - a line emitted before the tool runs and therefore covered by no response redaction.
 * <p>
 * The log now renders that argument through {@code publishableValue}, the same rule the readers use.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue8038RequestLogRedactionTest {

  private static final String DEFAULT_DATABASES = "Universe[albert:einstein:admin];Amiga[Jay:Miner,Jack:Tramiel]";

  @Test
  void theEmbeddedCredentialsOfDefaultDatabasesAreMaskedInTheRequestLog() {
    final JSONObject args = new JSONObject()
        .put("key", GlobalConfiguration.SERVER_DEFAULT_DATABASES.getKey())
        .put("value", DEFAULT_DATABASES);

    final String logged = MCPDispatcher.formatArgs("set_server_setting", args);

    assertThat(logged).as("the password of every credential in the value").doesNotContain("einstein");
    assertThat(logged).doesNotContain("Miner");
    assertThat(logged).doesNotContain("Tramiel");
  }

  /**
   * The log keeps everything an operator reads it for. Masking the whole argument on this key would leave the
   * line unable to say which databases a call was about, which is the reason the readers redact one span rather
   * than blanking the value.
   */
  @Test
  void theLogStillSaysWhichDatabasesAndUsersTheCallWasAbout() {
    final JSONObject args = new JSONObject()
        .put("key", GlobalConfiguration.SERVER_DEFAULT_DATABASES.getKey())
        .put("value", DEFAULT_DATABASES);

    final String logged = MCPDispatcher.formatArgs("set_server_setting", args);

    assertThat(logged).contains("value=\"" + GlobalConfiguration.SERVER_DEFAULT_DATABASES
        .publishableValue(DEFAULT_DATABASES) + "\"");
    assertThat(logged).contains("Universe").contains("Amiga").contains("albert").contains("admin");
  }

  /** A setting {@code isHidden()} does cover is still masked whole: the new rule is a superset of the old one. */
  @Test
  void aWhollyHiddenSettingIsStillMaskedWhole() {
    final JSONObject args = new JSONObject()
        .put("key", GlobalConfiguration.SERVER_ROOT_PASSWORD.getKey())
        .put("value", "s3cr3t");

    assertThat(MCPDispatcher.formatArgs("set_server_setting", args))
        .contains("value=\"*****\"")
        .doesNotContain("s3cr3t");
  }

  /** And a setting that carries no secret at all is still logged as written, or the log explains nothing. */
  @Test
  void anOrdinarySettingValueIsStillLoggedInFull() {
    final JSONObject args = new JSONObject()
        .put("key", GlobalConfiguration.ASYNC_WORKER_THREADS.getKey())
        .put("value", "8");

    assertThat(MCPDispatcher.formatArgs("set_server_setting", args)).contains("value=\"8\"");
  }

  /**
   * The redaction is keyed on the tool as well as the setting. An argument named {@code value} on another tool
   * carries caller data, which cannot be told apart from a secret without guessing.
   */
  @Test
  void anArgumentNamedValueOnAnotherToolIsUntouched() {
    final JSONObject args = new JSONObject()
        .put("key", GlobalConfiguration.SERVER_DEFAULT_DATABASES.getKey())
        .put("value", DEFAULT_DATABASES);

    assertThat(MCPDispatcher.formatArgs("upsert_entity", args)).contains("einstein");
  }

  /**
   * This line is built above the handler's try, so a raise here escapes the dispatcher and reaches the transport
   * as a bodiless HTTP 500. Resolving the key for the redactor must keep failing closed the way the boolean it
   * replaces did.
   */
  @Test
  void aSettingKeyOfAnotherJsonTypeDoesNotRaise() {
    final JSONObject args = new JSONObject()
        .put("key", new JSONObject())
        .put("value", DEFAULT_DATABASES);

    assertThat(MCPDispatcher.formatArgs("set_server_setting", args)).isNotNull();
  }

  /** An unresolvable key is rejected by the tool before it changes anything, so its value is not a secret. */
  @Test
  void anUnresolvableSettingKeyDoesNotMask() {
    final JSONObject args = new JSONObject()
        .put("key", "arcadedb.no.such.setting")
        .put("value", "plain");

    assertThat(MCPDispatcher.formatArgs("set_server_setting", args)).contains("value=\"plain\"");
  }

  /**
   * The embedded-credential rule is defined on the value's TEXT, so an argument that arrives as another JSON type
   * has to be redacted through that text: the renderer reaches it with toString() either way. Raised in the
   * PR #8080 review as the one shape of this argument the first patch left uncovered.
   */
  @Test
  void aDefaultDatabasesValueArrivingAsAnObjectIsRedactedThroughItsText() {
    final JSONObject args = new JSONObject()
        .put("key", GlobalConfiguration.SERVER_DEFAULT_DATABASES.getKey())
        .put("value", new JSONObject().put("databases", DEFAULT_DATABASES));

    final String logged = MCPDispatcher.formatArgs("set_server_setting", args);

    assertThat(logged).doesNotContain("einstein").doesNotContain("Miner").doesNotContain("Tramiel");
  }

  /** A wholly hidden setting was already covered whatever the type, because isHidden() is checked first. */
  @Test
  void aHiddenSettingValueArrivingAsAnObjectIsStillMasked() {
    final JSONObject args = new JSONObject()
        .put("key", GlobalConfiguration.SERVER_ROOT_PASSWORD.getKey())
        .put("value", new JSONObject().put("password", "s3cr3t"));

    assertThat(MCPDispatcher.formatArgs("set_server_setting", args)).doesNotContain("s3cr3t");
  }

  /** A non-string argument must not start being rendered differently just because the redactor resolved a key. */
  @Test
  void aNonStringValueArgumentIsStillRenderedAsBefore() {
    final JSONObject args = new JSONObject()
        .put("key", GlobalConfiguration.ASYNC_WORKER_THREADS.getKey())
        .put("value", 8);

    assertThat(MCPDispatcher.formatArgs("set_server_setting", args)).contains("value=8");
  }
}
