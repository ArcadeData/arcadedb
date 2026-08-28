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

import com.arcadedb.bolt.message.HelloMessage;
import com.arcadedb.bolt.message.LogonMessage;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #6801: with {@code arcadedb.bolt.debug=true} every inbound message is logged
 * through {@code toString()}, and for every BOLT version below 5.1 the HELLO extra map carries the caller's
 * cleartext password. Enabling protocol debug to troubleshoot a connection is exactly the situation where that
 * fires, and the value then flows into whatever log pipeline is configured.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6801HelloCredentialsRedactionTest {

  private static final String PASSWORD = "sup3r-s3cr3t-p4ssw0rd";

  private static Map<String, Object> helloExtra() {
    final Map<String, Object> extra = new LinkedHashMap<>();
    extra.put("user_agent", "neo4j-java/5.26.0");
    extra.put("scheme", "basic");
    extra.put("principal", "root");
    extra.put("credentials", PASSWORD);
    extra.put("routing", Map.of("db", "mydb"));
    return extra;
  }

  @Test
  void helloToStringNeverRendersTheCredentials() {
    final String rendered = new HelloMessage(helloExtra()).toString();

    assertThat(rendered).doesNotContain(PASSWORD);
    assertThat(rendered).contains("credentials=***");
  }

  /**
   * Redaction must not cost the diagnostics the debug flag was turned on for: everything except the password is
   * still there.
   */
  @Test
  void helloToStringKeepsEveryNonSecretField() {
    final String rendered = new HelloMessage(helloExtra()).toString();

    assertThat(rendered).startsWith("HELLO{extra=");
    assertThat(rendered).contains("user_agent=neo4j-java/5.26.0");
    assertThat(rendered).contains("scheme=basic");
    assertThat(rendered).contains("principal=root");
    assertThat(rendered).contains("routing={db=mydb}");
  }

  /**
   * A BOLT 5.1+ HELLO carries no credentials at all (auth is deferred to LOGON), and must render unchanged.
   */
  @Test
  void helloWithoutCredentialsIsRenderedVerbatim() {
    final String rendered = new HelloMessage(Map.of("user_agent", "neo4j-java/5.26.0")).toString();

    assertThat(rendered).isEqualTo("HELLO{extra={user_agent=neo4j-java/5.26.0}}");
  }

  /**
   * Redaction is a rendering concern only: the handshake still has to be able to read the password back.
   */
  @Test
  void redactionDoesNotAlterTheParsedMessage() {
    final HelloMessage message = new HelloMessage(helloExtra());

    assertThat(message.toString()).doesNotContain(PASSWORD);
    assertThat(message.getCredentials()).isEqualTo(PASSWORD);
    assertThat(message.getPrincipal()).isEqualTo("root");
    assertThat(message.getExtra()).containsEntry("credentials", PASSWORD);
  }

  /**
   * LOGON was already the intended shape - this pins it, so the two handlers cannot drift apart again.
   */
  @Test
  void logonToStringAlsoOmitsTheCredentials() {
    final String rendered = new LogonMessage(
        Map.of("scheme", "basic", "principal", "root", "credentials", PASSWORD)).toString();

    assertThat(rendered).doesNotContain(PASSWORD);
    assertThat(rendered).contains("principal=root");
  }
}
