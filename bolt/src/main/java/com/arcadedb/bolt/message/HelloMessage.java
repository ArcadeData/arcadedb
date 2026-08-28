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
package com.arcadedb.bolt.message;

import com.arcadedb.bolt.packstream.PackStreamWriter;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * HELLO message for initializing connection with authentication.
 * Signature: 0x01
 * Fields: extra (Map containing user_agent, scheme, principal, credentials, etc.)
 */
public class HelloMessage extends BoltMessage {
  private static final String CREDENTIALS_KEY = "credentials";
  private static final String REDACTED        = "***";

  private final Map<String, Object> extra;

  public HelloMessage(final Map<String, Object> extra) {
    super(HELLO);
    this.extra = extra != null ? extra : Map.of();
  }

  public Map<String, Object> getExtra() {
    return extra;
  }

  public String getUserAgent() {
    return (String) extra.get("user_agent");
  }

  public String getScheme() {
    return (String) extra.get("scheme");
  }

  public String getPrincipal() {
    return (String) extra.get("principal");
  }

  public String getCredentials() {
    return (String) extra.get(CREDENTIALS_KEY);
  }

  public String getRouting() {
    final Object routing = extra.get("routing");
    if (routing instanceof Map) {
      final Object address = ((Map<?, ?>) routing).get("address");
      return address != null ? address.toString() : null;
    }
    return null;
  }

  @Override
  public void writeTo(final PackStreamWriter writer) throws IOException {
    writer.writeStructureHeader(signature, 1);
    writer.writeMap(extra);
  }

  /**
   * Never renders the caller's credentials: for every BOLT version below 5.1 the HELLO extra map carries the
   * cleartext password, and {@code BoltNetworkExecutor} logs every inbound message through {@code toString()}
   * when {@code arcadedb.bolt.debug} is enabled - which is exactly the setting an operator turns on to
   * troubleshoot a connection problem (issue #6801). {@code LogonMessage.toString()} already omits it.
   */
  @Override
  public String toString() {
    if (!extra.containsKey(CREDENTIALS_KEY))
      return "HELLO{extra=" + extra + "}";

    // Only allocated on the debug-logging path of a pre-5.1 (or explicitly authenticated) HELLO.
    final Map<String, Object> redacted = new LinkedHashMap<>(extra);
    redacted.put(CREDENTIALS_KEY, REDACTED);
    return "HELLO{extra=" + redacted + "}";
  }
}
