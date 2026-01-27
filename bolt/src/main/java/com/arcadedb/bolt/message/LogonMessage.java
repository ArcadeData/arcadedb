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
import java.util.Map;

/**
 * LOGON message for authentication in BOLT 5.1+.
 * Signature: 0x6A
 * Fields: auth (Map containing scheme, principal, credentials)
 */
public class LogonMessage extends BoltMessage {
  private final Map<String, Object> auth;

  public LogonMessage(final Map<String, Object> auth) {
    super(LOGON);
    this.auth = auth != null ? auth : Map.of();
  }

  public Map<String, Object> getAuth() {
    return auth;
  }

  public String getScheme() {
    return (String) auth.get("scheme");
  }

  public String getPrincipal() {
    return (String) auth.get("principal");
  }

  public String getCredentials() {
    return (String) auth.get("credentials");
  }

  @Override
  public void writeTo(final PackStreamWriter writer) throws IOException {
    writer.writeStructureHeader(signature, 1);
    writer.writeMap(auth);
  }

  @Override
  public String toString() {
    return "LOGON{scheme=" + getScheme() + ", principal=" + getPrincipal() + "}";
  }
}
