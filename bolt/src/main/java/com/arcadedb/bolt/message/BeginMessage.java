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
 * BEGIN message to start an explicit transaction.
 * Signature: 0x11
 * Fields: extra (Map containing db, bookmarks, tx_timeout, tx_metadata, mode, etc.)
 */
public class BeginMessage extends BoltMessage {
  private final Map<String, Object> extra;

  public BeginMessage(final Map<String, Object> extra) {
    super(BEGIN);
    this.extra = extra != null ? extra : Map.of();
  }

  public Map<String, Object> getExtra() {
    return extra;
  }

  public String getDatabase() {
    return (String) extra.get("db");
  }

  public String getMode() {
    return (String) extra.get("mode");
  }

  @Override
  public void writeTo(final PackStreamWriter writer) throws IOException {
    writer.writeStructureHeader(signature, 1);
    writer.writeMap(extra);
  }

  @Override
  public String toString() {
    return "BEGIN{extra=" + extra + "}";
  }
}
