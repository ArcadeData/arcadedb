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
 * DISCARD message to discard remaining records from result stream.
 * Signature: 0x2F
 * Fields: extra (Map containing n, qid)
 */
public class DiscardMessage extends BoltMessage {
  private final Map<String, Object> extra;

  public DiscardMessage(final Map<String, Object> extra) {
    super(DISCARD);
    this.extra = extra != null ? extra : Map.of();
  }

  public Map<String, Object> getExtra() {
    return extra;
  }

  /**
   * Get the number of records to discard. -1 means all records.
   */
  public long getN() {
    final Object n = extra.get("n");
    if (n == null) {
      return -1; // Default: discard all
    }
    return ((Number) n).longValue();
  }

  /**
   * Get the query ID (qid) for which to discard records.
   * -1 means the last executed query.
   */
  public long getQid() {
    final Object qid = extra.get("qid");
    if (qid == null) {
      return -1; // Default: last query
    }
    return ((Number) qid).longValue();
  }

  @Override
  public void writeTo(final PackStreamWriter writer) throws IOException {
    writer.writeStructureHeader(signature, 1);
    writer.writeMap(extra);
  }

  @Override
  public String toString() {
    return "DISCARD{n=" + getN() + ", qid=" + getQid() + ", extra=" + extra + "}";
  }
}
