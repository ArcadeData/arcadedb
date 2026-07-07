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

import com.arcadedb.bolt.packstream.PackStreamReader;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Shared helpers for decoding a Bolt ROUTE SUCCESS response into its routing table, used by the ROUTE
 * tests so the SUCCESS/{@code rt} wire shape is parsed in a single place.
 */
final class BoltRouteTestSupport {

  private BoltRouteTestSupport() {
  }

  /**
   * Extracts the {@code rt} routing table from a ROUTE SUCCESS message. SUCCESS is a single-field
   * structure: [struct marker][signature][metadata map]. Skips the two header bytes, reads the metadata
   * map, then returns its {@code rt} entry.
   */
  @SuppressWarnings("unchecked")
  static Map<String, Object> readRoutingTable(final byte[] successResponse) throws IOException {
    final PackStreamReader reader = new PackStreamReader(successResponse);
    reader.readRawByte();
    reader.readRawByte();
    final Map<String, Object> metadata = (Map<String, Object>) reader.readValue();
    return (Map<String, Object>) metadata.get("rt");
  }

  /**
   * Returns the advertised addresses for a routing role (WRITE, READ, or ROUTE), or an empty list when
   * the role is absent.
   */
  @SuppressWarnings("unchecked")
  static List<String> addressesForRole(final Map<String, Object> rt, final String role) {
    final List<Map<String, Object>> servers = (List<Map<String, Object>>) rt.get("servers");
    for (final Map<String, Object> server : servers)
      if (role.equals(server.get("role")))
        return (List<String>) server.get("addresses");
    return List.of();
  }
}
