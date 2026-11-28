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
package com.arcadedb.bench.workload.ha;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

/**
 * The content of one type on one node as one SHA-256: every record, read in {@code @rid} order, as its JSON with the keys
 * sorted (at every depth) followed by {@code \n}. The record's {@code @rid} is part of its JSON, so the same contents at other
 * rids are another digest. Two nodes holding the same records give the same digest whatever order their JSON keys came in.
 */
public final class ContentDigest {
  private ContentDigest() {
  }

  /** The hex SHA-256 of the records, which must come in {@code @rid} order. */
  public static String of(final Iterator<JSONObject> recordsInRidOrder) {
    final MessageDigest sha;
    try {
      sha = MessageDigest.getInstance("SHA-256");
    } catch (final NoSuchAlgorithmException e) {
      throw new IllegalStateException("no SHA-256 in this JVM", e);
    }
    while (recordsInRidOrder.hasNext()) {
      sha.update(sorted(recordsInRidOrder.next()).toString().getBytes(StandardCharsets.UTF_8));
      sha.update((byte) '\n');
    }
    return HexFormat.of().formatHex(sha.digest());
  }

  /**
   * One {@code ha-<i> differs from the leader on <type>} line per node whose digest is not the leader's, the leader being
   * {@code ha-0}.
   */
  public static List<String> differences(final String type, final String[] digestsByNode) {
    return differences(type, digestsByNode, 0);
  }

  /** As {@link #differences(String, String[])}, the leader being {@code ha-<leader>}. */
  public static List<String> differences(final String type, final String[] digestsByNode, final int leader) {
    final List<String> lines = new ArrayList<>();
    for (int i = 0; i < digestsByNode.length; i++)
      if (i != leader && !digestsByNode[leader].equals(digestsByNode[i]))
        lines.add("ha-" + i + " differs from the leader on " + type);
    return lines;
  }

  /** A copy with the keys in ascending order at every depth: the JSON object keeps the order its keys were put in. */
  static JSONObject sorted(final JSONObject json) {
    final JSONObject out = new JSONObject();
    for (final String key : new TreeSet<>(json.keySet()))
      out.put(key, sortedValue(json.get(key)));
    return out;
  }

  private static Object sortedValue(final Object value) {
    if (value instanceof JSONObject object)
      return sorted(object);
    if (value instanceof JSONArray array) {
      final JSONArray out = new JSONArray();
      for (int i = 0; i < array.length(); i++)
        out.put(sortedValue(array.get(i)));
      return out;
    }
    return value;
  }
}
