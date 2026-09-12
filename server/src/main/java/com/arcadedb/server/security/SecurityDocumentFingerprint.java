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
package com.arcadedb.server.security;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;

/**
 * The compare-and-set precondition of a replicated security document (issue #7509).
 * <p>
 * The user list, the group document and the API-token document are replicated whole: the submitter reads the
 * current one, mutates a copy and submits it. The read-compute-submit sequence is serialised by a per-NODE
 * monitor, so two nodes can each build a document from their own view and the one that Raft orders second
 * silently reverts the first. The fix carries this fingerprint of the document the submitter READ alongside
 * the document it submits, and the apply - the only point that is linearised across nodes - refuses an entry
 * whose fingerprint no longer matches what is in force.
 * <p>
 * <b>Why it is not a hash of the payload string.</b> Two nodes holding the same logical document do not
 * necessarily serialise it the same way: the user map is a {@code ConcurrentHashMap} and the group document is
 * assembled key by key, so iteration order is not part of the state and must not be part of the fingerprint.
 * {@link #canonicalize} therefore sorts object keys and array elements before hashing, which makes the
 * fingerprint a function of the document's CONTENT. All three documents are sets - a user list, a token list,
 * a map of databases to groups - so sorting an array loses nothing that the documents mean.
 */
public final class SecurityDocumentFingerprint {

  private SecurityDocumentFingerprint() {
    // utility class
  }

  /**
   * The fingerprint of a security document, as the hex SHA-256 of its canonical form.
   *
   * @param json the document, either a JSON object (groups, API tokens) or a JSON array (users)
   *
   * @return the fingerprint, never null
   *
   * @throws IllegalArgumentException when the string is not JSON this method can canonicalise
   */
  public static String of(final String json) {
    if (json == null)
      throw new IllegalArgumentException("Cannot fingerprint a null security document");

    final StringBuilder canonical = new StringBuilder(json.length());
    canonicalize(parse(json), canonical);
    return sha256Hex(canonical.toString());
  }

  private static Object parse(final String json) {
    final String trimmed = json.trim();
    try {
      if (trimmed.startsWith("["))
        return new JSONArray(trimmed);
      return new JSONObject(trimmed);
    } catch (final RuntimeException e) {
      throw new IllegalArgumentException("Security document is not valid JSON and cannot be fingerprinted", e);
    }
  }

  /**
   * Appends {@code value}'s canonical form to {@code out}: object keys in ascending order, array elements
   * ordered by their own canonical form, scalars as their {@code toString}. Strings are quoted and every
   * quote and backslash inside them escaped, so {@code ["a","b"]} and {@code ["ab"]} cannot collide.
   */
  private static void canonicalize(final Object value, final StringBuilder out) {
    switch (value) {
    case final JSONObject object -> {
      out.append('{');
      boolean first = true;
      for (final String key : new TreeSet<>(object.keySet())) {
        if (!first)
          out.append(',');
        first = false;
        appendString(key, out);
        out.append(':');
        canonicalize(object.get(key), out);
      }
      out.append('}');
    }
    case final JSONArray array -> {
      final List<String> elements = new ArrayList<>(array.length());
      for (int i = 0; i < array.length(); i++) {
        final StringBuilder element = new StringBuilder();
        canonicalize(array.get(i), element);
        elements.add(element.toString());
      }
      Collections.sort(elements);
      out.append('[');
      for (int i = 0; i < elements.size(); i++) {
        if (i > 0)
          out.append(',');
        out.append(elements.get(i));
      }
      out.append(']');
    }
    case null -> out.append("null");
    case final String string -> appendString(string, out);
    default -> out.append(value);
    }
  }

  private static void appendString(final String value, final StringBuilder out) {
    out.append('"');
    for (int i = 0; i < value.length(); i++) {
      final char c = value.charAt(i);
      if (c == '"' || c == '\\')
        out.append('\\');
      out.append(c);
    }
    out.append('"');
  }

  private static String sha256Hex(final String canonical) {
    final byte[] digest;
    try {
      digest = MessageDigest.getInstance("SHA-256").digest(canonical.getBytes(StandardCharsets.UTF_8));
    } catch (final NoSuchAlgorithmException e) {
      // SHA-256 is mandated by the JLS for every conformant JRE, so this arm is unreachable in practice; it
      // exists because MessageDigest declares the checked exception.
      throw new IllegalStateException("SHA-256 is not available in this JRE", e);
    }
    final StringBuilder hex = new StringBuilder(digest.length * 2);
    for (final byte b : digest)
      hex.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
    return hex.toString();
  }
}
