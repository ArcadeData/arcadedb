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
package com.arcadedb;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * The identifier ArcadeDB generates for an instance (standalone server, HA node or embedded engine) so support can tell
 * which instance is talking. The customer copies it from the log or from the server information into the support portal.
 * <p>
 * Format: {@code adb-} followed by a canonical lowercase UUID, exactly 40 characters, e.g.
 * {@code adb-123e4567-e89b-12d3-a456-426614174000}. It is NOT a credential and is never used for authentication.
 * <p>
 * The server persists it in the file {@code .instance.id} of its databases directory (the directory that survives a
 * restart wherever the data does), falling back to the file {@code instance.id} of the configuration directory, which is
 * where older versions kept it. The file also records the name of the server that created it: when a volume or a
 * directory is cloned to create another node the names differ, a new id is generated and the log says so. The file is
 * node-local: it is never replicated between HA nodes. A server with no persistent directory (a container without a
 * volume) can instead set {@code arcadedb.instance.derived=true} to get an id computed from the cluster and server names
 * ({@link #derive(String, String)}), or set {@code arcadedb.instance.id}.
 */
public final class InstanceId {
  public static final String PREFIX = "adb-";

  private static final Pattern PATTERN = Pattern.compile(
      "^adb-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$");

  private InstanceId() {
  }

  /**
   * Tells whether {@code value} is exactly a canonical (lowercase) instance id. No trimming or case folding is applied,
   * use {@link #normalize(String)} first.
   */
  public static boolean isValid(final String value) {
    return value != null && PATTERN.matcher(value).matches();
  }

  /**
   * Trims and lower-cases {@code value}. Returns the normalized id, or null when the result is not a valid instance id.
   */
  public static String normalize(final String value) {
    if (value == null)
      return null;
    final String normalized = value.trim().toLowerCase(Locale.ROOT);
    return isValid(normalized) ? normalized : null;
  }

  public static String generate() {
    return PREFIX + UUID.randomUUID();
  }

  /** Fixed namespace of the name-based ids ({@link #derive}): never change it, it would change every derived id. */
  private static final UUID DERIVED_NAMESPACE = UUID.fromString("3f6d2a52-7c1e-4b8a-9d44-0a1c5e7f2b90");

  /**
   * The id of a server that has no persistent directory: a name-based UUID (version 5, RFC 4122) of
   * {@code <clusterName>/<serverName>}, so the same server gets the same id on every start with no file. SHA-1 is used
   * because the version 5 definition asks for it: the id is an identifier, never a credential.
   */
  public static String derive(final String clusterName, final String serverName) {
    final byte[] name = ((clusterName == null ? "" : clusterName) + "/" + (serverName == null ? "" : serverName)).getBytes(
        StandardCharsets.UTF_8);
    final MessageDigest sha1;
    try {
      sha1 = MessageDigest.getInstance("SHA-1");
    } catch (final NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-1 is not available", e);
    }
    sha1.update(ByteBuffer.allocate(16).putLong(DERIVED_NAMESPACE.getMostSignificantBits())
        .putLong(DERIVED_NAMESPACE.getLeastSignificantBits()).array());
    final byte[] hash = sha1.digest(name);
    hash[6] = (byte) ((hash[6] & 0x0f) | 0x50);
    hash[8] = (byte) ((hash[8] & 0x3f) | 0x80);
    final ByteBuffer buffer = ByteBuffer.wrap(hash);
    return PREFIX + new UUID(buffer.getLong(), buffer.getLong());
  }
}
