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
package com.arcadedb.database;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;

/**
 * Deterministic SHA-256 fingerprint over the persistent content of an ArcadeDB database directory.
 * <p>
 * Used by the HA bootstrap path (issue #4147): when the operator pre-stages identical database files
 * on every pod, peers exchange fingerprints at first cluster formation and confirm byte-level identity
 * before agreeing to skip the leader-shipped snapshot transfer.
 * <p>
 * <b>What is hashed.</b> Files in the database root directory whose name ends with one of the
 * extensions in {@link #DEFAULT_INCLUDED_EXTENSIONS} (data files + schema + configuration). Files are
 * visited in canonical (lexicographic by name) order so the result is reproducible across pods.
 * For each included file the digest absorbs:
 * <ol>
 *   <li>the file name (UTF-8) length-prefixed,</li>
 *   <li>the file size as a big-endian long,</li>
 *   <li>the streamed file content.</li>
 * </ol>
 * <p>
 * <b>What is excluded.</b> WAL files ({@code .wal}), lock files, files in subdirectories (the WAL
 * dir, external-bucket dirs, etc.). The persisted last-applied transaction id is tracked separately
 * (see {@code LocalDatabase.getLastTransactionId()}) and serves as the recency signal; including
 * the WAL in the fingerprint would create false-mismatches between two pods staged from the same
 * backup that happen to have different WAL rotation states.
 * <p>
 * <b>Stability.</b> The fingerprint changes whenever the persisted state changes, AND whenever the
 * file layout drifts due to legitimate non-determinism (compaction order, page allocation). It is
 * therefore meaningful only at the cold-start boundary, not as a runtime consistency check. See
 * the design discussion on issue #4147.
 */
public final class BootstrapFingerprint {

  /** Files matching one of these suffixes are included. Anything else (including {@code .wal}) is skipped. */
  public static final List<String> DEFAULT_INCLUDED_EXTENSIONS = List.of(
      ".bucket",
      ".unotidx",
      ".unique",
      ".vector",
      ".svg",
      ".svgraph",
      ".tsbucket",
      ".huniq",
      ".hnotuniq",
      ".dict",
      "schema.json",
      "schema.prev.json",
      "configuration.json");

  private BootstrapFingerprint() {
  }

  /**
   * Compute the fingerprint for the database whose files live directly under {@code databaseDir}.
   * Returns a 64-character lowercase hex string (SHA-256). Returns the empty-content digest when the
   * directory does not exist or contains no included files.
   */
  public static String compute(final File databaseDir) {
    return compute(databaseDir, DEFAULT_INCLUDED_EXTENSIONS);
  }

  /**
   * Compute the fingerprint over files under {@code databaseDir} whose names end with one of the
   * given suffixes. Visible for tests so they can pin down the fingerprint surface.
   */
  public static String compute(final File databaseDir, final List<String> includedSuffixes) {
    final MessageDigest md = sha256();

    if (databaseDir == null || !databaseDir.isDirectory())
      return HexFormat.of().formatHex(md.digest());

    final File[] children = databaseDir.listFiles(File::isFile);
    if (children == null || children.length == 0)
      return HexFormat.of().formatHex(md.digest());

    // Filter then sort by name. Canonical ordering is what makes the result reproducible across pods.
    final List<File> included = new ArrayList<>(children.length);
    for (final File f : children)
      if (matches(f.getName(), includedSuffixes))
        included.add(f);
    Collections.sort(included, (a, b) -> a.getName().compareTo(b.getName()));

    for (final File f : included)
      absorbFile(md, f);

    return HexFormat.of().formatHex(md.digest());
  }

  private static boolean matches(final String name, final List<String> suffixes) {
    for (final String s : suffixes)
      if (name.endsWith(s))
        return true;
    return false;
  }

  private static void absorbFile(final MessageDigest md, final File f) {
    final byte[] nameBytes = f.getName().getBytes(StandardCharsets.UTF_8);
    md.update(longToBytes(nameBytes.length));
    md.update(nameBytes);
    md.update(longToBytes(f.length()));
    try (final InputStream in = Files.newInputStream(f.toPath())) {
      final byte[] buf = new byte[64 * 1024];
      int n;
      while ((n = in.read(buf)) > 0)
        md.update(buf, 0, n);
    } catch (final IOException e) {
      // A read error makes the fingerprint meaningless. Surface as runtime so the bootstrap path
      // refuses to proceed; the operator must fix the file before the cluster forms.
      throw new RuntimeException("BootstrapFingerprint: cannot read " + f.getAbsolutePath(), e);
    }
  }

  private static byte[] longToBytes(final long v) {
    final byte[] out = new byte[8];
    long w = v;
    for (int i = 7; i >= 0; i--) {
      out[i] = (byte) (w & 0xff);
      w >>>= 8;
    }
    return out;
  }

  private static MessageDigest sha256() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (final NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 must be available in every standard JVM", e);
    }
  }
}
