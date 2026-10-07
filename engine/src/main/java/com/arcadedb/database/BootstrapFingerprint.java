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

import com.arcadedb.log.LogManager;
import com.arcadedb.schema.LocalSchema;

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
import java.util.logging.Level;

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
      LocalSchema.SCHEMA_FILE_NAME,
      LocalSchema.SCHEMA_PREV_FILE_NAME,
      LocalDatabase.CONFIGURATION_FILE_NAME);

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
   * Upper bound, in milliseconds, of the flush wait {@link #computeSettled(LocalDatabase)} takes before hashing (issue
   * #8843). It caps only the pathological cases - a wedged disk, a flush suspended for a long backup - because the wait
   * is already bounded by the backlog found at the call, never by later commits: on a quiet database it is the time
   * the last commit's pages take to land, normally milliseconds.
   */
  public static final long SETTLE_MAX_WAIT_MILLIS = 5_000L;

  /**
   * Fingerprint of an OPEN database, taken once the pages of the commits made so far have reached the disk (issue
   * #8843). {@link #compute(File)} hashes the files as they are, and a commit hands its pages to the asynchronous flush
   * thread rather than writing them itself, so two fingerprints of one unchanged copy taken either side of a flush
   * would differ - and the bootstrap protocol would read a matching peer as a mismatched one. Every caller that
   * fingerprints a database the server holds open must go through here.
   * <p>
   * The wait is bounded by {@link #SETTLE_MAX_WAIT_MILLIS}; see {@link #computeSettled(LocalDatabase, long)}.
   */
  public static String computeSettled(final LocalDatabase database) {
    return computeSettled(database, SETTLE_MAX_WAIT_MILLIS);
  }

  /**
   * {@link #computeSettled(LocalDatabase)} with an explicit bound. When the backlog does not drain in time the
   * fingerprint is computed anyway, over the files as they stand, and the outcome is logged: a database whose flush
   * cannot drain is either under writes - its fingerprint drifts with every commit, settled or not - or held by
   * something else (a wedged disk, a suspended flush), and in both cases a caller blocked indefinitely is worse than a
   * digest the bootstrap protocol already treats as a mismatch.
   */
  public static String computeSettled(final LocalDatabase database, final long maxWaitMillis) {
    if (!database.getPageManager().waitPagesPendingNowOfDatabaseAreFlushed(database, maxWaitMillis))
      // A zero bound is a sweep whose shared budget an earlier database already spent, and that one was logged at
      // WARNING: the rest of the sweep is reported at FINE so one wedged flush does not log once per database.
      LogManager.instance().log(BootstrapFingerprint.class, maxWaitMillis > 0 ? Level.WARNING : Level.FINE,
          "Bootstrap fingerprint of database '%s': the pending page flushes did not reach the disk within %d ms, "
              + "hashing the files as they stand (the fingerprint may not match a settled copy of the same data)",
          database.getName(), maxWaitMillis);
    return compute(new File(database.getDatabasePath()));
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
