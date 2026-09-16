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

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

/**
 * The fingerprint of the last REPLICATED document this node installed, per node-scoped security document, kept in
 * the configuration directory beside the documents themselves (issue #7693).
 * <p>
 * It answers one question, and only that one: <b>is the document this node currently holds one the cluster
 * installed, or is it this node's own local file?</b> {@code ServerSecurity.isSuperseded} needs the answer,
 * because the compare-and-set of issue #7509 compares the fingerprint the SUBMITTER read against the fingerprint
 * of the applying node's own document, and those are comparable only once every node holds the same document. A
 * node that has never applied a replicated one holds whatever its own bootstrap wrote - a {@code root} with an
 * independently salted password hash - so a refusal computed from it is not the same refusal its peers compute,
 * and a non-uniform refusal splits the cluster's security state instead of protecting it.
 * <p>
 * <b>Why it is on disk rather than a flag in memory.</b> An in-memory flag is reset by every restart, so a node
 * that had long since converged would go back to "cannot judge" on each start and install the next entry
 * unconditionally - reopening the identical bypass on a far more common trigger (a routine restart or a rolling
 * upgrade) than the never-seeded cluster the flag exists for (claude-review on PR #7748). The stored fingerprint
 * survives the restart, and the document it describes is the one {@code applyReplicated*} wrote to disk in the
 * same breath, so the answer after a restart is the same answer as before it.
 * <p>
 * <b>It validates rather than asserts.</b> What is stored is the fingerprint, not a boolean, and the caller
 * compares it with the document actually in force. So an operator who hand-edits {@code server-users.jsonl}, or a
 * file restored from a backup, no longer matches - and the node goes back to installing unconditionally, which is
 * the safe direction: it cannot refuse an entry its peers accept.
 * <p>
 * A write failure is not fatal and never fails an apply. The cost of losing it is one entry installed
 * unconditionally after the next restart, which is exactly the pre-#7509 behaviour; the cost of failing the apply
 * would be a node that stops applying committed security entries because a marker file could not be written.
 * <p>
 * <b>The first upgrade to a version carrying this file is the same one-entry window, by construction</b>
 * (claude-review on PR #7748). This file is new, so an already-converged cluster comes up with nothing recorded on
 * any node, and the first security entry after the upgrade installs without the compare-and-set of issue #7509 -
 * once per document kind. It is uniform (every node is in the same state, so none refuses what another installs)
 * and it is self-closing (that entry records the fingerprint everywhere), but for that one entry a concurrent
 * change on two nodes can still lose one of them, as it did before #7509. Nothing has to be done about it; it is
 * recorded here so it is not rediscovered as a surprise.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ReplicatedSecurityFingerprintRepository {
  public static final String USERS      = "users";
  public static final String GROUPS     = "groups";
  public static final String API_TOKENS = "apiTokens";

  public static final  String              FILE_NAME = "server-security-cluster.json";
  private final        String              securityConfPath;
  private final        Object              saveLock  = new Object();
  // Read once at construction and written through on every update, so the common path - one lookup per applied
  // security entry - touches no filesystem.
  private final Map<String, String> fingerprints = new ConcurrentHashMap<>();

  public ReplicatedSecurityFingerprintRepository(String securityConfPath) {
    if (!securityConfPath.endsWith(File.separator))
      securityConfPath += File.separator;
    this.securityConfPath = securityConfPath;
    load();
  }

  /** The fingerprint of the last replicated {@code document} installed here, or null when there has been none. */
  public String get(final String document) {
    return fingerprints.get(document);
  }

  /**
   * Records that {@code document} in force on this node is now the one the cluster installed, identified by
   * {@code fingerprint}. Best-effort: a write failure is logged and swallowed, see the class javadoc.
   */
  public void record(final String document, final String fingerprint) {
    fingerprints.put(document, fingerprint);
    save();
  }

  private void load() {
    final File file = new File(securityConfPath, FILE_NAME);
    if (!file.exists())
      return;

    try {
      final JSONObject root = new JSONObject(Files.readString(file.toPath(), DatabaseFactory.getDefaultCharset()));
      for (final String document : root.keySet()) {
        final String fingerprint = root.getString(document, null);
        if (fingerprint != null && !fingerprint.isEmpty())
          fingerprints.put(document, fingerprint);
      }
    } catch (final Exception e) {
      // A file this node cannot read is treated as absent, which costs the compare-and-set one entry and is the
      // fail-open direction. Refusing to start over it would be a hard failure for a marker whose whole purpose
      // is to make a soft one rarer.
      LogManager.instance().log(this, Level.WARNING,
          "Could not read '%s'; the replicated-security concurrency check re-engages after the next security "
              + "entry applies on this node: %s", e, FILE_NAME, e.getMessage());
    }
  }

  private void save() {
    final JSONObject root = new JSONObject();
    for (final Map.Entry<String, String> entry : fingerprints.entrySet())
      root.put(entry.getKey(), entry.getValue());
    final byte[] bytes = root.toString().getBytes(DatabaseFactory.getDefaultCharset());

    synchronized (saveLock) {
      try {
        final File file = new File(securityConfPath, FILE_NAME);
        final File dir = file.getParentFile();
        if (dir != null && !dir.exists())
          dir.mkdirs();

        // Same temp-file-then-atomic-rename shape the documents themselves use: a crash mid-write can only
        // damage the throwaway file, so this one is either the previous complete value or the new one - never a
        // truncated string that would read as a fingerprint matching nothing.
        final Path target = file.toPath();
        final Path tmp = Files.createTempFile(target.getParent(), FILE_NAME, ".tmp");
        try {
          try (final FileChannel channel = FileChannel.open(tmp, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(bytes));
            channel.force(true);
          }
          try {
            Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
          } catch (final AtomicMoveNotSupportedException e) {
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
          }
        } finally {
          Files.deleteIfExists(tmp);
        }
      } catch (final IOException | RuntimeException e) {
        LogManager.instance().log(this, Level.WARNING,
            "Could not write '%s'. The replicated security document IS installed on this node; what failed is "
                + "recording that it came from the cluster, so a restart before the next security entry lets this "
                + "node install one entry without the concurrency check of issue #7509: %s", e, FILE_NAME,
            e.getMessage());
      }
    }
  }
}
