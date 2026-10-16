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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
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
 * upgrade) than the never-seeded cluster the flag exists for (code review on PR #7748). The stored fingerprint
 * survives the restart, and the document it describes is the one {@code applyReplicated*} wrote to disk in the
 * same breath, so the answer after a restart is the same answer as before it.
 * <p>
 * <b>It validates rather than asserts.</b> What is stored is the fingerprint, not a boolean, and the caller
 * compares it with the document actually in force. So an operator who hand-edits {@code server-users.jsonl}, or a
 * file restored from a backup, no longer matches - and the node goes back to installing unconditionally, which is
 * the safe direction: it cannot refuse an entry its peers accept.
 * <p>
 * <b>A write failure never fails an apply, and it does not have to be survived by this file alone</b> (issue
 * #7752). Failing the apply would make a full or read-only configuration volume stop a node applying committed
 * security entries, which is the crash-loop issue #7137 exists to prevent, and it would not make the record durable
 * either. What makes it durable is the Raft log: {@link #isDurable(String)} tells the HA layer that what is in force
 * here has not reached the disk, and {@code ArcadeStateMachine} then keeps the entry that installed it out of every
 * snapshot checkpoint until it has (see {@code SecurityDurabilityFloor}). A restart in that state replays the entry
 * from the log, judged against the record this file still holds - the baseline as it stood right before that entry,
 * which is exactly the one every peer judged it against - so the node comes back on its peers' baseline by
 * construction instead of by a best-effort write. The entries are re-persisted from memory as the node goes on
 * applying, so the log stops being pinned as soon as the volume is writable again.
 * <p>
 * <b>That is why a failed write leaves the PREVIOUS record on disk</b>, reversing issue #7536, which removed it. Before
 * the replay was guaranteed, a previous record was the worse outcome: the entry that moved the baseline forward
 * could already have been compacted out of the log, so the restarted node refused the very next entry its peers
 * accepted, forever. With the entry guaranteed to replay, removing the record is the harmful choice instead: the
 * file holds all three document kinds, and a replay judged with NO baseline installs the entries its peers refused
 * - for every kind, not only the one whose write failed. It also needed the file to be removable, which a read-only
 * volume does not allow, and which left a stale record behind anyway (the sub-case recorded on issue #7752).
 * <p>
 * <b>The first upgrade to a version carrying this file is a one-entry window, by construction</b>
 * (code review on PR #7748). This file is new, so an already-converged cluster comes up with nothing recorded on
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
  // Holds the write and the update of {@link #persisted} together, so those two never disagree about what is on
  // disk no matter how many threads reach save(). It does NOT make record()'s check-then-write atomic: that rests
  // on the single-writer invariant documented on record(), and the cost of breaking it is named there
  // (code review on PR #7817).
  private final        Object              saveLock  = new Object();
  // Read once at construction and written through on every update, so the common path - one lookup per applied
  // security entry - touches no filesystem.
  private final Map<String, String> fingerprints = new ConcurrentHashMap<>();
  // What the last successful write put on disk, which is NOT the same as what is in force here: a write that
  // failed leaves the two apart, and record() has to know that to skip a write safely (issue #7536).
  private final Map<String, String> persisted    = new ConcurrentHashMap<>();

  public ReplicatedSecurityFingerprintRepository(final String securityConfPath) {
    // Taken as given: {@code new File(parent, child)} joins with a separator whether or not the parent carries
    // one, and this path is only ever used that way.
    this.securityConfPath = securityConfPath;
    load();
  }

  /** The fingerprint of the last replicated {@code document} installed here, or null when there has been none. */
  public String get(final String document) {
    return fingerprints.get(document);
  }

  /**
   * Records that {@code document} in force on this node is now the one the cluster installed, identified by
   * {@code fingerprint}. A write failure is logged and swallowed, and reported by {@link #isDurable(String)} instead,
   * see the class javadoc.
   * <p>
   * A fingerprint that has not changed writes nothing. That is the case for every re-apply of a document this
   * node already holds - the Ratis replay of the entries between the last snapshot and a restart is the routine
   * one - and skipping it keeps this off the {@code fsync} path for exactly the applies that have nothing new to
   * say. The write that remains is one small file per security change that actually changes something, beside
   * the document's own write, which already fsyncs.
   * <p>
   * <b>The check-then-put-then-write is not atomic, and does not need to be, because this runs on ONE thread</b>
   * (code review on PR #7748). Every production caller is an {@code applyReplicated*} inside
   * {@code ArcadeStateMachine}'s {@code applySecurity*Entry}, reachable only from the Raft apply callback, which
   * Ratis serializes per division. A future caller that reaches the single-argument {@code applyReplicated*}
   * overloads from anywhere else would break that, so it is written here rather than left to be inferred: the
   * cost of getting it wrong is two entries racing to record, and the file then naming a document neither of
   * them is the last to have installed.
   */
  public void record(final String document, final String fingerprint) {
    if (fingerprint == null)
      return;

    fingerprints.put(document, fingerprint);

    // Compared against what reached the DISK, not against what is in force here (issue #7536). Both are the same
    // value on every path that worked, so the fsync skip above still applies to every replay; they differ only
    // after a write that failed, and there the old comparison made the loss permanent - the identical document
    // could never repair the file, and only a change to a different one would.
    if (fingerprint.equals(persisted.get(document)))
      return;

    save();
  }

  /**
   * Whether the fingerprint in force for {@code document} is the one on disk, so a restart would read it back (issue
   * #7752). False only after a write that failed and has not been repaired since; a document never recorded is
   * durable, because there is nothing for a restart to lose.
   * <p>
   * Read without {@code saveLock}, from any thread (a metrics scrape, the state machine). A read that overlaps
   * {@code save()} can see {@code persisted} momentarily cleared and answer false for a document that is durable - the
   * safe direction, since it can only keep a snapshot checkpoint back for one more round, never let one through early.
   */
  public boolean isDurable(final String document) {
    return Objects.equals(fingerprints.get(document), persisted.get(document));
  }

  /**
   * Writes what is in force again when a previous write failed, so a volume that has become writable since stops
   * pinning the Raft log (issue #7752). Writes nothing when every fingerprint is already on disk.
   *
   * @return true when every fingerprint in force is on disk afterwards
   */
  public boolean flush() {
    if (persisted.equals(fingerprints))
      return true;
    save();
    return persisted.equals(fingerprints);
  }

  private void load() {
    final File file = new File(securityConfPath, FILE_NAME);
    if (!file.exists())
      return;

    try {
      final JSONObject root = new JSONObject(Files.readString(file.toPath(), DatabaseFactory.getDefaultCharset()));
      for (final String document : root.keySet()) {
        final String fingerprint = root.getString(document, null);
        if (fingerprint != null && !fingerprint.isEmpty()) {
          fingerprints.put(document, fingerprint);
          persisted.put(document, fingerprint);
        }
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
    final Map<String, String> snapshot = new LinkedHashMap<>(fingerprints);
    final JSONObject root = new JSONObject();
    for (final Map.Entry<String, String> entry : snapshot.entrySet())
      root.put(entry.getKey(), entry.getValue());
    final byte[] bytes = root.toString().getBytes(DatabaseFactory.getDefaultCharset());

    final File file = new File(securityConfPath, FILE_NAME);
    synchronized (saveLock) {
      try {
        final File dir = file.getParentFile();
        if (dir != null && !dir.exists())
          dir.mkdirs();

        writeAtomically(file.toPath(), bytes);

        persisted.clear();
        persisted.putAll(snapshot);
      } catch (final IOException | RuntimeException e) {
        // The previous record stays on disk, and so does persisted, which describes it (issue #7752): see the class
        // javadoc for why it is the baseline a replay of the entry needs, and why removing it is the harmful choice.
        LogManager.instance().log(this, Level.SEVERE,
            "Could not write '%s'. The replicated security document IS installed on this node and IS in force; what "
                + "failed is recording that it came from the cluster. The Raft log keeps the entry that installed it "
                + "until the record reaches the disk, so a restart replays it rather than losing it, and the write is "
                + "retried as this node applies further entries. Free or fix the configuration volume: %s",
            e, FILE_NAME, e.getMessage());
      }
    }
  }

  /**
   * The atomic write, isolated from {@link #save()}'s failure policy so a test can inject the one failure that
   * policy is entirely about, and so the two read as what they are: a write, and what to do when it does not
   * happen. Package-private and overridable rather than private for that reason only - production has one
   * implementation, this one.
   * <p>
   * Same temp-file-then-atomic-rename shape the documents themselves use: a crash mid-write can only damage the
   * throwaway file, so the target is either the previous complete value or the new one - never a truncated
   * string that would read as a fingerprint matching nothing.
   */
  void writeAtomically(final Path target, final byte[] bytes) throws IOException {
    final Path tmp = Files.createTempFile(target.getParent(), FILE_NAME, ".tmp");
    try {
      try (final FileChannel channel = FileChannel.open(tmp, StandardOpenOption.WRITE)) {
        // Looped, because FileChannel.write is only obliged to consume SOME of what remains (CodeRabbit on PR
        // #7817). A short write here would fsync and publish a truncated JSON object, which load() cannot parse
        // and therefore treats as absent - silently dropping the compare-and-set baseline this class exists to
        // keep, and doing it on the path where the write reported success, so nothing would log.
        final ByteBuffer buffer = ByteBuffer.wrap(bytes);
        while (buffer.hasRemaining())
          channel.write(buffer);
        channel.force(true);
      }

      // Owner-only before publishing, the same as every other file in this directory (code review on PR
      // #7748). What is stored is a digest of a document rather than credential material, but an attacker
      // who can read it can confirm a candidate copy of the security document - an exfiltrated backup, say -
      // against what this node has installed, and a convention that holds for three files in a directory
      // and not the fourth is one nobody can rely on.
      SecurityUserFileRepository.applyOwnerOnlyPermissions(tmp);

      try {
        Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      } catch (final AtomicMoveNotSupportedException e) {
        Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
      }
    } finally {
      // Swallowed rather than propagated, because by here the rename may ALREADY have published the new value:
      // letting a cleanup failure out would send save() into its failure arm, which reports a record that had just
      // been written correctly as not durable - and pins the Raft log over a stray temp file (issue #7752).
      try {
        Files.deleteIfExists(tmp);
      } catch (final IOException | RuntimeException e) {
        LogManager.instance().log(this, Level.FINE, "Could not remove the temporary file '%s'", e, tmp);
      }
    }
  }
}
