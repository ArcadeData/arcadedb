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
 * <b>A write failure is not fatal and never fails an apply, and what it leaves behind is a DIVERGENCE risk, not
 * the pre-#7509 one</b> (CodeRabbit on PR #7748, correcting what this paragraph used to claim). Pre-#7509 a
 * conditional entry did not exist, so every node installed and none of them disagreed. Here, a node that lost this
 * write and then restarted comes up with no recorded fingerprint while its peers still have theirs, so a later
 * entry carrying a precondition the cluster has moved past is REFUSED by the peers and INSTALLED here - which can
 * put a revoked user, grant or token back on this one node. It is narrow: it needs the marker write to fail while
 * the document's own write, into the same directory and moments earlier, succeeded; then a restart; then a
 * genuine compare-and-set race as the first security entry afterwards. It is also self-closing, because the next
 * security change that does persist records every document kind again. But it is real, and closing it properly
 * means making the marker part of the replicated protocol - carried in the entry, or gated behind a resync - which
 * is a change of a different size than this one and is filed separately, as issue #7752.
 * <p>
 * <b>"No recorded fingerprint" is what a failed write leaves behind because {@link #save()} makes it so</b>
 * (issue #7536). Left to itself, a failed write leaves the PREVIOUS record on disk, and that is the worse of the
 * two outcomes by a wide margin: a node whose baseline is one change behind its peers' refuses the very next
 * entry they accept, with no race and no second coincidence, and it goes on refusing, because nothing it refuses
 * moves its baseline forward. A node with no record installs and is back on its peers' baseline as soon as the
 * next entry applies. {@code save()} therefore REMOVES the file when it cannot rewrite it - see
 * {@code discardStaleRecord} - which turns the unconditional split of #7536 into the race-conditional one of
 * #7752. The document that was recorded stays in force in memory, so the running node judges correctly until it
 * restarts; only the restart loses it.
 * <p>
 * Failing the apply instead is not the alternative it looks like: it would make a full or read-only configuration
 * volume stop a node applying committed security entries, which is the crash-loop issue #7137 exists to prevent,
 * and it would not make the marker durable either. So the failure is reported at SEVERE with the consequence
 * named, and the instruction is the one #7137 and #7227 already give for the document's own write: fix the volume
 * and reissue the security change, which records every fingerprint again.
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
   * {@code fingerprint}. Best-effort: a write failure is logged and swallowed, see the class javadoc.
   * <p>
   * A fingerprint that has not changed writes nothing. That is the case for every re-apply of a document this
   * node already holds - the Ratis replay of the entries between the last snapshot and a restart is the routine
   * one - and skipping it keeps this off the {@code fsync} path for exactly the applies that have nothing new to
   * say. The write that remains is one small file per security change that actually changes something, beside
   * the document's own write, which already fsyncs.
   * <p>
   * <b>The check-then-put-then-write is not atomic, and does not need to be, because this runs on ONE thread</b>
   * (claude-review on PR #7748). Every production caller is an {@code applyReplicated*} inside
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
        discardStaleRecord(file, e);
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

      // Owner-only before publishing, the same as every other file in this directory (claude-review on PR
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
      // letting a cleanup failure out would send save() into discardStaleRecord and delete the record that had
      // just been written correctly, turning a stray temp file into the loss this class exists to avoid.
      try {
        Files.deleteIfExists(tmp);
      } catch (final IOException | RuntimeException e) {
        LogManager.instance().log(this, Level.FINE, "Could not remove the temporary file '%s'", e, tmp);
      }
    }
  }

  /**
   * Removes the marker file after a write that failed, so what survives the next restart is NOTHING rather than
   * the fingerprint of a document the cluster has moved past (issue #7536). Called with {@link #saveLock} held.
   * <p>
   * <b>The stale value is the harmful one, not the missing one.</b> {@code ServerSecurity.isSuperseded} takes the
   * recorded fingerprint as the baseline it compares an entry's precondition against. One change behind its
   * peers', this node refuses the very next entry they accept, and keeps refusing - nothing it refuses can move
   * its baseline forward - so a single lost write splits the cluster's security state for good, with no race and
   * no second coincidence. With no marker the node answers "I cannot judge" and installs, which is what every
   * node does before the first replicated document of that kind lands, and it is back on its peers' baseline as
   * soon as that entry applies. The remaining exposure - the first entry after such a restart being itself a
   * superseded one - needs the race the stale marker did not, and is issue #7752, which closes it by carrying
   * the baseline in the replicated protocol rather than in a local file.
   * <p>
   * Removing the file discards all three document kinds, not only the one being recorded, because they share one
   * document and a rewrite is precisely what just failed. Unlinking is also the one operation still likely to
   * succeed on a volume that has no room left. It is conservative in the same direction, and self-closing: the
   * next security change that persists records every kind again.
   */
  private void discardStaleRecord(final File file, final Exception writeFailure) {
    // Cleared whether or not the unlink succeeds, so the next record() of an UNCHANGED fingerprint still
    // attempts the write instead of short-circuiting on a value only memory ever had.
    persisted.clear();

    final boolean removed;
    try {
      removed = Files.deleteIfExists(file.toPath());
    } catch (final IOException | RuntimeException e) {
      writeFailure.addSuppressed(e);
      LogManager.instance().log(this, Level.SEVERE,
          "Could not write '%s', and the record it already held could not be removed either. The replicated "
              + "security document IS installed on this node and IS in force; what failed is recording that it "
              + "came from the cluster. A RESTART of this node would therefore read a fingerprint OLDER than the "
              + "document beside it, and this node would then refuse the replicated security entries its peers "
              + "accept - and go on refusing them, because nothing it refuses moves that fingerprint forward. Fix "
              + "the configuration volume, delete '%s' and reissue the security change: %s",
          writeFailure, FILE_NAME, FILE_NAME, writeFailure.getMessage());
      return;
    }

    // Distinguished because the two states read the same to an operator otherwise: a write that failed with a
    // record already on disk leaves a file that had to be unlinked, while the first failed write on a fresh node
    // never had one, and reporting the second as a removal describes a file that never existed (claude-review on
    // PR #7817).
    final String markerState = removed ?
        String.format("'%s' has been REMOVED rather than left naming a document this node has moved past", FILE_NAME) :
        String.format("'%s' held no record to remove, so nothing on disk names a document this node has moved past",
            FILE_NAME);

    LogManager.instance().log(this, Level.SEVERE,
        "Could not write '%s'. The replicated security document IS installed on this node and IS in force; what "
            + "failed is recording that it came from the cluster, so %s. Until the next security change persists, "
            + "a RESTART of this node leaves it unable to judge an entry's compare-and-set precondition: it "
            + "installs the next replicated document of each kind unconditionally, which rejoins its peers' "
            + "baseline but costs that one entry's lost-update protection (issue #7752). Fix the configuration "
            + "volume and reissue the security change, which records every document again: %s",
        writeFailure, FILE_NAME, markerState, writeFailure.getMessage());
  }
}
