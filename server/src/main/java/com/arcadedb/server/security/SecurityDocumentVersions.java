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
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.logging.Level;

/**
 * The compare-and-set counter of each replicated security document (issue #7509).
 * <p>
 * All three security documents are replicated as the WHOLE document: the submitter reads the current one,
 * mutates a copy and submits the result. Nothing serialised that sequence ACROSS nodes - the
 * {@code synchronized} block in {@code ServerSecurity.createUserClusterWide} and its siblings is a per-node
 * monitor - so two operators changing two different users on two nodes in the same second produced two entries
 * each built without the other in it. Raft linearises them, the second one wins, and the first change is
 * reverted on every node including the one that answered 200.
 * <p>
 * Each submitted entry now carries the version the submitter read plus the version it produces, and the apply
 * installs it only when the node's current version still matches. The counter is therefore what makes a
 * concurrent change detectable at all.
 * <p>
 * <b>Why a counter and not a hash of the document.</b> The check has to reach the SAME verdict on every node,
 * or a committed entry is installed on some nodes and refused on others - which is divergence, the failure
 * #6808 and #7373 exist to prevent, and strictly worse than the lost update being fixed here. A digest of the
 * local document does not have that property: nodes can legitimately hold different documents (each node
 * creates its own {@code root} with its own salt before the first seed, a group file can be hand-edited, the
 * {@code SecurityGroupFileRepository} watcher re-reads it on its own interval). A counter that is only ever
 * advanced by an apply does: every node that has applied the same prefix of the Raft log holds the same value.
 * <p>
 * <b>Why it is persisted.</b> An in-memory-only counter resets on restart, and a restarted node would then
 * disagree with its peers about the very next entry - accepting what they refuse, or refusing what they
 * accept. The file is written on every apply, and its absence means 0 on every node, which is what a cluster
 * upgrading into this fix converges on without any migration step.
 * <p>
 * Writes are atomic (temp file, fsync, rename) and owner-only, the same shape
 * {@link SecurityUserFileRepository#save} uses, so a crash mid-write can only damage the throwaway temp file.
 */
public class SecurityDocumentVersions {
  public static final String FILE_NAME = "server-security-versions.json";

  /**
   * {@code expectedVersion} of an entry that must be applied whatever the node's current version is: what peer
   * seeding submits, because a joining peer holds nothing to compare against and a refused seed would leave it
   * running on its own stale documents. Also what an entry from a peer that predates this fix decodes to.
   */
  public static final long UNCONDITIONAL = -1L;

  /**
   * {@code newVersion} of an entry that must leave the counter alone: an entry produced by a node that predates
   * this fix, which carries no version at all. Deliberately the same value as {@link #UNCONDITIONAL} and
   * deliberately a separate name - the two say different things about different fields, and a reader who has
   * to work out which meaning applies from the value alone is one refactor away from swapping them.
   */
  public static final long NO_VERSION = -1L;

  /** The replicated security documents, each with its own independent counter. */
  public enum Document {
    USERS("users", SecurityUserFileRepository.FILE_NAME),
    GROUPS("groups", SecurityGroupFileRepository.FILE_NAME),
    API_TOKENS("apiTokens", ApiTokenConfiguration.FILE_NAME);

    private final String key;
    private final String documentFileName;

    Document(final String key, final String documentFileName) {
      this.key = key;
      this.documentFileName = documentFileName;
    }

    /** The JSON key this document's counter is stored under. */
    public String getKey() {
      return key;
    }

    /** The file this counter tracks, for messages an operator has to act on. */
    public String getDocumentFileName() {
      return documentFileName;
    }
  }

  private static final Document[]     DOCUMENTS = Document.values();
  private final        String         filePath;
  private final        AtomicLongArray versions = new AtomicLongArray(DOCUMENTS.length);
  // Serialises writers exactly as SecurityUserFileRepository does, so two saves never interleave.
  private final        Object         saveLock  = new Object();

  public SecurityDocumentVersions(final String configPath) {
    this.filePath = new File(configPath, FILE_NAME).getPath();
  }

  /**
   * Reads the persisted counters, leaving every one of them at 0 when the file is absent or unreadable.
   * <p>
   * 0 rather than "unknown" on purpose: every node of a cluster upgrading into this fix starts from the same
   * value, so the first conditional entry is agreed on by all of them. An "unknown" state would have to be
   * resolved by accepting unconditionally, which is the lost update this class exists to stop.
   */
  public void load() {
    final File file = new File(filePath);
    if (!file.exists()) {
      for (int i = 0; i < DOCUMENTS.length; i++)
        versions.set(i, 0L);
      return;
    }

    try {
      final JSONObject json = new JSONObject(Files.readString(file.toPath(), DatabaseFactory.getDefaultCharset()));
      for (int i = 0; i < DOCUMENTS.length; i++)
        versions.set(i, Math.max(0L, json.getLong(DOCUMENTS[i].getKey(), 0L)));
    } catch (final Exception e) {
      // An unreadable counter file is reported and read as 0, not as a reason to refuse to start: the
      // documents themselves are still loadable and the node is still able to authenticate. What it costs is
      // that this node can disagree with its peers about the next security entry until one is applied.
      LogManager.instance().log(this, Level.WARNING,
          "Could not read the replicated security document versions from '%s'; starting from 0. Until the next "
              + "security change is applied this node may refuse one its peers accept", e, FILE_NAME);
      for (int i = 0; i < DOCUMENTS.length; i++)
        versions.set(i, 0L);
    }
  }

  /** The version of {@code document} this node currently holds. */
  public long get(final Document document) {
    return versions.get(document.ordinal());
  }

  /**
   * Whether an entry built from {@code expectedVersion} may be applied to {@code document} on this node.
   * {@link #UNCONDITIONAL} always may.
   */
  public boolean accepts(final Document document, final long expectedVersion) {
    return expectedVersion == UNCONDITIONAL || expectedVersion == get(document);
  }

  /**
   * Records that {@code document} is now at {@code newVersion} and persists every counter.
   * <p>
   * Returns the persistence failure instead of throwing it, the shape
   * {@code ServerSecurity.applyReplicatedUsers} already uses for the document itself (issue #7137): the
   * document is in force in memory by the time this is called, so the counter has to follow it in memory
   * whatever the disk does. {@link #NO_VERSION} leaves the counter alone and writes nothing.
   *
   * @return the failure, or null when the counters reached disk
   */
  public Exception record(final Document document, final long newVersion) {
    if (newVersion == NO_VERSION)
      return null;

    versions.set(document.ordinal(), newVersion);
    return trySave();
  }

  private Exception trySave() {
    final JSONObject json = new JSONObject();
    for (int i = 0; i < DOCUMENTS.length; i++)
      json.put(DOCUMENTS[i].getKey(), versions.get(i));

    try {
      save(json);
      return null;
    } catch (final Exception e) {
      return e;
    }
  }

  private void save(final JSONObject json) throws IOException {
    final byte[] bytes = json.toString().getBytes(DatabaseFactory.getDefaultCharset());

    synchronized (saveLock) {
      final Path target = new File(filePath).toPath();
      final File dir = target.getParent() != null ? target.getParent().toFile() : null;
      if (dir != null && !dir.exists())
        dir.mkdirs();

      final Path tmp = Files.createTempFile(target.getParent(), FILE_NAME, ".tmp");
      try {
        try (final FileChannel channel = FileChannel.open(tmp, StandardOpenOption.WRITE)) {
          channel.write(ByteBuffer.wrap(bytes));
          channel.force(true);
        }
        applyOwnerOnlyPermissions(tmp);
        try {
          Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (final AtomicMoveNotSupportedException e) {
          Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
        }
      } finally {
        Files.deleteIfExists(tmp);
      }
    }
  }

  private static void applyOwnerOnlyPermissions(final Path path) {
    try {
      final PosixFileAttributeView posixView = Files.getFileAttributeView(path, PosixFileAttributeView.class);
      if (posixView != null)
        posixView.setPermissions(Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));
    } catch (final IOException | UnsupportedOperationException e) {
      // Non-POSIX system (e.g. Windows): skip
    }
  }
}
