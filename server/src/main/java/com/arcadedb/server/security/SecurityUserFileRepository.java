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
import com.arcadedb.security.SecurityManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONException;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.FileUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;

public class SecurityUserFileRepository {
  public static final  String FILE_NAME        = "server-users.jsonl";
  /** Where a {@link #FILE_NAME} that cannot be loaded is preserved before the default configuration replaces it. */
  public static final  String ERROR_FILE_NAME  = "server-users-error.jsonl";
  private static final int    BUFFER_SIZE      = 65536 * 10;
  private static final String DEFAULT_FALLBACK = "using the default configuration";
  private final        String securityConfPath;
  // Serializes all writers (local mutations, config reload, Raft apply) so their output never interleaves.
  private final        Object saveLock         = new Object();
  private volatile     long   fileLastModified = -1;
  // Whether the users in force are NEWER than the file (issue #10451): a replicated list is published in memory even when
  // its write fails (issue #7137). Set by markDiskBehind() from the Raft apply thread, which must not take the
  // ServerSecurity monitor, cleared by any successful save() - every save writes the users that are, or are about to be,
  // in force. A save that FAILS leaves it as it was: the local path publishes nothing when its write fails, so it cannot
  // put the disk behind memory, and it cannot bring a disk that already was behind level either. VOLATILE because the
  // watcher's reload reads it under ServerSecurity's users-policy lock, not under saveLock. Set AFTER the failed write,
  // never before: a local save succeeding in between would clear it and the failure that follows would leave it clear.
  // In memory only: after a restart the older file is loaded, and the replicated list comes back from the Raft log,
  // whose entry the #7752 durability floor keeps out of every snapshot checkpoint until the list is on disk.
  private volatile     boolean diskBehind;

  public SecurityUserFileRepository(String securityConfPath) {
    // EITHER SEPARATOR CONVENTION (ISSUE #7588)
    this.securityConfPath = FileUtils.appendSeparatorIfMissing(securityConfPath);
  }

  public void save(final List<JSONObject> configuration) throws IOException {
    final File file = new File(securityConfPath, FILE_NAME);

    final StringBuilder sb = new StringBuilder();
    for (final JSONObject line : configuration)
      sb.append(line.toString()).append("\n");
    final byte[] bytes = sb.toString().getBytes(DatabaseFactory.getDefaultCharset());

    synchronized (saveLock) {
      final File dir = file.getParentFile();
      if (dir != null && !dir.exists())
        dir.mkdirs();

      final Path target = file.toPath();
      // Write to a sibling temp file, fsync it, then atomically rename over the target. A crash or power
      // loss mid-write can only ever damage the throwaway temp file: the live users file remains the
      // previous complete version, so restart never falls back to createDefault() and resets all users.
      final Path tmp = Files.createTempFile(target.getParent(), FILE_NAME, ".tmp");
      try {
        try (final FileChannel channel = FileChannel.open(tmp, StandardOpenOption.WRITE)) {
          // Looped, because FileChannel.write() is only obliged to consume SOME of what remains: a short write
          // here would fsync and publish a truncated server-users.jsonl, which load() cannot parse and falls back
          // to createDefault() - a single 'root' user and nobody else, silently (issue #7825).
          FileUtils.writeFully(channel, ByteBuffer.wrap(bytes));
          channel.force(true);
        }

        // Restrict to owner-only (mode 600) before publishing, matching ApiTokenConfiguration, so the
        // password-hash file is never world-readable.
        applyOwnerOnlyPermissions(tmp);

        try {
          Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (final AtomicMoveNotSupportedException e) {
          Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
        }
      } finally {
        Files.deleteIfExists(tmp);
      }

      fileLastModified = file.lastModified();
      diskBehind = false;
    }
  }

  /**
   * Records that the users in force did not reach {@link #FILE_NAME}: the replicated install of issue #7137 published a
   * list whose write failed. Until the next successful {@link #save}, the file holds an OLDER list and a reload must not
   * read it (issue #10451).
   */
  void markDiskBehind() {
    diskBehind = true;
  }

  /** Whether the users in force have not reached {@link #FILE_NAME} yet. See {@link #markDiskBehind()}. */
  boolean isDiskBehind() {
    return diskBehind;
  }

  /**
   * Takes the file's current stamp as seen without reading it, so {@link #isUserFileChanged()} stops reporting a change
   * the caller has decided not to load: the watcher's refusal of issue #10451 would otherwise log on every tick.
   */
  void markFileSeen() {
    synchronized (saveLock) {
      fileLastModified = new File(securityConfPath, FILE_NAME).lastModified();
    }
  }

  /**
   * Package-private rather than private since issue #7693: {@link ReplicatedSecurityFingerprintRepository} writes
   * a fourth file into the same configuration directory and has to give it the same permissions, and a second
   * copy of this would be a second place for the convention to drift (code review on PR #7748).
   */
  public static void applyOwnerOnlyPermissions(final Path path) {
    try {
      final PosixFileAttributeView posixView = Files.getFileAttributeView(path, PosixFileAttributeView.class);
      if (posixView != null)
        posixView.setPermissions(Set.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE));
    } catch (final IOException | UnsupportedOperationException e) {
      // Non-POSIX system (e.g. Windows): skip
    }
  }

  /**
   * Reports that {@link #FILE_NAME} could not be turned into users and preserves it verbatim as
   * {@link #ERROR_FILE_NAME} (issue #10321). Must run BEFORE the caller falls back to {@link #createDefault()}:
   * the root user created from the configured password is saved over {@link #FILE_NAME}, and with it the only copy
   * of the operator's file.
   */
  public void saveInError(final Exception cause) {
    saveInError(cause, DEFAULT_FALLBACK);
  }

  /**
   * As {@link #saveInError(Exception)}, with {@code outcome} saying in the SEVERE what the caller does instead of loading
   * the file: a reload on a running server keeps the users already in force rather than using the default configuration
   * (issue #10326).
   */
  public void saveInError(final Exception cause, final String outcome) {
    synchronized (saveLock) {
      preserveInError(this, new File(securityConfPath, FILE_NAME), new File(securityConfPath, ERROR_FILE_NAME), cause, outcome);
    }
  }

  /**
   * Shared by the users and groups repositories: logs ONE SEVERE naming the file that failed and copies it, byte for
   * byte, to {@code errorFile}. The copy is written to a temp file (created owner-only by the JDK) and renamed over the
   * target, so the users file's password hashes are never exposed with a wider mode, not even for an instant (the
   * groups file carries none, and gets the same owner-only mode as a harmless side effect of sharing the code).
   * <p>
   * An existing {@code errorFile} is REPLACED: it always holds the most recent file that failed to load. Keeping every
   * failure would need a rotation policy for a directory operators hand-edit; the latest bad file is the one the SEVERE
   * just reported.
   * <p>
   * Not synchronized itself: each caller holds the lock that serializes its own writers ({@code saveLock} here, the
   * repository monitor in {@link SecurityGroupFileRepository#saveInError}).
   */
  static void preserveInError(final Object requester, final File file, final File errorFile, final Exception cause) {
    preserveInError(requester, file, errorFile, cause, DEFAULT_FALLBACK);
  }

  static void preserveInError(final Object requester, final File file, final File errorFile, final Exception cause,
      final String outcome) {
    if (!file.isFile()) {
      LogManager.instance().log(requester, Level.SEVERE,
          "Error on loading file '%s', %s. The file is no longer there, so nothing was saved as '%s'", cause, file.getName(),
          outcome, errorFile.getPath());
      return;
    }

    final Path target = errorFile.toPath();
    Path tmp = null;
    try {
      tmp = Files.createTempFile(target.getParent(), errorFile.getName(), ".tmp");
      // Streamed channel to channel, then fsynced before the rename, as save() does: once the default users replace
      // FILE_NAME this copy is the only one left, and a crash after an unsynced rename could leave it empty.
      try (final FileChannel source = FileChannel.open(file.toPath(), StandardOpenOption.READ);
          final FileChannel copy = FileChannel.open(tmp, StandardOpenOption.WRITE)) {
        final long size = source.size();
        long position = 0;
        while (position < size) {
          final long transferred = copy.transferFrom(source, position, size - position);
          if (transferred <= 0)
            break;
          position += transferred;
        }
        // A short copy renamed into place would be logged as "saved" while missing the tail of the operator's file.
        if (position != size)
          throw new IOException("Copied only " + position + " of " + size + " bytes");
        copy.force(true);
      }
      applyOwnerOnlyPermissions(tmp);
      try {
        Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      } catch (final AtomicMoveNotSupportedException e) {
        Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
      }
      LogManager.instance().log(requester, Level.SEVERE,
          "Error on loading file '%s', %s and saving the corrupt file as '%s'", cause, file.getName(), outcome,
          errorFile.getPath());
    } catch (final IOException | RuntimeException e) {
      LogManager.instance().log(requester, Level.SEVERE,
          "Error on loading file '%s', %s. The corrupt file could not be saved as '%s' (error=%s)", cause, file.getName(),
          outcome, errorFile.getPath(), e.toString());
    } finally {
      if (tmp != null)
        try {
          Files.deleteIfExists(tmp);
        } catch (final IOException ignore) {
          // BEST EFFORT
        }
    }
  }

  public List<JSONObject> getUsers() {
    try {
      return load();
    } catch (final IOException e) {
      LogManager.instance().log(this, Level.SEVERE, "Error on loading file '%s', using default configuration", e, FILE_NAME);
      return createDefault();
    }
  }

  public boolean isUserFileChanged() {
    final File file = new File(securityConfPath, FILE_NAME);
    return file.lastModified() > fileLastModified;
  }

  protected List<JSONObject> load() throws IOException {
    final List<JSONObject> resultSet = loadRecords();
    if (!resultSet.isEmpty())
      return resultSet;
    return createDefault();
  }

  /**
   * The records in {@link #FILE_NAME} as they are, WITHOUT the {@link #createDefault()} substitution {@link #load()} makes
   * for a missing or empty file: an empty list then. A reload on a running server (issue #10326) has users in force to
   * keep, so an empty file - typically an editor caught between truncating and writing it - must not turn into a
   * root-only configuration there.
   *
   * @throws IOException   if the file cannot be read
   * @throws JSONException if a line is not a JSON object
   */
  List<JSONObject> loadRecords() throws IOException {
    final File file = new File(securityConfPath, FILE_NAME);

    final List<JSONObject> resultSet = new ArrayList<>();
    if (file.exists()) {
      fileLastModified = file.lastModified();

      try (final InputStreamReader is = new InputStreamReader(new FileInputStream(file));//
          final BufferedReader reader = new BufferedReader(is, BUFFER_SIZE)) {
        while (reader.ready())
          resultSet.add(new JSONObject(reader.readLine()));
      }
    }
    return resultSet;
  }

  public static List<JSONObject> createDefault() {
    // ROOT USER
    return List.of(new JSONObject().put("name", "root")
        .put("databases", new JSONObject().put(SecurityManager.ANY, new JSONArray(new String[] { "admin" }))));
  }

  public long getFileLastModified() {
    return fileLastModified;
  }
}
