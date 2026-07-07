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
import com.arcadedb.serializer.json.JSONObject;

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
  private static final int    BUFFER_SIZE      = 65536 * 10;
  private final        String securityConfPath;
  // Serializes all writers (local mutations, config reload, Raft apply) so their output never interleaves.
  private final        Object saveLock         = new Object();
  private volatile     long   fileLastModified = -1;

  public SecurityUserFileRepository(String securityConfPath) {
    if (!securityConfPath.endsWith(File.separator))
      securityConfPath += File.separator;
    this.securityConfPath = securityConfPath;
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
          channel.write(ByteBuffer.wrap(bytes));
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

    if (!resultSet.isEmpty())
      return resultSet;
    return createDefault();
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
