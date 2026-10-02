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
package com.arcadedb.server;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.InstanceId;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.security.SecurityUserFileRepository;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.logging.Level;

/**
 * Resolves the {@link InstanceId} of a server, in this order:
 * <ol>
 *   <li>the setting {@code arcadedb.instance.id}, when valid (a malformed value is ignored with a warning);</li>
 *   <li>when {@code arcadedb.instance.derived} is true, the id computed from the cluster and server names
 *   ({@link InstanceId#derive}), stable with no file;</li>
 *   <li>the file {@value #FILE_NAME} of the databases directory, then the legacy file {@value #LEGACY_FILE_NAME} of the
 *   configuration directory (read, and copied to the databases directory; the old file is left in place);</li>
 *   <li>a newly generated id, written atomically to the first writable of those two directories.</li>
 * </ol>
 * The file holds the id and, on a second line, {@code server=<name>} of the server that created it. When the name differs
 * from this server's the volume or directory was cloned: a new id is generated, the file is rewritten and the log says so
 * in a WARNING. A one-line file (older versions) is adopted and gets the name. A file with invalid content is renamed to
 * {@value #INVALID_FILE_NAME} (never silently overwritten) and a new id is generated. When no directory is writable the id
 * is stable for this JVM only and ONE warning says it will change on restart. This class never throws.
 * <p>
 * The file is node-local: it is not one of the documents replicated between HA nodes and no snapshot install touches
 * these locations. The databases directory is scanned for subdirectories only, so the hidden file is never a database.
 */
public final class InstanceIdResolver {
  public static final String FILE_NAME         = ".instance.id";
  public static final String INVALID_FILE_NAME = ".instance.id.invalid";
  public static final String LEGACY_FILE_NAME  = "instance.id";

  private static final String SERVER_PREFIX = "server=";

  /** Id used when no directory is writable: generated once per JVM so restarts in-process agree. */
  private static volatile String jvmFallbackId;

  private InstanceIdResolver() {
  }

  /** The id and the name of the server that wrote it (null for the one-line format), or null when not a valid id. */
  private record Stored(String id, String serverName) {
  }

  public static String resolve(final ContextConfiguration configuration, final Path configDir, final Path databaseDir,
      final String clusterName, final String serverName) {
    final String configured = configuration.getValueAsString(GlobalConfiguration.INSTANCE_ID);
    if (configured != null && !configured.isBlank()) {
      final String normalized = InstanceId.normalize(configured);
      if (normalized != null)
        return normalized;
      LogManager.instance().log(InstanceIdResolver.class, Level.WARNING,
          "Ignoring invalid value for setting '%s': expected 'adb-' followed by a lowercase UUID (e.g. %s)",
          GlobalConfiguration.INSTANCE_ID.getKey(), "adb-123e4567-e89b-12d3-a456-426614174000");
    }

    if (configuration.getValueAsBoolean(GlobalConfiguration.INSTANCE_DERIVED)) {
      final String derived = InstanceId.derive(clusterName, serverName);
      LogManager.instance().log(InstanceIdResolver.class, Level.INFO,
          "The instance id %s is derived from the cluster name '%s' and the server name '%s' (%s=true)", derived, clusterName,
          serverName, GlobalConfiguration.INSTANCE_DERIVED.getKey());
      // Every pod left on the default name would get the same id and be one server in the portal.
      if (GlobalConfiguration.SERVER_NAME.getDefValue().equals(serverName))
        LogManager.instance().log(InstanceIdResolver.class, Level.WARNING,
            "%s=true but the server name is still the default '%s': every server that keeps it gets the same instance id. Give each server its own name with %s (e.g. the pod name)",
            GlobalConfiguration.INSTANCE_DERIVED.getKey(), serverName, GlobalConfiguration.SERVER_NAME.getKey());
      return derived;
    }

    final String name = serverName == null ? "" : serverName;
    final List<Path> places = List.of(databaseDir.resolve(FILE_NAME), configDir.resolve(LEGACY_FILE_NAME));
    final Path primary = places.get(0);

    for (final Path file : places) {
      final Stored stored = read(file);
      if (stored == null)
        continue;
      if (stored.serverName() != null && !stored.serverName().equals(name)) {
        final String fresh = InstanceId.generate();
        LogManager.instance().log(InstanceIdResolver.class, Level.WARNING,
            "The instance id file '%s' was created by the server '%s' but this server is '%s': probably a cloned volume or a "
                + "copied directory. A new instance id %s was generated", file, stored.serverName(), name, fresh);
        return persist(places, fresh, name);
      }
      if (!file.equals(primary) || stored.serverName() == null)
        // legacy location, or a one-line file: bring it to the primary place with the server name (best effort)
        tryWrite(primary, stored.id(), name);
      return stored.id();
    }
    return persist(places, InstanceId.generate(), name);
  }

  /** Writes to the first writable place; when none is, the JVM-stable fallback id with one warning. */
  private static String persist(final List<Path> places, final String id, final String serverName) {
    IOException last = null;
    for (final Path place : places) {
      try {
        write(place, id, serverName);
        return id;
      } catch (final IOException | RuntimeException e) {
        last = e instanceof IOException io ? io : new IOException(e);
      }
    }
    return fallback(places.get(0), last, id);
  }

  private static void tryWrite(final Path file, final String id, final String serverName) {
    try {
      write(file, id, serverName);
    } catch (final IOException | RuntimeException e) {
      // the id is still valid where it was found
    }
  }

  private static Stored read(final Path file) {
    try {
      if (!Files.exists(file))
        return null;
      final List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
      final String id = lines.isEmpty() ? null : InstanceId.normalize(lines.get(0));
      if (id != null) {
        String name = null;
        for (final String line : lines.subList(1, lines.size()))
          if (line.startsWith(SERVER_PREFIX))
            name = line.substring(SERVER_PREFIX.length()).trim();
        return new Stored(id, name);
      }
      quarantineInvalidFile(file);
    } catch (final IOException | RuntimeException e) {
      LogManager.instance().log(InstanceIdResolver.class, Level.WARNING, "Cannot read the instance id file '%s'", e, file);
    }
    return null;
  }

  private static void quarantineInvalidFile(final Path file) throws IOException {
    final Path invalid = file.resolveSibling(file.getFileName().toString().equals(FILE_NAME) ? INVALID_FILE_NAME : LEGACY_FILE_NAME + ".invalid");
    LogManager.instance().log(InstanceIdResolver.class, Level.WARNING,
        "The instance id file '%s' does not contain a valid instance id: it is kept as '%s' and a new id is generated", file,
        invalid);
    Files.move(file, invalid, StandardCopyOption.REPLACE_EXISTING);
  }

  private static void write(final Path file, final String id, final String serverName) throws IOException {
    final Path tmp = Files.createTempFile(file.getParent(), FILE_NAME, ".tmp");
    try {
      Files.writeString(tmp, id + System.lineSeparator() + SERVER_PREFIX + serverName + System.lineSeparator(),
          StandardCharsets.UTF_8);
      SecurityUserFileRepository.applyOwnerOnlyPermissions(tmp);
      try {
        Files.move(tmp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
      } catch (final AtomicMoveNotSupportedException e) {
        Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING);
      }
    } finally {
      Files.deleteIfExists(tmp);
    }
  }

  private static String fallback(final Path file, final IOException cause, final String wanted) {
    if (jvmFallbackId == null) {
      synchronized (InstanceIdResolver.class) {
        if (jvmFallbackId == null)
          jvmFallbackId = wanted;
      }
    }
    LogManager.instance().log(InstanceIdResolver.class, Level.WARNING,
        "Cannot persist the instance id in '%s' or in the configuration directory (%s). The id %s is stable for this JVM only "
            + "and will not survive a restart. Make the databases directory writable, or set '%s' (or '%s=true')", file,
        cause == null ? "" : cause.toString(), jvmFallbackId, GlobalConfiguration.INSTANCE_ID.getKey(),
        GlobalConfiguration.INSTANCE_DERIVED.getKey());
    return jvmFallbackId;
  }
}
