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
import java.util.logging.Level;

/**
 * Resolves the {@link InstanceId} of a server, in this order:
 * <ol>
 *   <li>the setting {@code arcadedb.instance.id}, when valid (a malformed value is ignored with a warning);</li>
 *   <li>the file {@value #FILE_NAME} in the configuration directory, when valid;</li>
 *   <li>a newly generated id, written atomically to that file.</li>
 * </ol>
 * A file with invalid content is renamed to {@value #INVALID_FILE_NAME} (never silently overwritten) and a new id is
 * generated. When the configuration directory is not writable the id is stable for this JVM only and the log says to use
 * the setting. This class never throws.
 * <p>
 * The file is node-local: it is not one of the documents replicated between HA nodes (server-users.jsonl,
 * server-groups.json, server-api-tokens.json) and no snapshot install touches the configuration directory. Copying a
 * configuration directory to create a new node duplicates the id: remove the file from the copy.
 */
public final class InstanceIdResolver {
  public static final String FILE_NAME         = "instance.id";
  public static final String INVALID_FILE_NAME = "instance.id.invalid";

  /** Id used when the configuration directory is not writable: generated once per JVM so restarts in-process agree. */
  private static volatile String jvmFallbackId;

  private InstanceIdResolver() {
  }

  public static String resolve(final ContextConfiguration configuration, final Path configDir) {
    final String configured = configuration.getValueAsString(GlobalConfiguration.INSTANCE_ID);
    if (configured != null && !configured.isBlank()) {
      final String normalized = InstanceId.normalize(configured);
      if (normalized != null)
        return normalized;
      LogManager.instance().log(InstanceIdResolver.class, Level.WARNING,
          "Ignoring invalid value for setting '%s': expected 'adb-' followed by a lowercase UUID (e.g. %s)",
          GlobalConfiguration.INSTANCE_ID.getKey(), "adb-123e4567-e89b-12d3-a456-426614174000");
    }

    final Path file = configDir.resolve(FILE_NAME);
    try {
      if (Files.exists(file)) {
        final String existing = InstanceId.normalize(Files.readString(file, StandardCharsets.UTF_8));
        if (existing != null)
          return existing;
        quarantineInvalidFile(file, configDir);
      }
    } catch (final IOException | RuntimeException e) {
      LogManager.instance().log(InstanceIdResolver.class, Level.WARNING, "Cannot read the instance id file '%s'", e, file);
      return fallback(file, e);
    }

    final String generated = InstanceId.generate();
    try {
      write(file, generated);
      return generated;
    } catch (final IOException | RuntimeException e) {
      return fallback(file, e);
    }
  }

  private static void quarantineInvalidFile(final Path file, final Path configDir) throws IOException {
    final Path invalid = configDir.resolve(INVALID_FILE_NAME);
    LogManager.instance().log(InstanceIdResolver.class, Level.WARNING,
        "The instance id file '%s' does not contain a valid instance id: it is kept as '%s' and a new id is generated",
        file, invalid);
    Files.move(file, invalid, StandardCopyOption.REPLACE_EXISTING);
  }

  private static void write(final Path file, final String id) throws IOException {
    final Path tmp = Files.createTempFile(file.getParent(), FILE_NAME, ".tmp");
    try {
      Files.writeString(tmp, id + System.lineSeparator(), StandardCharsets.UTF_8);
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

  private static String fallback(final Path file, final Exception cause) {
    if (jvmFallbackId == null) {
      synchronized (InstanceIdResolver.class) {
        if (jvmFallbackId == null)
          jvmFallbackId = InstanceId.generate();
      }
    }
    LogManager.instance().log(InstanceIdResolver.class, Level.WARNING,
        "Cannot persist the instance id in '%s' (%s): the configuration directory is not writable. The id %s is stable "
            + "for this JVM only and will change on restart. Set the setting '%s' to keep a stable id",
        file, cause.toString(), jvmFallbackId, GlobalConfiguration.INSTANCE_ID.getKey());
    return jvmFallbackId;
  }
}
