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

import com.arcadedb.log.LogManager;
import com.arcadedb.server.security.SecurityGroupFileRepository;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.stream.Stream;

/**
 * Seeds a configuration directory moved off {@code <root>/config} with the default files the distribution ships there
 * (issue #10536). A Kubernetes PersistentVolumeClaim, unlike a Docker named volume, is never populated from the image,
 * so a server pointed at one would run without the shipped {@code server-groups.json} (and its {@code readonly} group)
 * and the Gremlin server files. Doing it here instead of in an init container needs no shell, so it works on the native
 * images too, and the defaults always come from the image the server runs.
 * <p>
 * Only the files in {@link #SEEDED_FILES} are copied: whatever else sits under {@code <root>/config} (users, API tokens,
 * a configuration file) belongs to a server that ran there, not to the defaults. And only a directory that holds no file
 * yet is seeded, the first time a server starts on it: from then on what is in it, or missing from it, is an operator's
 * choice or the server's own runtime write, which is what the volume is there to keep. A file deleted on purpose is not
 * brought back on the next start, and the defaults a later image adds or changes do not reach a directory in use.
 * <p>
 * The seed is all or nothing: the files are staged in a sub-directory and moved into place only once every copy has
 * succeeded, and a failure removes what it placed. A half-seeded directory would otherwise hold a file, and so never be
 * seeded again.
 * <p>
 * Each default is read from the image's {@code <root>/config} when it holds a copy, otherwise from the copy bundled in
 * the server jar under {@link #BUNDLED_DEFAULTS} (issue #10554), which the build takes from the same single source,
 * {@code package/src/main/config}. A volume mounted over {@code <root>/config} itself hides what the image shipped
 * there, so it is seeded from the bundled copies alone. A server with no {@code <root>/config} directory at all (an
 * embedded server) is never seeded.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
public final class ConfigDirectorySeeder {
  /**
   * The shipped defaults the server reads from its configuration directory. The logging files are not here: the JVM
   * loads them through {@code -Djava.util.logging.config.file}, never from the configuration directory.
   */
  public static final List<String> SEEDED_FILES = List.of(SecurityGroupFileRepository.FILE_NAME, "gremlin-server.yaml",
      "gremlin-server.groovy", "gremlin-server.properties");

  /** Classpath folder holding a copy of each of {@link #SEEDED_FILES}, copied there from the distribution at build time. */
  public static final String BUNDLED_DEFAULTS = "com/arcadedb/server/config/defaults/";

  private ConfigDirectorySeeder() {
  }

  /**
   * Opens the copy of a shipped default bundled in the server jar.
   *
   * @return the stream, or null when {@code file} is not one of the bundled defaults
   */
  public static InputStream openBundledDefault(final String file) {
    return ConfigDirectorySeeder.class.getClassLoader().getResourceAsStream(BUNDLED_DEFAULTS + file);
  }

  /**
   * Copies each of {@link #SEEDED_FILES} into {@code configDirectory} when that directory is missing or holds no regular
   * file (a fresh volume may hold a {@code lost+found} directory). Each file comes from {@code shippedDirectory} when it
   * holds a copy, otherwise from the bundled one; when both name the same directory it is empty, so every file comes
   * from the bundled copy. Nothing happens when {@code shippedDirectory} does not exist (an embedded server).
   *
   * @return the names of the files copied
   */
  public static List<String> seed(final Path shippedDirectory, final Path configDirectory) throws IOException {
    final List<String> seeded = new ArrayList<>();
    if (!Files.isDirectory(shippedDirectory))
      return seeded;

    final Path source = shippedDirectory.toRealPath();
    // Normalized first: on POSIX "missing/.." does not resolve, so the same directory spelled that way would not exist
    final Path target = configDirectory.toAbsolutePath().normalize();
    if (Files.exists(target) && holdsAFile(target))
      return seeded;

    Files.createDirectories(target);
    // A directory, so a staging area left by a crash does not count as a file and the next start seeds again
    final Path staging = Files.createTempDirectory(target, ".seeding-");
    try {
      final List<String> staged = new ArrayList<>(SEEDED_FILES.size());
      for (final String file : SEEDED_FILES)
        if (stage(source.resolve(file), file, staging.resolve(file)))
          staged.add(file);

      for (final String file : staged)
        try {
          // No REPLACE_EXISTING: a file that appears concurrently is kept as it is
          Files.move(staging.resolve(file), target.resolve(file));
          seeded.add(file);
        } catch (final FileAlreadyExistsException ignored) {
          // Written concurrently by someone else: theirs is kept
        }
    } catch (final IOException | RuntimeException e) {
      for (final String file : seeded)
        Files.deleteIfExists(target.resolve(file));
      seeded.clear();
      throw e;
    } finally {
      deleteStaging(staging);
    }
    return seeded;
  }

  /**
   * Copies the shipped file, or the bundled copy when none was shipped, to {@code staged}.
   *
   * @return false when neither exists
   */
  private static boolean stage(final Path shipped, final String file, final Path staged) throws IOException {
    if (Files.isRegularFile(shipped)) {
      Files.copy(shipped, staged);
      return true;
    }
    try (final InputStream bundled = openBundledDefault(file)) {
      if (bundled == null)
        return false;
      Files.copy(bundled, staged);
      return true;
    }
  }

  /** Best effort: a leftover staging directory is not a file, so it never stops a later seed. */
  private static void deleteStaging(final Path staging) {
    try {
      try (final Stream<Path> entries = Files.list(staging)) {
        final Iterator<Path> it = entries.iterator();
        while (it.hasNext())
          Files.deleteIfExists(it.next());
      }
      Files.deleteIfExists(staging);
    } catch (final IOException e) {
      LogManager.instance().log(ConfigDirectorySeeder.class, Level.FINE, "Cannot remove the staging directory '%s'", e, staging);
    }
  }

  private static boolean holdsAFile(final Path directory) throws IOException {
    if (!Files.isDirectory(directory))
      return true;
    try (final Stream<Path> entries = Files.list(directory)) {
      final Iterator<Path> it = entries.iterator();
      while (it.hasNext())
        if (Files.isRegularFile(it.next()))
          return true;
    }
    return false;
  }

  /**
   * Seeds the configuration directory of a server from {@code <root>/config} and the bundled defaults. A failure is logged, not thrown: the server
   * still starts on its built-in defaults, as it did before seeding existed.
   */
  static void seedFromRoot(final String rootPath, final String configPath) {
    final Path shipped = Path.of(rootPath, "config");
    final Path target = Path.of(configPath);
    try {
      final List<String> seeded = seed(shipped, target);
      if (!seeded.isEmpty())
        LogManager.instance()
            .log(ConfigDirectorySeeder.class, Level.INFO, "Seeded the configuration directory '%s' with the defaults %s from '%s' and the server's bundled copies",
                target, seeded, shipped);
    } catch (final IOException | RuntimeException e) {
      LogManager.instance()
          .log(ConfigDirectorySeeder.class, Level.WARNING,
              "Cannot seed the configuration directory '%s' with the defaults from '%s', the server uses its built-in defaults", e,
              target, shipped);
    }
  }
}
