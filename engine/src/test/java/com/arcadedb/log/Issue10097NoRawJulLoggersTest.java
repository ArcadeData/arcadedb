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
package com.arcadedb.log;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10097: {@link LogManager#isLoggable} is the one authority on whether a record is emitted, because it asks the
 * installed {@link Logger} - JUL by default, SLF4J with {@code arcadedb.log.impl=slf4j}, a capturing one in a test. A
 * level guard or a record written straight against {@code java.util.logging.Logger.getLogger(...)} answers from JUL
 * whatever is installed: in an SLF4J deployment {@code ServerMonitor}'s records (its low-heap WARNING among them) never
 * reached the configured backend and {@code FileManager}'s two FINE diagnostics were tested against a JUL logger no
 * ArcadeDB configuration had touched.
 * <p>
 * This is the static assertion over the whole family, in the shape of {@link LogMessageArityTest}: the production sources
 * of every module are scanned, so a third conversion is not needed. The only legitimate users of a raw JUL logger are the
 * {@code com.arcadedb.log} package itself and the code that tames Apache Ratis's OWN JUL loggers, which are not ArcadeDB
 * records.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10097NoRawJulLoggersTest {
  /** A call to {@code Logger.getLogger(...)} whatever the qualification: JUL's, or one imported under the same name. */
  private static final Pattern RAW_JUL_LOGGER = Pattern.compile("\\bLogger\\s*\\.\\s*getLogger\\s*\\(");

  /**
   * Files that configure Apache Ratis's own JUL loggers: raising a third-party logger's level, not emitting an ArcadeDB
   * record, so there is no installed logger to route through.
   */
  private static final Set<String> RATIS_TAMING = Set.of( //
      "ha-raft/src/main/java/com/arcadedb/server/ha/raft/RaftHAServer.java",
      "ha-raft/src/main/java/com/arcadedb/server/ha/raft/ratis/RatisLogRecordFilter.java");

  private static final String LOG_PACKAGE = "engine/src/main/java/com/arcadedb/log/";

  @Test
  void noProductionCodeAsksJulDirectlyWhetherARecordIsEmitted() throws IOException {
    final Path repoRoot = Path.of("").toAbsolutePath().getParent();
    assertThat(repoRoot).isNotNull();

    final List<String> offenders = new ArrayList<>();
    int scanned = 0;
    try (final Stream<Path> modules = Files.list(repoRoot)) {
      for (final Path module : modules.sorted().toList()) {
        final Path sources = module.resolve("src").resolve("main").resolve("java");
        if (!Files.isDirectory(sources))
          continue;

        try (final Stream<Path> files = Files.walk(sources)) {
          for (final Path file : files.filter(p -> p.toString().endsWith(".java")).sorted().toList()) {
            scanned++;
            final String relative = repoRoot.relativize(file).toString().replace('\\', '/');
            if (relative.startsWith(LOG_PACKAGE) || RATIS_TAMING.contains(relative))
              continue;

            final String source = read(file);
            if (RAW_JUL_LOGGER.matcher(source).find())
              offenders.add(relative);
          }
        }
      }
    }

    assertThat(scanned).as("no production source found under " + repoRoot).isGreaterThan(1000);
    assertThat(offenders)
        .as("production classes that build a java.util.logging logger of their own instead of going through "
            + "LogManager.instance().log(...) / isLoggable(...): their records and guards ignore the installed logger "
            + "(arcadedb.log.impl=slf4j, a capturing logger), issue #10097")
        .isEmpty();
  }

  private static String read(final Path file) {
    try {
      return Files.readString(file, StandardCharsets.UTF_8);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
