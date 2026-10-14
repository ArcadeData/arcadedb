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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9774: the query admission gate's definition of "a request that does work" and the running-query registry's
 * ({@code list queries}, {@code terminate query}, {@code SHOW TRANSACTIONS}) are the same set by design, but the two were
 * wired by hand at every protocol, and each protocol that took a slot without opening an entry had to be found and fixed
 * on its own: work an administrator could neither see nor stop, on a server refusing requests because of it. This scans
 * the main sources of every module and fails for a class that takes an admission slot and never opens a registry entry,
 * so the next protocol cannot miss it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AdmittedWorkIsListedTest {
  private static final Path    REACTOR_ROOT = Path.of("..");
  private static final Pattern ADMITS       = Pattern.compile("\\.admit\\(");
  private static final Pattern REGISTERS    = Pattern.compile("getRunningQueries\\(\\)|registerRunningQuery\\(");

  @Test
  void everyClassTakingAnAdmissionSlotOpensARunningQueryEntry() throws IOException {
    final Map<String, String> sources = readMainSources();
    assertThat(sources).as("the scan must reach the protocol modules").containsKey(
        "mcp/src/main/java/com/arcadedb/mcp/MCPDispatcher.java");
    assertThat(offenders(sources)).as("classes that take an admission slot but are not listed by 'list queries'").isEmpty();
  }

  @Test
  void theScanFlagsAClassThatAdmitsWithoutRegistering() {
    final Map<String, String> sources = new TreeMap<>();
    sources.put("a/Unlisted.java", """
        class Unlisted { void run() { try (QueryAdmissionGate.Ticket t = QueryAdmissionGate.getInstance().admit()) { work(); } } }""");
    sources.put("b/Listed.java", """
        class Listed { void run() { QueryAdmissionGate.getInstance().admit(); server.getRunningQueries().register(db, u, "x", null, null, c); } }""");
    sources.put("b/NoGate.java", "class NoGate { void run() { other.admit(); } }");

    assertThat(offenders(sources)).containsExactly("a/Unlisted.java");
  }

  static List<String> offenders(final Map<String, String> sources) {
    final List<String> offenders = new ArrayList<>();
    for (final Map.Entry<String, String> entry : sources.entrySet())
      if (entry.getValue().contains("QueryAdmissionGate") && ADMITS.matcher(entry.getValue()).find() && !REGISTERS.matcher(entry.getValue()).find())
        offenders.add(entry.getKey());
    return offenders;
  }

  private static Map<String, String> readMainSources() throws IOException {
    final Map<String, String> sources = new TreeMap<>();
    try (final Stream<Path> modules = Files.list(REACTOR_ROOT)) {
      for (final Path module : modules.filter(Files::isDirectory).toList()) {
        final String moduleName = module.getFileName().toString();
        if (moduleName.startsWith(".") || moduleName.equals("target"))
          continue;
        final Path mainSources = module.resolve("src").resolve("main").resolve("java");
        if (!Files.isDirectory(mainSources))
          continue;
        try (final Stream<Path> files = Files.walk(mainSources)) {
          for (final Path file : files.filter(p -> p.toString().endsWith(".java"))
              // the gate itself defines admit()
              .filter(p -> !p.getFileName().toString().equals("QueryAdmissionGate.java")).toList())
            sources.put(REACTOR_ROOT.relativize(file).toString().replace('\\', '/'), Files.readString(file, StandardCharsets.UTF_8));
        }
      }
    }
    return sources;
  }
}
