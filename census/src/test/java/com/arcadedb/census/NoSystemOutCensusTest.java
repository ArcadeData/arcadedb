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
package com.arcadedb.census;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Production code does not print to {@code System.out} or {@code System.err} outside the classes whose purpose is a terminal or
 * stdio. A print elsewhere is a debug line left behind (CLAUDE.md): it bypasses the log level, the log file and the server's
 * log handlers, and on an embedded database it writes into the application's own console.
 * <p>
 * The census ranges over the authority: every read of the two static fields ({@code GETSTATIC java/lang/System.out|err}) in the
 * compiled classes of every module ({@link CompiledClasses}). A read is the print whatever is called on the stream next, so a
 * stream handed to a helper is seen too, and a mention in a comment or a literal is not a read.
 */
class NoSystemOutCensusTest {
  private static final String SYSTEM = "java/lang/System";

  /** Whole classes whose purpose is a terminal or stdio, keyed as the class part of a call-site key. */
  private static final Map<String, String> CONSOLE_CLASSES = new TreeMap<>(Map.ofEntries(//
      Map.entry("bench/BenchMain", "the benchmark harness CLI: usage, errors and results go to the terminal"),
      Map.entry("bench/BenchMain$Guard", "the benchmark harness CLI: the stall watchdog's thread dumps go to the terminal"),
      Map.entry("bench/BenchMain$RunFile", "the benchmark harness CLI: per-case progress and dump notices go to the terminal"),
      Map.entry("bench/gate/BenchChanges", "the benchmark gate CLI: unreadable inputs are reported on the terminal"),
      Map.entry("bench/gate/RawData", "the benchmark gate CLI: unreadable inputs are reported on the terminal"),
      Map.entry("bench/runner/BenchRunner", "the benchmark harness: the stall watchdog reports to the terminal"),
      Map.entry("console/Console", "the interactive console"),
      Map.entry("log/DefaultLogger", "the logging layer's own console sink and its last-resort errors"),
      Map.entry("log/LogManager", "the logging layer's own console sink and its last-resort errors"),
      Map.entry("log/Slf4jLogger", "the logging layer's own console sink and its last-resort errors"),
      Map.entry("server/DefaultConsoleReader", "the interactive password prompt"),
      Map.entry("server/DefaultConsoleReader$EraserThread", "masks the password typed at the interactive prompt"),
      Map.entry("mcp/MCPStdioServer", "stdio IS the transport"),
      Map.entry("query/sql/parser/ConsoleStatement", "the SQL CONSOLE statement prints by contract"),
      Map.entry("integration/importer/ConsoleLogger", "the importer's console sink"),
      Map.entry("index/lsm/LSMTreeIndexDebugger", "a developer's index dump to the terminal")));

  /** The remaining methods that print, each interactive, a CLI, or output before logging is up, by design. */
  private static final Map<String, String> REVIEWED = new TreeMap<>(Map.ofEntries(//
      Map.entry("server/security/ServerSecurity#askForRootPassword", "the first-run root password prompt, interactive by design"),
      Map.entry("GlobalConfiguration#call",
          "the anonymous setting callbacks: the dumpMetricsEvery dump to the terminal, and the maxPageRAM warning when "
              + "LogManager is not up yet"),
      Map.entry("GlobalConfiguration#dumpConfigurationToLog", "falls back to the terminal when LogManager is not up yet"),
      Map.entry("GlobalConfiguration$MetricsDumpTimer#run", "the periodic Profiler dump the dumpMetricsEvery setting asks for"),
      Map.entry("integration/importer/Neo4jImporter#log", "CLI importer output"),
      Map.entry("integration/importer/Neo4jImporter#error", "CLI importer output"),
      Map.entry("integration/importer/graph/GraphImporter#main", "usage and summary of a CLI tool"),
      Map.entry("index/lsm/LSMTreeIndexCompactor#compactInternal",
          "the debug flag's before/after dump, printed next to LSMTreeIndexDebugger's")));

  @Test
  void noProductionClassPrintsToTheConsole() throws Exception {
    final Map<String, Set<String>> reads = readsOf(CompiledClasses.ofEveryModule());

    assertThat(reads).as("the census must reach the console it exempts, console/Console").containsKey("console/Console#execute");

    final Set<String> classesSeen = new TreeSet<>();
    final Map<String, Set<String>> outsideTheConsole = new TreeMap<>();
    for (final Map.Entry<String, Set<String>> site : reads.entrySet()) {
      final String classKey = site.getKey().substring(0, site.getKey().indexOf('#'));
      classesSeen.add(classKey);
      if (!CONSOLE_CLASSES.containsKey(classKey))
        outsideTheConsole.put(site.getKey(), site.getValue());
    }

    assertThat(CompiledClasses.unreviewed(outsideTheConsole, REVIEWED)).as(
            "production code that prints to System.out/System.err: a debug print left behind - delete it or route it through "
                + "LogManager.instance().log(this, Level.<level>, ...)")
        .isEmpty();
    assertThat(CompiledClasses.stale(outsideTheConsole, REVIEWED)).as("REVIEWED entries that no longer print: remove them").isEmpty();

    final List<String> staleConsoles = new ArrayList<>();
    for (final String console : CONSOLE_CLASSES.keySet())
      if (!classesSeen.contains(console))
        staleConsoles.add(console);
    assertThat(staleConsoles).as("CONSOLE_CLASSES entries that no longer read System.out/System.err: remove them").isEmpty();
  }

  /** The control: the census sees a print on either stream and a stream handed on, not a literal nor a logger call. */
  @Test
  void theCensusSeesEveryShapeOfAPrint() throws Exception {
    final Map<String, Set<String>> sites = readsOf(CompiledClasses.ofTheProbes());

    assertThat(sites).containsKeys("census/ShapeProbes$ConsolePrint#plain", "census/ShapeProbes$ConsolePrint#errStream",
        "census/ShapeProbes$ConsolePrint#passedAsAStream");
    assertThat(sites).doesNotContainKeys("census/ShapeProbes$ConsolePrint#namedInALiteral",
        "census/ShapeProbes$ConsolePrint#throughTheLogger");
  }

  private static Map<String, Set<String>> readsOf(final Map<String, byte[]> classes) {
    return CompiledClasses.staticFieldReadsOf(classes,
        (owner, name) -> SYSTEM.equals(owner) && ("out".equals(name) || "err".equals(name)));
  }
}
