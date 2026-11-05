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

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10097: {@code LogManager#isLoggable} is the one authority on whether a record is emitted, because it asks the installed
 * {@code Logger} - JUL by default, SLF4J with {@code arcadedb.log.impl=slf4j}, a capturing one in a test. A level guard or a
 * record written straight against a {@code java.util.logging.Logger} of its own answers from JUL whatever is installed: in an
 * SLF4J deployment {@code ServerMonitor}'s records (its low-heap WARNING among them) never reached the configured backend and
 * {@code FileManager}'s two FINE diagnostics were tested against a JUL logger no ArcadeDB configuration had touched.
 * <p>
 * The census ranges over the authority (#11288): every call of a static factory of {@code java.util.logging.Logger}
 * ({@code getLogger}, {@code getGlobal}, {@code getAnonymousLogger}), or method reference to one, in the compiled classes of every
 * module ({@link CompiledClasses}). The source-text census it replaces matched {@code Logger.getLogger(} in the text, so a static
 * import, a method reference or the two other factories passed it, and a comment naming the call failed it.
 * <p>
 * The only legitimate users of a raw JUL logger are the {@code com.arcadedb.log} package itself, which IS the installed logger,
 * and the code that tames Apache Ratis's OWN JUL loggers, which are not ArcadeDB records.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class NoRawJulLoggersCensusTest {
  private static final String      AUTHORITY = "java/util/logging/Logger";
  private static final Set<String> FACTORY   = Set.of("getLogger", "getGlobal", "getAnonymousLogger");
  /** The installed logger itself, keyed as {@link CompiledClasses} keys the {@code com.arcadedb.log} package. */
  private static final String      LOG_PACKAGE = "log/";

  private static final String RATIS_TAMING = "configures Apache Ratis's own JUL loggers: raises a third-party logger's level, "
      + "emits no ArcadeDB record";

  private static final Map<String, String> REVIEWED = new TreeMap<>(Map.ofEntries(//
      Map.entry("server/ha/raft/RaftHAServer#start", RATIS_TAMING),
      Map.entry("server/ha/raft/RaftHAServer#stop", "silences Apache Ratis's own JUL loggers for the shutdown, then restores their levels"),
      Map.entry("server/ha/raft/ratis/RatisLogRecordFilter#install", RATIS_TAMING)));

  @Test
  void noProductionClassBuildsAJulLoggerOfItsOwn() throws Exception {
    final Map<String, Set<String>> callSites = callSitesOf(CompiledClasses.ofEveryModule());
    callSites.keySet().removeIf(key -> key.startsWith(LOG_PACKAGE));

    assertThat(CompiledClasses.unreviewed(callSites, REVIEWED)).as(
        "production classes that build a java.util.logging logger of their own instead of going through "
            + "LogManager.instance().log(...) / isLoggable(...): their records and guards ignore the installed logger "
            + "(arcadedb.log.impl=slf4j, a capturing logger), issue #10097").isEmpty();
    assertThat(CompiledClasses.stale(callSites, REVIEWED)).as("REVIEWED entries that no longer build a JUL logger: remove them")
        .isEmpty();
  }

  /** The control: every factory and a method reference to one are call sites, a literal naming the call is not. */
  @Test
  void theCensusSeesEveryShapeOfTheFactory() throws Exception {
    final Map<String, Set<String>> sites = callSitesOf(CompiledClasses.ofTheProbes());

    assertThat(sites).containsKeys("census/ShapeProbes$JulLogger#hoisted", "census/ShapeProbes$JulLogger#global",
        "census/ShapeProbes$JulLogger#anonymous");
    assertThat(sites).doesNotContainKey("census/ShapeProbes$JulLogger#namedInALiteral");
  }

  private static Map<String, Set<String>> callSitesOf(final Map<String, byte[]> classes) {
    return CompiledClasses.callSitesOf(classes, (owner, name, desc) -> AUTHORITY.equals(owner) && FACTORY.contains(name));
  }
}
