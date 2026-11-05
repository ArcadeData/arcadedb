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
 * Issue #10596: {@code LocalDateTime.parse(text, formatter)} takes the LOCAL fields of the parse and discards an offset the
 * pattern read, so a second parse written next to {@code DateUtils}' own answers a different instant for the same text - which is
 * how three of them got there ({@code asDateTime(format)}, {@code date(value, format)} and {@code DateParse}). Every
 * text-to-datetime conversion with a pattern goes through {@code DateUtils}, whose methods name what becomes of the offset
 * ({@code parseDateTimeWithFormat}, {@code parseDateTimeKeepingWallClockWithFormat}); the behaviour is pinned by
 * {@code Issue10596ExplicitFormatOffsetTest}.
 * <p>
 * The census ranges over the authority (#11288): every call of {@code LocalDateTime.parse(CharSequence, DateTimeFormatter)}, or
 * method reference to it, in the compiled classes of every module ({@link CompiledClasses}). The source-text census it replaces
 * matched the spelling {@code LocalDateTime.parse(} followed by a top-level comma, and allowed by file name.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class DatetimeParseWithAPatternCensusTest {
  private static final String AUTHORITY    = "java/time/LocalDateTime";
  private static final String METHOD       = "parse";
  private static final String WITH_PATTERN = "(Ljava/lang/CharSequence;Ljava/time/format/DateTimeFormatter;)Ljava/time/LocalDateTime;";
  /** The one class whose methods name what becomes of the offset: every method of it may parse. */
  private static final String DATE_UTILS   = "utility/DateUtils#";

  /** Parses of a value no user writes, each with why. */
  private static final Map<String, String> READS_NO_USER_TEXT = new TreeMap<>(Map.of(//
      "server/backup/BackupCoordinator#parseArchiveTimestamp",
      "reads the timestamp the engine itself put in the name of an archive, no zone in it"));

  @Test
  void noProductionClassParsesADatetimeWithAPatternOutsideDateUtils() throws Exception {
    final Map<String, Set<String>> callSites = callSitesOf(CompiledClasses.ofEveryModule());
    // DateUtils resolves with parseBest, which keeps the offset to decide on: the known parse is the archive name's
    assertThat(callSites).as("the census must reach the one parse with a pattern known outside DateUtils").containsKey(
        "server/backup/BackupCoordinator#parseArchiveTimestamp");
    callSites.keySet().removeIf(k -> k.startsWith(DATE_UTILS));

    assertThat(CompiledClasses.unreviewed(callSites, READS_NO_USER_TEXT)).as(
        "LocalDateTime.parse(text, pattern) outside DateUtils keeps the wall clock and drops the offset the pattern read: use "
            + "DateUtils.parseDateTimeWithFormat, or parseDateTimeKeepingWallClockWithFormat when the target keeps no zone (issue #10596)")
        .isEmpty();
    assertThat(CompiledClasses.stale(callSites, READS_NO_USER_TEXT)).as("entries that no longer parse: remove them").isEmpty();
  }

  private static Map<String, Set<String>> callSitesOf(final Map<String, byte[]> classes) {
    return CompiledClasses.callSitesOf(classes,
        (owner, name, desc) -> AUTHORITY.equals(owner) && METHOD.equals(name) && WITH_PATTERN.equals(desc));
  }
}
