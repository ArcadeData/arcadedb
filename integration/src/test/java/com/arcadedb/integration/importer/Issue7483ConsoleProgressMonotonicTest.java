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
package com.arcadedb.integration.importer;

import com.arcadedb.integration.importer.format.CSVImporterFormat;
import com.arcadedb.integration.importer.format.FormatImporter;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7483's CLI-side half (raised in comment by robfrank): {@code FormatImporter#printProgress}, used by every
 * format's {@code AbstractImporter.printProgress()}, printed {@code context.parsed.get()} as the running
 * "Parsed %,d" figure. That counter is per-phase and is reset to zero by {@code ImporterContext#beginPhase()} at
 * every phase boundary (issue #7342), so a multi-phase CLI run (documents + vertices + edges) printed a total that
 * climbed, dropped back to near zero at the phase boundary, then climbed again - the console-log twin of the SSE
 * stream's #7483, just on stdout instead of {@code GET /api/v1/progress}.
 * <p>
 * {@code context.getParsedTotal()} is the monotonic, import-wide figure both readers want (per the same comment),
 * so {@code printProgress} now reads it, and {@code lastParsed} - what the NEXT call subtracts to turn the counter
 * into a rate - is kept in the same unit so the rate itself cannot go negative either.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7483ConsoleProgressMonotonicTest {

  @Test
  void printProgressTracksTheImportWideTotalAcrossAPhaseBoundary() {
    final ImporterSettings settings = new ImporterSettings();
    settings.verboseLevel = 2;
    final ImporterContext context = new ImporterContext();
    final ConsoleLogger logger = new ConsoleLogger(settings.verboseLevel);
    final FormatImporter importer = new CSVImporterFormat();

    context.parsed.set(100);
    importer.printProgress(settings, context, null, null, logger);
    assertThat(context.lastParsed).as("the first phase's own count").isEqualTo(100);

    // PHASE BOUNDARY: THE RAW PER-PHASE COUNTER RESETS TO 0 HERE. THIS IS THE EXACT MOMENT THE LOG USED TO PRINT A
    // SMALLER "Parsed" FIGURE THAN THE LINE BEFORE IT.
    context.beginPhase();
    context.parsed.set(5);
    importer.printProgress(settings, context, null, null, logger);

    assertThat(context.lastParsed)
        .as("the figure the log prints (and the next call's rate is measured against) is the import-wide total, "
            + "not the just-reset per-phase counter, so it must never go backwards across a phase boundary")
        .isEqualTo(105);
  }

  /**
   * The rate half of the same fix. Reading {@code getParsedTotal()} as the numerator is not enough on its own:
   * {@code ImporterContext#beginPhase()} has to rebase {@code lastParsed} to the cumulative total AS OF the
   * boundary, not zero it, or the very next call's {@code (parsedTotal - lastParsed) / deltaInSecs} divides the
   * WHOLE import's total-so-far by one tick's elapsed time - a rate spike as visible as the negative-rate bug this
   * reset originally fixed, just inflated instead of negative. Pinned by parsing the printed rate straight out of
   * the log line, not just the raw counter state after the call, since that state looks identical either way.
   */
  @Test
  void printProgressDoesNotSpikeTheRateAcrossAPhaseBoundary() {
    final ImporterSettings settings = new ImporterSettings();
    settings.verboseLevel = 2;
    final ImporterContext context = new ImporterContext();
    final List<String> lines = new ArrayList<>();
    final ConsoleLogger logger = new ConsoleLogger(settings.verboseLevel, lines::add);
    final FormatImporter importer = new CSVImporterFormat();

    context.parsed.set(100);
    importer.printProgress(settings, context, null, null, logger);

    context.beginPhase();
    context.parsed.set(5);
    importer.printProgress(settings, context, null, null, logger);

    assertThat(lines).hasSize(2);
    // deltaInSecs IS FORCED TO 1 WHEN THE CLOCK HASN'T ADVANCED (BOTH CALLS HAPPEN WITHIN THE SAME MILLISECOND
    // HERE), SO THE PRINTED RATE IS DIRECTLY (parsedTotal - lastParsed): 5 FOR THE NEW PHASE'S OWN PROGRESS IF
    // lastParsed WAS CORRECTLY REBASED TO 100, OR 105 (THE WHOLE IMPORT'S TOTAL) IF IT WAS WRONGLY ZEROED.
    assertThat(lines.get(1))
        .as("the second phase parsed only 5 rows of its own since the boundary, so the printed rate must read that, "
            + "not the cumulative 105")
        .contains("Parsed 105 (5/sec)")
        .doesNotContain("(105/sec)");
  }
}
