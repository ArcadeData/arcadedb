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
}
