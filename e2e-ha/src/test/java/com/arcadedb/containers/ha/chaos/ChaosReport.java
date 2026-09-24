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

package com.arcadedb.containers.ha.chaos;

import com.arcadedb.serializer.json.JSONObject;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Writes the run's report under {@code target/chaos/<seed>/}. Step and trend lines are flushed as they are written,
 * so a run killed by the CI timeout still leaves everything up to the step it hung in.
 */
public final class ChaosReport implements AutoCloseable {
  private final Path           dir;
  private final ChaosConfig    config;
  private final BufferedWriter steps;
  private final BufferedWriter trends;

  public ChaosReport(final Path dir, final ChaosConfig config) throws IOException {
    Files.createDirectories(dir);
    this.dir = dir;
    this.config = config;
    this.steps = Files.newBufferedWriter(dir.resolve("steps.log"), StandardCharsets.UTF_8);
    this.trends = Files.newBufferedWriter(dir.resolve("trends.csv"), StandardCharsets.UTF_8);
    trends.write(TrendRow.CSV_HEADER);
    trends.newLine();
    trends.flush();
  }

  public Path dir() {
    return dir;
  }

  public synchronized void step(final StepRecord record) throws IOException {
    steps.write(record.toLine());
    steps.newLine();
    steps.flush();
  }

  /** The line of a step that failed after its fault was injected, so before its checkpoint could write a record. */
  public synchronized void stepFailed(final int step, final String fault, final String targets, final ChaosResult result)
      throws IOException {
    steps.write("step=" + step + " fault=" + fault + " targets=" + targets + " FAILED kind=" + result.kind() + " message="
        + result.message());
    steps.newLine();
    steps.flush();
  }

  public synchronized void trend(final TrendRow row) throws IOException {
    for (final String line : row.toCsvLines()) {
      trends.write(line);
      trends.newLine();
    }
    trends.flush();
  }

  public void ledgerDiff(final List<Violation> violations) throws IOException {
    final StringBuilder text = new StringBuilder();
    for (final Violation violation : violations) {
      text.append("== ").append(violation.describe()).append('\n');
      if (violation.details().isEmpty())
        for (final long key : violation.keys())
          text.append(Ledger.format(key)).append('\n');
      else
        for (final String line : violation.details())
          text.append(line).append('\n');
    }
    Files.writeString(dir.resolve("ledger-diff.txt"), text.toString(), StandardCharsets.UTF_8);
  }

  public void summary(final ChaosResult result, final Ledger ledger, final long lateCommits) throws IOException {
    final JSONObject counts = new JSONObject()
        .put("acked", ledger.count(Ledger.ACKED))
        .put("ackedLate", ledger.count(Ledger.ACKED_LATE))
        .put("failed", ledger.count(Ledger.FAILED))
        .put("unknown", ledger.count(Ledger.UNKNOWN))
        .put("lostUnknown", ledger.count(Ledger.LOST_UNKNOWN))
        .put("inFlight", ledger.count(Ledger.IN_FLIGHT));
    final JSONObject summary = new JSONObject()
        .put("seed", config.seed())
        .put("nodes", config.nodes())
        .put("duration", config.duration().toString())
        .put("writers", config.writers())
        .put("faults", config.faultsSpec())
        .put("result", result.kind().name())
        .put("message", result.message())
        .put("steps", result.steps())
        .put("ledger", counts)
        .put("lateCommits", lateCommits)
        .put("replay", config.replayCommand());
    Files.writeString(dir.resolve("summary.json"), summary.toString(2), StandardCharsets.UTF_8);
  }

  @Override
  public void close() throws IOException {
    steps.close();
    trends.close();
  }
}
