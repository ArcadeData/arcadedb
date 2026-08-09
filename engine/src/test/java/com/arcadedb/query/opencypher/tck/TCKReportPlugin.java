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
package com.arcadedb.query.opencypher.tck;

import io.cucumber.plugin.ConcurrentEventListener;
import io.cucumber.plugin.event.EventPublisher;
import io.cucumber.plugin.event.TestCaseFinished;
import io.cucumber.plugin.event.TestCaseStarted;
import io.cucumber.plugin.event.TestRunFinished;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Cucumber plugin that generates a summary compliance report for the OpenCypher TCK.
 * <p>
 * Writes two files next to each other under {@code target/}, both consumed by the {@code opencypher-tck-tests} CI job:
 * <ul>
 *   <li>{@code tck-compliance-report.txt} - the fixed-width form, also echoed to stdout so it is readable in a raw build log;</li>
 *   <li>{@code tck-compliance-report.md} - the same numbers as markdown, appended to {@code $GITHUB_STEP_SUMMARY} so conformance renders on the job
 *       page without anyone downloading an artifact.</li>
 * </ul>
 * The markdown form leads with the categories that are not at 100%, because that is the question a compliance report exists to answer, and keeps the
 * full list behind a {@code <details>} block so the summary stays short. Category order inside each table is stable (worst rate first for the gaps,
 * alphabetical for the full list) so two runs can be diffed against each other.
 * <p>
 * Percentages are reported to one decimal here and as integers in the text form. Integer truncation is misleading at this scale: 3812 of 3897 is 97.8%,
 * which the text form has always rendered as "97%".
 */
public class TCKReportPlugin implements ConcurrentEventListener {

  private final AtomicInteger totalScenarios = new AtomicInteger();
  private final AtomicInteger passed = new AtomicInteger();
  private final AtomicInteger failed = new AtomicInteger();
  private final AtomicInteger skipped = new AtomicInteger();
  private final Map<String, int[]> categoryStats = new ConcurrentHashMap<>();
  // categoryStats value: [total, passed, failed, skipped]

  @Override
  public void setEventPublisher(final EventPublisher publisher) {
    publisher.registerHandlerFor(TestCaseStarted.class, this::handleTestCaseStarted);
    publisher.registerHandlerFor(TestCaseFinished.class, this::handleTestCaseFinished);
    publisher.registerHandlerFor(TestRunFinished.class, this::handleTestRunFinished);
  }

  private void handleTestCaseStarted(final TestCaseStarted event) {
    totalScenarios.incrementAndGet();
  }

  private void handleTestCaseFinished(final TestCaseFinished event) {
    final String category = extractCategory(event.getTestCase().getUri());

    final int[] stats = categoryStats.computeIfAbsent(category, k -> new int[4]);
    stats[0]++;

    switch (event.getResult().getStatus()) {
    case PASSED:
      passed.incrementAndGet();
      stats[1]++;
      break;
    case FAILED:
      failed.incrementAndGet();
      stats[2]++;
      break;
    case SKIPPED:
    case PENDING:
    case AMBIGUOUS:
    case UNDEFINED:
      skipped.incrementAndGet();
      stats[3]++;
      break;
    }
  }

  private void handleTestRunFinished(final TestRunFinished event) {
    final int total = totalScenarios.get();
    final int pass = passed.get();
    final int fail = failed.get();
    final int skip = skipped.get();

    final StringBuilder sb = new StringBuilder();
    sb.append("\n");
    sb.append("=======================================================\n");
    sb.append("         OpenCypher TCK Compliance Report\n");
    sb.append("=======================================================\n");
    sb.append(String.format("Total scenarios:  %d%n", total));
    sb.append(String.format("Passed:           %d (%d%%)%n", pass, total > 0 ? pass * 100 / total : 0));
    sb.append(String.format("Failed:           %d (%d%%)%n", fail, total > 0 ? fail * 100 / total : 0));
    sb.append(String.format("Skipped:          %d (%d%%)%n", skip, total > 0 ? skip * 100 / total : 0));
    sb.append("-------------------------------------------------------\n");
    sb.append("By category:\n");

    categoryStats.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry -> {
      final String cat = entry.getKey();
      final int[] s = entry.getValue();
      sb.append(String.format("  %-40s %3d/%3d passed (%d%%)%n", cat, s[1], s[0], s[0] > 0 ? s[1] * 100 / s[0] : 0));
    });

    sb.append("=======================================================\n");

    System.out.println(sb);

    write("target/tck-compliance-report.txt", sb.toString());
    write("target/tck-compliance-report.md", buildMarkdownReport(total, pass, fail, skip));
  }

  private String buildMarkdownReport(final int total, final int pass, final int fail, final int skip) {
    final StringBuilder md = new StringBuilder();

    md.append("## openCypher TCK compliance\n\n");
    md.append(String.format(Locale.ROOT, "**%,d of %,d scenarios passed (%s)** - %,d failed, %,d skipped%n%n", pass, total, percentage(pass, total),
        fail, skip));

    final List<Map.Entry<String, int[]>> gaps = categoryStats.entrySet().stream().filter(e -> e.getValue()[1] < e.getValue()[0])
        .sorted(Comparator.<Map.Entry<String, int[]>>comparingDouble(e -> ratio(e.getValue()[1], e.getValue()[0]))
            .thenComparing(Map.Entry::getKey)).toList();

    if (gaps.isEmpty())
      md.append("Every category is at 100%.\n\n");
    else {
      md.append(String.format(Locale.ROOT, "### Not yet at 100%% (%d of %d categories)%n%n", gaps.size(), categoryStats.size()));
      appendTable(md, gaps);
      md.append("\n");
    }

    md.append("<details>\n<summary>All ").append(categoryStats.size()).append(" categories</summary>\n\n");
    appendTable(md, categoryStats.entrySet().stream().sorted(Map.Entry.comparingByKey()).toList());
    md.append("\n</details>\n");

    return md.toString();
  }

  private void appendTable(final StringBuilder md, final List<Map.Entry<String, int[]>> rows) {
    md.append("| Category | Passed | Total | Rate |\n|---|---:|---:|---:|\n");
    for (final Map.Entry<String, int[]> entry : rows) {
      final int[] s = entry.getValue();
      md.append(String.format(Locale.ROOT, "| `%s` | %,d | %,d | %s |%n", entry.getKey(), s[1], s[0], percentage(s[1], s[0])));
    }
  }

  private static String percentage(final int part, final int whole) {
    return String.format(Locale.ROOT, "%.1f%%", ratio(part, whole) * 100.0);
  }

  private static double ratio(final int part, final int whole) {
    return whole > 0 ? (double) part / whole : 0.0;
  }

  private static void write(final String path, final String content) {
    try (final PrintWriter writer = new PrintWriter(new FileWriter(path))) {
      writer.print(content);
    } catch (final IOException e) {
      System.err.println("Failed to write TCK report '" + path + "': " + e.getMessage());
    }
  }

  private String extractCategory(final URI uri) {
    final String path = uri.toString();
    // Extract category from path like: classpath:opencypher/tck/features/clauses/match/Match1.feature
    final int featuresIdx = path.indexOf("features/");
    if (featuresIdx < 0)
      return "unknown";

    final String relative = path.substring(featuresIdx + "features/".length());
    final int lastSlash = relative.lastIndexOf('/');
    if (lastSlash < 0)
      return relative;
    return relative.substring(0, lastSlash);
  }
}
