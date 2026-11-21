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
package com.arcadedb.bench.gate;

import com.arcadedb.serializer.json.JSONObject;

import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Whether the bench sources changed between two bench commits, as the workflow answered it with {@code git diff --quiet A B -- bench/}
 * into a file {@code {"A..B": true|false}}. The gate itself runs no git: a pair the file does not answer is unknown.
 */
public final class BenchChanges {
  private final Map<String, Boolean> answers;

  private BenchChanges(final Map<String, Boolean> answers) {
    this.answers = answers;
  }

  /** Reads the answers; a missing file answers nothing (every pair unknown). */
  public static BenchChanges read(final Path file) {
    return read(file, System.err);
  }

  /** Reads the answers; a missing file, or one that is not a JSON object (warned on {@code warn}), answers nothing. */
  public static BenchChanges read(final Path file, final PrintStream warn) {
    final Map<String, Boolean> answers = new HashMap<>();
    if (file == null || !Files.isRegularFile(file))
      return new BenchChanges(answers);
    final JSONObject json;
    try {
      json = new JSONObject(Files.readString(file, StandardCharsets.UTF_8));
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    } catch (final RuntimeException e) {
      warn.println("WARNING: ignoring unreadable bench changes file " + file + " (every bench change unknown): " + e.getMessage());
      return new BenchChanges(answers);
    }
    for (final String pair : json.keySet()) {
      final Object answer = json.get(pair, null);
      if (answer instanceof Boolean b)
        answers.put(pair, b);
    }
    return new BenchChanges(answers);
  }

  /** True or false as answered, false for the same commit, null when unknown (a null commit or no answer). */
  public Boolean changed(final String base, final String next) {
    if (base == null || next == null)
      return null;
    if (base.equals(next))
      return Boolean.FALSE;
    return answers.get(pair(base, next));
  }

  /** The pairs the judged verdicts need answered: "A..B" for each verdict with a baseline and two different known bench commits. */
  public static Set<String> pairsNeeded(final List<Verdict> verdicts) {
    final Set<String> pairs = new TreeSet<>();
    for (final Verdict v : verdicts)
      if (v.baseline() != null)
        addPair(pairs, v.baseline().benchCommit(), v.newest().benchCommit());
    return pairs;
  }

  /**
   * The pairs a plan over these lines and issues needs answered: those of the verdicts plus, for each line of an open issue, its
   * recorded baseline's bench commit against the newest point of the issue's trigger (the pair its body row shows).
   */
  public static Set<String> pairsNeeded(final Map<LineKey, List<Point>> lines, final List<TrackedIssue> issues) {
    final Set<String> pairs = pairsNeeded(Judge.judge(lines));
    final Map<String, LineKey> keysById = new HashMap<>();
    for (final LineKey key : lines.keySet())
      keysById.put(key.id(), key);
    for (final TrackedIssue issue : issues) {
      if (!issue.open())
        continue;
      for (final String id : issue.marker().lineIds()) {
        final LineKey key = keysById.get(id);
        if (key == null)
          continue;
        final Point newest = Judge.newest(lines.get(key), issue.marker().getTrigger());
        if (newest != null)
          addPair(pairs, issue.marker().line(id).getString("baseBench", null), newest.benchCommit());
      }
    }
    return pairs;
  }

  static String pair(final String base, final String next) {
    return base + ".." + next;
  }

  private static void addPair(final Set<String> pairs, final String base, final String next) {
    if (base != null && next != null && !base.equals(next))
      pairs.add(pair(base, next));
  }
}
