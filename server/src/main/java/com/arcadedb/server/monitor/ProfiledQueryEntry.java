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
package com.arcadedb.server.monitor;

import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

/**
 * One recorded query. The profiler keeps a fixed number of these in a ring buffer, so what an entry holds is what bounds the memory
 * a recording can use: the statement text, the names and the plan are kept in a bounded form. Of the plan only what the results
 * report is kept - the name and the cost of each step, flattened in the order the plan lists them - in two arrays of at most
 * {@value #MAX_PLAN_STEPS} elements, never the plan itself.
 */
public class ProfiledQueryEntry {
  /** Longest statement text kept, in chars; a longer one is cut and says how long it was. */
  static final int MAX_QUERY_TEXT_LENGTH = 4096;
  /** Longest database, language or step name kept, in chars. */
  static final int MAX_NAME_LENGTH       = 256;
  /** Most steps kept of a plan, nested ones included. */
  static final int MAX_PLAN_STEPS        = 256;
  /** Deepest level of nested steps read from a plan. */
  static final int MAX_PLAN_DEPTH        = 32;

  private static final int                              MAX_CANONICAL_NAMES = 512;
  private static final String                           UNNAMED_STEP        = "unknown";
  private static final String[]                         NO_NAMES            = new String[0];
  private static final long[]                           NO_COSTS            = new long[0];
  private static final ConcurrentHashMap<String, String> CANONICAL_NAMES     = new ConcurrentHashMap<>();

  final String   database;
  final String   language;
  final String   queryText;
  final long     timestampMs;
  final long     deserializationNanos;
  final long     engineNanos;
  final long     serializationNanos;
  // Parallel arrays, one slot per step in the order the plan lists them: the name, and the cost the engine reported for it
  // (-1 when the plan gave none, as the engine does for "not calculated")
  final String[] stepNames;
  final long[]   stepCosts;

  public ProfiledQueryEntry(final String database, final String language, final String queryText, final long timestampMs,
      final long deserializationNanos, final long engineNanos, final long serializationNanos, final JSONObject executionPlan) {
    this.database = limitName(database);
    this.language = limitName(language);
    this.queryText = limitQueryText(queryText);
    this.timestampMs = timestampMs;
    this.deserializationNanos = deserializationNanos;
    this.engineNanos = engineNanos;
    this.serializationNanos = serializationNanos;

    final PlanSteps steps = new PlanSteps();
    if (executionPlan != null)
      steps.collect(executionPlan);
    this.stepNames = steps.count == 0 ? NO_NAMES : Arrays.copyOf(steps.names, steps.count);
    this.stepCosts = steps.count == 0 ? NO_COSTS : Arrays.copyOf(steps.costs, steps.count);
  }

  public long getTotalNanos() {
    return deserializationNanos + engineNanos + serializationNanos;
  }

  static String limitQueryText(final String text) {
    if (text == null || text.length() <= MAX_QUERY_TEXT_LENGTH)
      return text;

    // Never end on the first half of a surrogate pair
    int end = MAX_QUERY_TEXT_LENGTH;
    if (Character.isHighSurrogate(text.charAt(end - 1)))
      end--;
    return text.substring(0, end) + "... [truncated, " + text.length() + " chars in total]";
  }

  private static String limitName(final String name) {
    if (name == null || name.length() <= MAX_NAME_LENGTH)
      return name;

    int end = MAX_NAME_LENGTH;
    if (Character.isHighSurrogate(name.charAt(end - 1)))
      end--;
    return name.substring(0, end);
  }

  /**
   * One shared instance per step name, so the entries of a ring buffer that ran the same plans hold references to a few strings
   * instead of a copy each. The names are the step classes of the engine, a handful; the table stops taking new ones at
   * {@value #MAX_CANONICAL_NAMES}, and a name past that is simply kept as it came.
   */
  private static String canonicalName(final String name) {
    final String known = CANONICAL_NAMES.get(name);
    if (known != null)
      return known;
    if (CANONICAL_NAMES.size() >= MAX_CANONICAL_NAMES)
      return name;
    final String raced = CANONICAL_NAMES.putIfAbsent(name, name);
    return raced != null ? raced : name;
  }

  /** The steps of a plan, read into arrays that never grow past {@link #MAX_PLAN_STEPS}. */
  private static final class PlanSteps {
    private String[] names = new String[8];
    private long[]   costs = new long[8];
    private int      count;

    void collect(final JSONObject plan) {
      try {
        if (plan.opt("steps") instanceof JSONArray steps)
          collect(steps, 0);
      } catch (final RuntimeException e) {
        // A plan that does not have the shape the engine gives it keeps the steps read so far: recording must never fail the query
        LogManager.instance().log(this, Level.FINE, "Could not read the steps of the execution plan for profiling", e);
      }
    }

    private void collect(final JSONArray steps, final int depth) {
      for (int i = 0; i < steps.length() && count < MAX_PLAN_STEPS; i++) {
        if (!(steps.get(i) instanceof JSONObject step))
          continue;

        final String name = limitName(step.getString("name", UNNAMED_STEP));
        // Default to the same -1 the engine uses for "not calculated", so a plan that omits the field is classified as
        // untimed rather than as a step that measurably took no time at all.
        //
        // "cost" is the step's SELF cost and never the subtree roll-up: the roll-up travels as "totalCost" and is
        // deliberately not read here. Reading it while also collecting the children it already contains charged the same
        // nanoseconds to both the container and the step that actually timed them, so a plain type scan showed up twice
        // and the per-step totals added up to more than the Engine total (issue #7329).
        add(name.isBlank() ? UNNAMED_STEP : canonicalName(name), step.getLong("cost", -1));

        if (depth + 1 < MAX_PLAN_DEPTH && step.opt("subSteps") instanceof JSONArray subSteps)
          collect(subSteps, depth + 1);
      }
    }

    private void add(final String name, final long cost) {
      if (count == names.length) {
        final int capacity = Math.min(MAX_PLAN_STEPS, count * 2);
        names = Arrays.copyOf(names, capacity);
        costs = Arrays.copyOf(costs, capacity);
      }
      names[count] = name;
      costs[count++] = cost;
    }
  }
}
