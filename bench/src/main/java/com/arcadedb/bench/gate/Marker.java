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

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The state a regression issue keeps in a hidden comment in its body: the run that opened it and, per line id (see
 * {@link LineKey#id()}), the baseline it was judged against, the regression it was reported with and its newest run.
 * <p>
 * In memory an entry is a {@link JSONObject} with readable field names (see {@link #FIELDS}). GitHub caps an issue body at 65,536
 * characters and a nightly can report a few hundred lines, so {@link #render()} compacts the JSON, and {@link #parse(String)} restores it:
 * <ul>
 * <li>each field is written under a short key;</li>
 * <li>each ref, run id and bench commit is written once, in the {@code strings} table, and the entries hold its index;</li>
 * <li>the value most lines share for a field is written once, in {@code defaults}, and left out of those lines. A line that lacks a field
 * that has a default holds an explicit {@code null}.</li>
 * </ul>
 */
public final class Marker {
  private static final int VERSION = 1;

  private static final String NIGHTLY              = "nightly";
  /** The scale of a nightly marker that records none: phase 3 measured nightlies at M only. */
  private static final String LEGACY_NIGHTLY_SCALE = "M";

  private static final String PREFIX = "<!-- perf-regression ";
  private static final String SUFFIX = " -->";

  private static final Pattern PATTERN = Pattern.compile("<!-- perf-regression (\\{.*?\\}) -->", Pattern.DOTALL);

  /** A string kept in the {@code strings} table: a ref, a run id or a bench commit. */
  private static final int SHARED = 0;
  private static final int TEXT   = 1;
  private static final int NUMBER = 2;
  private static final int FLAG   = 3;

  /** The fields of an entry: readable name, short key in the rendered marker. Their kinds are in {@link #KINDS}. */
  private static final String[][] FIELDS = { //
      { "baseRef", "br" }, { "baseRunId", "bi" }, { "baseBench", "bc" }, { "baseStatus", "bs" }, { "baseMedian", "bm" },
      { "baseSpread", "bd" }, { "unit", "u" }, { "better", "o" }, { "added", "a" }, //
      { "worseRunId", "wi" }, { "worseBench", "wc" }, { "worseStatus", "ws" }, { "worseMedian", "wm" }, { "worseChange", "wx" },
      { "worseBand", "wn" }, //
      { "lastRun", "lr" }, { "lastStatus", "ls" }, { "lastMedian", "lm" }, { "lastChange", "lx" }, { "lastBand", "ln" },
      { "streak", "k" }, { "recovered", "v" } };
  private static final int[]      KINDS  = { SHARED, SHARED, SHARED, TEXT, NUMBER, NUMBER, TEXT, TEXT, FLAG, //
      SHARED, SHARED, TEXT, NUMBER, NUMBER, NUMBER, //
      SHARED, TEXT, NUMBER, NUMBER, NUMBER, NUMBER, FLAG };

  private static final Map<String, Integer> BY_NAME = new HashMap<>();
  private static final Map<String, Integer> BY_KEY  = new HashMap<>();

  static {
    for (int i = 0; i < FIELDS.length; i++) {
      BY_NAME.put(FIELDS[i][0], i);
      BY_KEY.put(FIELDS[i][1], i);
    }
  }

  private String                        trigger;
  private String                        ref;
  private String                        scale;
  private String                        baseRef;
  private final Map<String, JSONObject> lines = new LinkedHashMap<>();

  private Marker() {
  }

  public static Marker create(final String trigger, final String ref, final String baseRef) {
    return create(trigger, ref, null, baseRef);
  }

  /** A marker of the run {@code trigger:ref}; {@code scale} is the scale of a nightly event, null for a release. */
  public static Marker create(final String trigger, final String ref, final String scale, final String baseRef) {
    final Marker marker = new Marker();
    marker.trigger = trigger;
    marker.ref = ref;
    marker.scale = scale;
    marker.baseRef = baseRef;
    return marker;
  }

  /** Whether the body carries something that looks like a marker, valid or not. */
  static boolean hasMarker(final String issueBody) {
    return issueBody != null && issueBody.contains(PREFIX);
  }

  /**
   * Returns the marker of the body, or {@code null} when there is none, its JSON is invalid, its version is not 1, it lacks trigger or
   * ref, or one of its entries is not of the shape {@link #isValidEntry} reads.
   */
  public static Marker parse(final String issueBody) {
    if (issueBody == null)
      return null;
    final Matcher matcher = PATTERN.matcher(issueBody);
    if (!matcher.find())
      return null;
    try {
      final JSONObject json = new JSONObject(matcher.group(1));
      if (json.getInt("v", -1) != VERSION)
        return null;
      final String trigger = json.getString("trigger", null);
      final String ref = json.getString("ref", null);
      if (trigger == null || ref == null)
        return null;
      final JSONArray strings = json.getJSONArray("strings", new JSONArray());
      final JSONObject defaults = json.getJSONObject("defaults", new JSONObject());
      for (final String key : defaults.keySet())
        if (!BY_KEY.containsKey(key))
          return null;
      final Marker marker = create(trigger, ref, json.getString("scale", null), json.getString("baseRef", null));
      final JSONObject lineObjects = json.getJSONObject("lines", new JSONObject());
      for (final String id : lineObjects.keySet()) {
        final Object entry = lineObjects.get(id, null);
        if (!(entry instanceof JSONObject e))
          return null;
        final JSONObject decoded = decode(e, defaults, strings);
        if (decoded == null || !isValidEntry(id, decoded))
          return null;
        marker.lines.put(id, decoded);
      }
      return marker;
    } catch (final RuntimeException e) {
      return null;
    }
  }

  /** The readable entry of a rendered one, its defaults applied; null when a key is unknown or a value is of the wrong type. */
  private static JSONObject decode(final JSONObject encoded, final JSONObject defaults, final JSONArray strings) {
    for (final String key : encoded.keySet())
      if (!BY_KEY.containsKey(key))
        return null;
    final JSONObject entry = new JSONObject();
    for (int i = 0; i < FIELDS.length; i++) {
      final String key = FIELDS[i][1];
      final Object value;
      if (encoded.has(key))
        value = encoded.get(key, null);
      else
        value = defaults.get(key, null);
      if (value == null)
        continue;
      final Object decoded = switch (KINDS[i]) {
        case SHARED -> value instanceof Integer index && index >= 0 && index < strings.length() && strings.get(index) instanceof String text ?
            text :
            null;
        case TEXT -> value instanceof String ? value : null;
        case NUMBER -> value instanceof Number ? value : null;
        default -> value instanceof Boolean ? value : null;
      };
      if (decoded == null)
        return null;
      entry.put(FIELDS[i][0], decoded);
    }
    return entry;
  }

  /**
   * Whether a decoded entry has the shape the planner reads: baseRef, lastRun and baseStatus, a numeric streak, and a numeric baseMedian and
   * baseSpread present exactly when the line is not a status line. A marker a human edited into another shape is unusable as a whole,
   * so the issue is skipped with a warning instead of failing the gate.
   */
  private static boolean isValidEntry(final String lineId, final JSONObject entry) {
    if (!entry.has("baseRef") || !entry.has("lastRun") || !entry.has("baseStatus") || !entry.has("streak"))
      return false;
    final boolean statusLine = lineId.endsWith("/" + LineKey.STATUS);
    return statusLine ? !entry.has("baseMedian") && !entry.has("baseSpread") : entry.has("baseMedian") && entry.has("baseSpread");
  }

  /**
   * Renders the marker deterministically (lines sorted by id, fields in their fixed order), compacted as the class comment says, without
   * {@code -->} or {@code >} inside the JSON.
   *
   * @throws IllegalArgumentException when an entry holds a field the marker does not know
   */
  public String render() {
    final List<String> ids = new ArrayList<>(lines.keySet());
    Collections.sort(ids);

    // the values of each line, shared strings replaced by their index in the table
    final Map<String, Integer> strings = new LinkedHashMap<>();
    final Object[][] values = new Object[ids.size()][];
    for (int l = 0; l < ids.size(); l++) {
      final JSONObject entry = lines.get(ids.get(l));
      for (final String name : entry.keySet())
        if (!BY_NAME.containsKey(name))
          throw new IllegalArgumentException("Unknown marker field '" + name + "' in line " + ids.get(l));
      values[l] = new Object[FIELDS.length];
      for (int i = 0; i < FIELDS.length; i++) {
        final Object value = entry.get(FIELDS[i][0], null);
        values[l][i] = KINDS[i] == SHARED && value instanceof String text ? strings.computeIfAbsent(text, k -> strings.size()) : value;
      }
    }

    // the default of each field: the value most lines hold, when at least two do and more lines hold it than lack the field
    final JSONObject defaults = new JSONObject();
    final Object[] defaultOf = new Object[FIELDS.length];
    for (int i = 0; i < FIELDS.length; i++) {
      final Map<Object, Integer> counts = new LinkedHashMap<>();
      int absent = 0;
      for (final Object[] line : values)
        if (line[i] == null)
          absent++;
        else
          counts.merge(line[i], 1, Integer::sum);
      Object best = null;
      int bestCount = 1;
      for (final Map.Entry<Object, Integer> c : counts.entrySet())
        if (c.getValue() > bestCount) {
          best = c.getKey();
          bestCount = c.getValue();
        }
      if (best != null && bestCount > absent) {
        defaultOf[i] = best;
        defaults.put(FIELDS[i][1], best);
      }
    }

    final JSONObject encodedLines = new JSONObject();
    for (int l = 0; l < ids.size(); l++) {
      final JSONObject encoded = new JSONObject();
      for (int i = 0; i < FIELDS.length; i++) {
        final Object value = values[l][i];
        if (value == null) {
          if (defaultOf[i] != null)
            encoded.put(FIELDS[i][1], JSONObject.NULL);
        } else if (!value.equals(defaultOf[i]))
          encoded.put(FIELDS[i][1], value);
      }
      encodedLines.put(ids.get(l), encoded);
    }

    final JSONObject json = new JSONObject();
    json.put("v", VERSION);
    json.put("trigger", trigger);
    json.put("ref", ref);
    if (scale != null)
      json.put("scale", scale);
    if (baseRef != null)
      json.put("baseRef", baseRef);
    json.put("strings", new JSONArray(new ArrayList<>(strings.keySet())));
    json.put("defaults", defaults);
    json.put("lines", encodedLines);
    return PREFIX + json.toString().replace(">", "\\u003e") + SUFFIX;
  }

  public String getTrigger() {
    return trigger;
  }

  public void setTrigger(final String trigger) {
    this.trigger = trigger;
  }

  public String getRef() {
    return ref;
  }

  public void setRef(final String ref) {
    this.ref = ref;
  }

  /**
   * The scale of the nightly event: the lines of one commit measured at another scale are another event. A nightly marker written before
   * the scale was recorded (phase 3) reads as {@code M}, the only nightly scale then; null for a release, whose scales are one run.
   */
  public String getScale() {
    if (scale != null)
      return scale;
    return NIGHTLY.equals(trigger) ? LEGACY_NIGHTLY_SCALE : null;
  }

  public String getBaseRef() {
    return baseRef;
  }

  public void setBaseRef(final String baseRef) {
    this.baseRef = baseRef;
  }

  /** The entry of the line, or {@code null} when the marker does not track it. */
  public JSONObject line(final String lineId) {
    return lines.get(lineId);
  }

  public Set<String> lineIds() {
    return lines.keySet();
  }

  public void putLine(final String lineId, final JSONObject entry) {
    lines.put(lineId, entry);
  }
}
