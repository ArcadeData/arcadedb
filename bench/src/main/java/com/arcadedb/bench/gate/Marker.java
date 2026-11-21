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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The state a regression issue keeps in a hidden comment in its body: the run that opened it and, per line id (see
 * {@link LineKey#id()}), the baseline it was judged against and its recovery streak.
 */
public final class Marker {
  private static final int VERSION = 1;

  private static final String PREFIX = "<!-- perf-regression ";
  private static final String SUFFIX = " -->";

  private static final Pattern PATTERN = Pattern.compile("<!-- perf-regression (\\{.*?\\}) -->", Pattern.DOTALL);

  private String                   trigger;
  private String                   ref;
  private String                   baseRef;
  private final Map<String, JSONObject> lines = new LinkedHashMap<>();

  private Marker() {
  }

  public static Marker create(final String trigger, final String ref, final String baseRef) {
    final Marker marker = new Marker();
    marker.trigger = trigger;
    marker.ref = ref;
    marker.baseRef = baseRef;
    return marker;
  }

  /** Whether the body carries something that looks like a marker, valid or not. */
  static boolean hasMarker(final String issueBody) {
    return issueBody != null && issueBody.contains(PREFIX);
  }

  /** Returns the marker of the body, or {@code null} when there is none, its JSON is invalid, its version is not 1 or it lacks trigger or ref. */
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
      final Marker marker = create(trigger, ref, json.getString("baseRef", null));
      final JSONObject lineObjects = json.getJSONObject("lines", new JSONObject());
      for (final String id : lineObjects.keySet())
        marker.lines.put(id, lineObjects.getJSONObject(id));
      return marker;
    } catch (final RuntimeException e) {
      return null;
    }
  }

  /** Renders the marker deterministically (lines and their fields sorted by name), without {@code -->} or {@code >} inside the JSON. */
  public String render() {
    final JSONObject json = new JSONObject();
    json.put("v", VERSION);
    json.put("trigger", trigger);
    json.put("ref", ref);
    if (baseRef != null)
      json.put("baseRef", baseRef);
    final JSONObject sortedLines = new JSONObject();
    final List<String> ids = new ArrayList<>(lines.keySet());
    Collections.sort(ids);
    for (final String id : ids) {
      final JSONObject entry = lines.get(id);
      final JSONObject sorted = new JSONObject();
      final List<String> fields = new ArrayList<>(entry.keySet());
      Collections.sort(fields);
      for (final String field : fields)
        sorted.put(field, entry.get(field));
      sortedLines.put(id, sorted);
    }
    json.put("lines", sortedLines);
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
