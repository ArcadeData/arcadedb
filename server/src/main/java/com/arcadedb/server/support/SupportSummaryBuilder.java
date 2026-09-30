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
package com.arcadedb.server.support;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Builds {@code summary.json} (v1) from the (already redacted) entries of the collected log window: line count, count per
 * level and the most frequent exceptions with their first message, first and last time seen and up to 15 stack lines.
 * <p>
 * Fed by {@link SupportLogCollector}: {@link #beginEntry} for a line that starts a log entry, {@link #continuation} for the
 * lines that follow it (a stack trace belongs to the entry before it), {@link #finish} at the end.
 */
public class SupportSummaryBuilder {
  static final int MAX_STACK_LINES   = 15;
  static final int MAX_MESSAGE       = 300;
  static final int MAX_GROUPS        = 1000;
  static final int MAX_TOP           = 20;

  private static final Pattern EXCEPTION_LINE = Pattern.compile(
      "^\\s*(?:Caused by:\\s*|Suppressed:\\s*)?((?:[a-z_][\\w$]*\\.)+[A-Z][\\w$]*)(?::\\s?(.*))?$");

  private final ZoneId             zone;
  private final Map<String, Long>  levels = new TreeMap<>();
  private final Map<String, Group> groups = new LinkedHashMap<>();
  private       long               lines;

  // current entry
  private long          entryKey = -1;
  private String        exClass;
  private String        exMessage;
  private StringBuilder exStack;
  private int           exStackLines;

  private static final class Group {
    final String className;
    final String message;
    final String sampleStack;
    long         count;
    long         firstSeenKey;
    long         lastSeenKey;

    Group(final String className, final String message, final String sampleStack, final long key) {
      this.className = className;
      this.message = message;
      this.sampleStack = sampleStack;
      this.firstSeenKey = key;
      this.lastSeenKey = key;
    }
  }

  public SupportSummaryBuilder(final ZoneId zone) {
    this.zone = zone;
  }

  public long getLines() {
    return lines;
  }

  /** A line that starts a log entry (it has a timestamp). {@code key} is the timestamp as {@link SupportLogCollector#toKey}. */
  public void beginEntry(final long key, final String level) {
    endEntry();
    lines++;
    entryKey = key;
    if (level != null)
      levels.merge(level, 1L, Long::sum);
  }

  /** A line that continues the current entry. */
  public void continuation(final String redactedLine) {
    lines++;
    if (entryKey < 0)
      return;
    if (exClass == null) {
      final Matcher m = EXCEPTION_LINE.matcher(redactedLine);
      if (m.matches()) {
        exClass = m.group(1);
        final String message = m.group(2) == null ? "" : m.group(2).strip();
        exMessage = message.length() > MAX_MESSAGE ? message.substring(0, MAX_MESSAGE) : message;
        exStack = new StringBuilder(redactedLine.strip());
        exStackLines = 1;
      }
    } else if (exStackLines < MAX_STACK_LINES) {
      exStack.append('\n').append(redactedLine.stripTrailing());
      exStackLines++;
    }
  }

  private void endEntry() {
    if (exClass != null) {
      final String groupKey = exClass + '\u0000' + exMessage;
      final Group group = groups.get(groupKey);
      if (group != null) {
        group.count++;
        group.lastSeenKey = Math.max(group.lastSeenKey, entryKey);
        group.firstSeenKey = Math.min(group.firstSeenKey, entryKey);
      } else if (groups.size() < MAX_GROUPS) {
        final Group created = new Group(exClass, exMessage, exStack.toString(), entryKey);
        created.count = 1;
        groups.put(groupKey, created);
      }
    }
    entryKey = -1;
    exClass = null;
    exMessage = null;
    exStack = null;
    exStackLines = 0;
  }

  /** Closes the last entry and renders the summary. */
  public JSONObject finish(final SupportLogWindow window) {
    endEntry();

    final JSONObject json = new JSONObject();
    json.put("schema", 1);
    json.put("window", new JSONObject().put("from", window.from().toString()).put("to", window.to().toString()));
    json.put("timeZone", zone.getId());
    json.put("lines", lines);

    final JSONObject levelsJson = new JSONObject();
    for (final Map.Entry<String, Long> e : levels.entrySet())
      levelsJson.put(e.getKey(), e.getValue());
    json.put("levels", levelsJson);

    final List<Group> sorted = new ArrayList<>(groups.values());
    sorted.sort(Comparator.comparingLong((Group g) -> g.count).reversed().thenComparing(g -> g.className));
    final JSONArray top = new JSONArray();
    for (int i = 0; i < sorted.size() && i < MAX_TOP; i++) {
      final Group g = sorted.get(i);
      top.put(new JSONObject().put("class", g.className).put("message", g.message).put("count", g.count)
          .put("firstSeen", toInstant(g.firstSeenKey).toString()).put("lastSeen", toInstant(g.lastSeenKey).toString())
          .put("sampleStack", g.sampleStack));
    }
    json.put("topExceptions", top);
    return json;
  }

  private Instant toInstant(final long key) {
    return SupportLogCollector.fromKey(key).atZone(zone).toInstant();
  }
}
