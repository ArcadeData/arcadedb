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

import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** A regression issue as the workflow lists it: its open or closed state, who closed it, and the state kept in its marker. */
public record TrackedIssue(int number, boolean open, boolean closedByMaintainer, Marker marker) {
  private static final String BOT = "github-actions[bot]";

  /**
   * Reads the workflow's issue list {@code [{number, state, closedBy, body}]}. An issue whose marker is missing or invalid is skipped with
   * one warning line on {@code warn}; a file that is not a JSON array is an error, as it is the workflow's own output.
   */
  public static List<TrackedIssue> readAll(final Path issuesFile, final PrintStream warn) {
    final JSONArray array;
    try {
      array = new JSONArray(Files.readString(issuesFile, StandardCharsets.UTF_8));
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    } catch (final RuntimeException e) {
      throw new IllegalArgumentException("Issues file " + issuesFile + " is not a JSON array", e);
    }

    final List<TrackedIssue> result = new ArrayList<>();
    for (int i = 0; i < array.length(); i++) {
      final JSONObject item;
      try {
        item = array.getJSONObject(i);
      } catch (final RuntimeException e) {
        warn.println("Skipping entry " + i + " of the issues file: not an object");
        continue;
      }
      final int number = item.getInt("number", -1);
      final String body = item.getString("body", null);
      final Marker marker = Marker.parse(body);
      if (marker == null) {
        warn.println("Skipping issue #" + number + ": " + (Marker.hasMarker(body) ? "invalid marker" : "no marker") + " in its body");
        continue;
      }
      final boolean open = !"closed".equals(item.getString("state", "open"));
      final String closedBy = item.getString("closedBy", null);
      result.add(new TrackedIssue(number, open, !open && closedBy != null && !BOT.equals(closedBy), marker));
    }
    return result;
  }
}
