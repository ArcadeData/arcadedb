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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Turns the judged lines and the tracked issues into a plan of GitHub actions: {@code create}, {@code update} (title and body),
 * {@code comment} (one per issue and run, keyed by the run so the workflow upserts it) and {@code close} (carrying its last comment).
 * The plan is a pure function of its inputs, and the state it needs next time is in the markers it writes, so a rerun over the same
 * data plans nothing. For each issue the comments and the close come before the update that moves its marker, so an effect that failed is
 * planned again; an open issue whose lines have all recovered already is closed even without new data.
 */
public final class Planner {
  static final String LABEL = "performance-regression";

  private static final Comparator<Point> BY_TIME = Comparator.comparing(Point::t).thenComparing(Point::runAt).thenComparing(Point::ref);

  private Planner() {
  }

  public static JSONObject plan(final Map<LineKey, List<Point>> lines, final List<TrackedIssue> issues, final BenchChanges bench) {
    final List<Verdict> verdicts = Judge.judge(lines);
    final Map<String, LineKey> keysById = new HashMap<>();
    for (final LineKey key : lines.keySet())
      keysById.put(key.id(), key);

    final List<TrackedIssue> sorted = new ArrayList<>(issues);
    sorted.sort(Comparator.comparingInt(TrackedIssue::number));

    // working copies of the open issues' markers, so the input is never mutated
    final Map<Integer, Marker> markers = new TreeMap<>();
    final Map<Integer, String> rendered = new HashMap<>();
    final Map<String, Integer> openByLine = new HashMap<>();
    final Map<String, Integer> openByRun = new HashMap<>();
    final Map<String, Set<String>> accepted = new HashMap<>();
    final Map<String, Set<String>> reportedByBot = new HashMap<>();
    for (final TrackedIssue issue : sorted) {
      final Marker marker = issue.marker();
      if (issue.open()) {
        final Marker copy = Marker.parse(marker.render());
        markers.put(issue.number(), copy);
        rendered.put(issue.number(), copy.render());
        openByRun.putIfAbsent(runKey(copy.getTrigger(), copy.getRef()), issue.number());
        for (final String id : copy.lineIds())
          openByLine.putIfAbsent(lineKey(copy.getTrigger(), id), issue.number());
      } else if (issue.closedByMaintainer())
        for (final String id : marker.lineIds())
          accepted.computeIfAbsent(lineKey(marker.getTrigger(), id), k -> new HashSet<>()).add(marker.line(id).getString("baseRef", ""));
      else
        // closed by the bot after recovering: the run it last followed is reported, even if that run is worse than the one before it
        for (final String id : marker.lineIds())
          reportedByBot.computeIfAbsent(lineKey(marker.getTrigger(), id), k -> new HashSet<>()).add(marker.line(id).getString("lastRun", ""));
    }

    // the refs each issue gets a comment for, with the time of the run so the newest comes last
    final Map<Integer, Map<String, Point>> runsByIssue = new HashMap<>();

    // lines already tracked by an open issue: follow them against the recorded baseline
    for (final Map.Entry<Integer, Marker> e : markers.entrySet()) {
      final int number = e.getKey();
      final Marker marker = e.getValue();
      for (final String id : new ArrayList<>(marker.lineIds())) {
        if (openByLine.get(lineKey(marker.getTrigger(), id)) != number)
          continue;
        final LineKey key = keysById.get(id);
        if (key == null)
          continue;
        final Point newest = Judge.newest(lines.get(key), marker.getTrigger());
        final JSONObject entry = marker.line(id);
        if (newest == null || newest.ref().equals(entry.getString("lastRun", null)))
          continue;
        PlanEntries.follow(entry, newest, LineKey.STATUS.equals(key.metric()));
        addRun(runsByIssue, number, newest);
      }
    }

    // worse lines nobody tracks: the events of their runs
    final Map<String, List<Verdict>> events = new TreeMap<>();
    for (final Verdict v : verdicts) {
      if (v.kind() != Verdict.Kind.WORSE)
        continue;
      final String tracked = lineKey(v.trigger(), v.key().id());
      if (openByLine.containsKey(tracked))
        continue;
      final Set<String> acceptedBases = accepted.get(tracked);
      if (acceptedBases != null && acceptedBases.contains(v.baseline().ref()))
        continue;
      final Set<String> botRuns = reportedByBot.get(tracked);
      if (botRuns != null && botRuns.contains(v.newest().ref()))
        continue;
      events.computeIfAbsent(runKey(v.trigger(), v.newest().ref()), k -> new ArrayList<>()).add(v);
    }

    final JSONArray actions = new JSONArray();
    for (final Map.Entry<String, List<Verdict>> event : events.entrySet()) {
      final List<Verdict> worse = event.getValue();
      final Integer existing = openByRun.get(event.getKey());
      if (existing != null) {
        final Marker marker = markers.get(existing);
        for (final Verdict v : worse)
          marker.putLine(v.key().id(), PlanEntries.reported(v, true));
        addRun(runsByIssue, existing, worse.getFirst().newest());
        continue;
      }
      final Point newest = worse.getFirst().newest();
      final Marker marker = Marker.create(newest.trigger(), newest.ref(), PlanEntries.eventBaseRef(worse));
      for (final Verdict v : worse)
        marker.putLine(v.key().id(), PlanEntries.reported(v, false));
      actions.put(new JSONObject().put("op", "create").put("key", event.getKey()).put("title", IssueText.title(marker))
          .put("body", IssueText.body(marker, bench)));
    }

    // per issue the comments and the close come before the update: the update moves the marker, so an effect that failed is planned again
    // next time. A comment is upserted by its run key and a close by its comment's, so planning one twice never doubles it.
    for (final Map.Entry<Integer, Marker> e : markers.entrySet()) {
      final int number = e.getKey();
      final Marker marker = e.getValue();
      final boolean close = PlanEntries.allRecovered(marker);
      final Map<String, Point> runs = runsByIssue.get(number);
      if (runs != null) {
        final List<Point> ordered = new ArrayList<>(runs.values());
        ordered.sort(BY_TIME);
        for (int i = 0; i < ordered.size(); i++) {
          final String ref = ordered.get(i).ref();
          final List<String> rows = IssueText.runRows(marker, ref);
          if (close && i == ordered.size() - 1)
            actions.put(close(number, marker, ref, verdicts, openByLine.keySet()));
          else if (!rows.isEmpty())
            actions.put(new JSONObject().put("op", "comment").put("issue", number).put("runKey", runKey(marker.getTrigger(), ref))
                .put("body", IssueText.runComment(runKey(marker.getTrigger(), ref), rows, improvements(verdicts, marker, ref, openByLine.keySet()),
                    null)));
        }
      } else if (close)
        // every line had recovered already: the close planned with the run that completed the streaks did not happen
        actions.put(close(number, marker, closingRef(marker, lines, keysById), verdicts, openByLine.keySet()));
      if (!marker.render().equals(rendered.get(number)))
        actions.put(new JSONObject().put("op", "update").put("issue", number).put("title", IssueText.title(marker))
            .put("body", IssueText.body(marker, bench)));
    }
    return new JSONObject().put("label", LABEL).put("actions", actions);
  }

  private static JSONObject close(final int number, final Marker marker, final String ref, final List<Verdict> verdicts,
      final Set<String> tracked) {
    final String key = runKey(marker.getTrigger(), ref);
    return new JSONObject().put("op", "close").put("issue", number).put("body",
        IssueText.runComment(key, IssueText.runRows(marker, ref), improvements(verdicts, marker, ref, tracked),
            IssueText.closing(marker.getTrigger())));
  }

  /** The newest run any line of the marker was last followed to: the newest of their points, or the greatest ref without data. */
  private static String closingRef(final Marker marker, final Map<LineKey, List<Point>> lines, final Map<String, LineKey> keysById) {
    Point newest = null;
    String greatest = null;
    for (final String id : marker.lineIds()) {
      final String lastRun = marker.line(id).getString("lastRun", null);
      if (greatest == null || lastRun.compareTo(greatest) > 0)
        greatest = lastRun;
      final LineKey key = keysById.get(id);
      final Point point = key == null ? null : Judge.newest(lines.get(key), marker.getTrigger());
      if (point != null && point.ref().equals(lastRun) && (newest == null || BY_TIME.compare(point, newest) > 0))
        newest = point;
    }
    return newest != null ? newest.ref() : greatest;
  }

  /** Lines of the run that improved on their own baseline and that no open issue tracks, in line order. */
  private static List<String> improvements(final List<Verdict> verdicts, final Marker marker, final String ref, final Set<String> tracked) {
    final String trigger = marker.getTrigger();
    final List<String> rows = new ArrayList<>();
    for (final Verdict v : verdicts)
      if (v.kind() == Verdict.Kind.BETTER && v.trigger().equals(trigger) && v.newest().ref().equals(ref) && !tracked.contains(
          lineKey(trigger, v.key().id())))
        rows.add(IssueText.improvementRow(v));
    return Collections.unmodifiableList(rows);
  }

  private static void addRun(final Map<Integer, Map<String, Point>> runsByIssue, final int number, final Point newest) {
    runsByIssue.computeIfAbsent(number, k -> new HashMap<>()).merge(newest.ref(), newest,
        (a, b) -> a.runAt().compareTo(b.runAt()) >= 0 ? a : b);
  }

  static String runKey(final String trigger, final String ref) {
    return trigger + ":" + ref;
  }

  private static String lineKey(final String trigger, final String lineId) {
    return trigger + "|" + lineId;
  }
}
