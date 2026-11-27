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
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The plan the gate produces over a copy of real published runs of arcadedb-perf: the 2026-11-17 nightly of the older file shape, the
 * nightlies of ed6090eaa6 and 0f8f74e803, and the release runs of 26.10.2 and 26.11.1. The pinned plan was checked by hand against the
 * raw numbers; a change to the rules that moves it must be checked again, not just re-pinned.
 */
class GateFixtureTest {
  private static final BenchChanges NO_BENCH = BenchChanges.read(Path.of("does-not-exist.json"));

  private static Path resource(final String name) throws URISyntaxException {
    return Path.of(GateFixtureTest.class.getResource("/gate/" + name).toURI());
  }

  private static Map<LineKey, List<Point>> lines() throws URISyntaxException {
    final ByteArrayOutputStream warnings = new ByteArrayOutputStream();
    final Map<LineKey, List<Point>> lines = RawData.read(resource("data"), new PrintStream(warnings, true, StandardCharsets.UTF_8));
    assertThat(warnings.toString(StandardCharsets.UTF_8)).isEmpty();
    return lines;
  }

  @Test
  void realDataPlanIsPinned() throws Exception {
    final JSONObject plan = Planner.plan(lines(), List.of(), NO_BENCH);

    assertThat(plan.toString(2)).isEqualTo(Files.readString(resource("expected-plan.json"), StandardCharsets.UTF_8).strip());
  }

  @Test
  void realDataRerunIsEmpty() throws Exception {
    final Map<LineKey, List<Point>> lines = lines();
    final JSONArray created = Planner.plan(lines, List.of(), NO_BENCH).getJSONArray("actions");
    final List<TrackedIssue> issues = new ArrayList<>();
    for (int i = 0; i < created.length(); i++) {
      final JSONObject action = created.getJSONObject(i);
      assertThat(action.getString("op")).isEqualTo("create");
      issues.add(new TrackedIssue(i + 1, true, false, Marker.parse(action.getString("body"))));
    }
    assertThat(issues).isNotEmpty();

    final JSONObject rerun = Planner.plan(lines, issues, NO_BENCH);

    assertThat(rerun.getJSONArray("actions").length()).isZero();
  }
}
