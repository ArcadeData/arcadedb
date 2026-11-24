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
package com.arcadedb.bench.model;

import com.arcadedb.bench.model.CaseResult.Status;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RunResultTest {
  private static JSONObject sample() throws IOException {
    try (final InputStream in = RunResultTest.class.getResourceAsStream("/run-sample.json")) {
      return new JSONObject(new String(in.readAllBytes(), StandardCharsets.UTF_8));
    }
  }

  @Test
  void roundTripPreservesEverything() throws IOException {
    final RunResult first = RunResult.fromJSON(sample());
    final RunResult second = RunResult.fromJSON(new JSONObject(first.toJSON().toString()));

    assertThat(first.results()).hasSize(2);
    assertThat(first.results().get(0).metrics().get(0).values()).hasSize(5);
    assertThat(second.info()).isEqualTo(first.info());
    assertThat(second.results()).hasSameSizeAs(first.results());
    for (int i = 0; i < first.results().size(); i++) {
      final CaseResult a = first.results().get(i);
      final CaseResult b = second.results().get(i);
      assertThat(b.workload()).isEqualTo(a.workload());
      assertThat(b.caseName()).isEqualTo(a.caseName());
      assertThat(b.scale()).isEqualTo(a.scale());
      assertThat(b.params()).isEqualTo(a.params());
      assertThat(b.reps()).isEqualTo(a.reps());
      assertThat(b.warmup()).isEqualTo(a.warmup());
      assertThat(b.status()).isEqualTo(a.status());
      assertThat(b.error()).isEqualTo(a.error());
      assertThat(b.metrics()).isEqualTo(a.metrics());
    }
    assertThat(second.toJSON()).isEqualTo(first.toJSON());
  }

  @Test
  void writeAndReadBack(@TempDir final Path dir) throws IOException {
    final RunResult run = RunResult.fromJSON(sample());
    run.write(dir);
    final Path file = dir.resolve(run.info().runId() + ".json");
    assertThat(Files.exists(file)).isTrue();
    assertThat(RunResult.read(file).toJSON()).isEqualTo(run.toJSON());
  }

  @Test
  void failedCaseKeepsStatusAndError() {
    final CaseResult failed = new CaseResult("graph", "traverse", "M", new JSONObject(), 5, 2, List.of(), Status.FAILED,
        "OOM");
    final String json = failed.toJSON().toString();
    assertThat(json).contains("\"status\":\"failed\"", "\"error\":\"OOM\"", "\"metrics\":[]");
    assertThat(CaseResult.fromJSON(new JSONObject(json)).status()).isEqualTo(Status.FAILED);
  }

  @Test
  void unknownVersionIsRefused() throws IOException {
    final JSONObject json = sample().put("schemaVersion", 3);
    assertThatThrownBy(() -> RunResult.fromJSON(json)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("3");
  }

  @Test
  void v2RoundTripKeepsNotesHistogramsCompleteAndDiagnostics() throws IOException {
    final RunResult base = RunResult.fromJSON(sample());
    final CaseResult first = base.results().get(0);
    final CaseResult withExtras = new CaseResult(first.workload(), first.caseName(), first.scale(), first.params(), first.reps(),
        first.warmup(), first.metrics(), first.status(), first.error(), new JSONObject().put("serverDied", "oom"),
        new JSONArray().put("AAAA").put("BBBB"));
    final RunResult run = new RunResult(base.info(), List.of(withExtras), false,
        new JSONObject("{\"ingestion\":{\"threads\":42}}"));

    final RunResult back = RunResult.fromJSON(new JSONObject(run.toJSON().toString()));

    assertThat(run.toJSON().getInt("schemaVersion")).isEqualTo(2);
    assertThat(back.complete()).isFalse();
    assertThat(back.diagnostics().getJSONObject("ingestion").getInt("threads")).isEqualTo(42);
    final CaseResult c = back.results().get(0);
    assertThat(c.notes().getString("serverDied")).isEqualTo("oom");
    assertThat(c.histograms().length()).isEqualTo(2);
    assertThat(c.histograms().getString(0)).isEqualTo("AAAA");
    assertThat(c.histograms().getString(1)).isEqualTo("BBBB");
  }

  @Test
  void v1FileStillReads() throws IOException {
    final RunResult run = RunResult.fromJSON(sample().put("schemaVersion", 1));
    assertThat(run.complete()).isTrue();
    assertThat(run.diagnostics().isEmpty()).isTrue();
    assertThat(run.results().get(0).notes().isEmpty()).isTrue();
    assertThat(run.results().get(0).histograms().length()).isZero();
  }

  @Test
  void emptyNotesAndHistogramsAreOmitted() throws IOException {
    final JSONObject json = RunResult.fromJSON(sample()).results().get(0).toJSON();
    assertThat(json.has("notes")).isFalse();
    assertThat(json.has("histograms")).isFalse();
  }

  @Test
  void withNoteAndWithStatusReturnCopies() {
    final CaseResult c = new CaseResult("graph", "traverse", "M", new JSONObject(), 5, 2, List.of(), Status.OK, null);
    final CaseResult noted = c.withNote("stallDumps", 2).withStatus(Status.FAILED, "boom");
    assertThat(c.notes().isEmpty()).isTrue();
    assertThat(c.status()).isEqualTo(Status.OK);
    assertThat(noted.notes().getInt("stallDumps")).isEqualTo(2);
    assertThat(noted.status()).isEqualTo(Status.FAILED);
    assertThat(noted.error()).isEqualTo("boom");
  }

  @Test
  void runIdFormat() {
    assertThat(RunInfo.newRunId(Instant.parse("2026-11-17T03:00:00Z"), "1e1ad51768", "jvm")).isEqualTo(
        "2026-11-17T03-00-00Z-1e1ad51768-jvm");
  }

  @Test
  void runIdKeepsStartsInTheSameSecondApart() {
    final String a = RunInfo.newRunId(Instant.parse("2026-11-17T03:00:00.100Z"), "1e1ad51768", "jvm");
    final String b = RunInfo.newRunId(Instant.parse("2026-11-17T03:00:00.900Z"), "1e1ad51768", "jvm");
    assertThat(a).isEqualTo("2026-11-17T03-00-00.100Z-1e1ad51768-jvm");
    assertThat(b).isNotEqualTo(a);
  }
}
