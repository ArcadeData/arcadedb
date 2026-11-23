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
package com.arcadedb.bench.store;

import com.arcadedb.bench.model.CaseResult;
import com.arcadedb.bench.model.Metric;
import com.arcadedb.bench.model.RunInfo;
import com.arcadedb.bench.model.RunResult;
import com.arcadedb.bench.server.ServerUnderTest;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteHttpComponent;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Runs {@link ResultsStore} and {@link Uploader} against a real ArcadeDB container.
 */
class UploaderIT {
  private static final String WRITER_PASSWORD = "writer-secret-1";
  private static final String READER_PASSWORD = "reader-secret-1";

  /** Server wording of a permission refusal, e.g. User 'perf-reader' is not allowed to delete records on type 'Run'. */
  private static final String DENIED = "is not allowed to";

  private static ServerUnderTest server;
  private static String          url;

  @BeforeAll
  static void start() {
    server = ServerUnderTest.start(System.getProperty("arcadedb.bench.image", "arcadedata/arcadedb:latest"), "", 4, "1g", Map.of());
    url = server.httpUrl();
    ResultsStore.init(url, "root", ServerUnderTest.PASSWORD, WRITER_PASSWORD, READER_PASSWORD);
  }

  @AfterAll
  static void stop() {
    if (server != null)
      server.close();
  }

  @BeforeEach
  void clean() {
    try (final RemoteDatabase db = open("root", ServerUnderTest.PASSWORD)) {
      db.command("sql", "DELETE FROM Measurement");
      db.command("sql", "DELETE FROM Run");
    }
  }

  private static RemoteDatabase open(final String user, final String password) {
    final RemoteDatabase db = new RemoteDatabase(server.wrapper().host(), server.wrapper().httpPort(), "perf", user, password);
    db.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.FIXED);
    return db;
  }

  private static long count(final String type) {
    try (final RemoteDatabase db = open("root", ServerUnderTest.PASSWORD)) {
      try (final ResultSet rs = db.query("sql", "SELECT count(*) AS c FROM " + type)) {
        return ((Number) rs.next().getProperty("c")).longValue();
      }
    }
  }

  private static long expectedMeasurements(final RunResult run) {
    long n = 0;
    for (final CaseResult c : run.results())
      n += Math.max(1, c.metrics().size());
    return n;
  }

  private static RunResult withMedian(final RunResult run, final double median) {
    final List<CaseResult> cases = new ArrayList<>(run.results());
    final CaseResult first = cases.getFirst();
    final List<Metric> metrics = new ArrayList<>(first.metrics());
    final Metric m = metrics.getFirst();
    metrics.set(0, new Metric(m.name(), m.unit(), m.better(), median, m.values()));
    cases.set(0, new CaseResult(first.workload(), first.caseName(), first.scale(), first.params(), first.reps(), first.warmup(),
        metrics, first.status(), first.error()));
    return new RunResult(run.info(), cases);
  }

  @Test
  void initIsIdempotent() {
    ResultsStore.init(url, "root", ServerUnderTest.PASSWORD, WRITER_PASSWORD, READER_PASSWORD);
    try (final RemoteDatabase db = open("root", ServerUnderTest.PASSWORD)) {
      try (final ResultSet rs = db.query("sql", "SELECT name FROM schema:types")) {
        final List<String> names = new ArrayList<>();
        while (rs.hasNext())
          names.add(rs.next().getProperty("name"));
        assertThat(names).contains("Run", "Measurement");
      }
    }
  }

  @Test
  void uploadInsertsRows() {
    final RunResult run = ExporterIT.sample();
    Uploader.upload(url, "perf-writer", WRITER_PASSWORD, run);
    assertThat(count("Run")).isEqualTo(1);
    assertThat(count("Measurement")).isEqualTo(expectedMeasurements(run)).isEqualTo(3);
    try (final RemoteDatabase db = open("perf-reader", READER_PASSWORD)) {
      try (final ResultSet rs = db.query("sql", "SELECT FROM Measurement WHERE metric = 'throughput'")) {
        final Result r = rs.next();
        assertThat((String) r.getProperty("machineId")).isEqualTo("perf-01");
        assertThat((String) r.getProperty("variant")).isEqualTo("jvm");
        assertThat((String) r.getProperty("commit")).isEqualTo("1e1ad51768");
        assertThat((String) r.getProperty("case")).isEqualTo("knn-k10");
        assertThat(((Number) r.getProperty("median")).doubleValue()).isEqualTo(1234.5);
        assertThat((List<?>) r.getProperty("values")).hasSize(5);
      }
    }
  }

  @Test
  void reuploadReplacesRun() {
    final RunResult run = ExporterIT.sample();
    Uploader.upload(url, "perf-writer", WRITER_PASSWORD, run);
    Uploader.upload(url, "perf-writer", WRITER_PASSWORD, withMedian(run, 999.0));
    assertThat(count("Run")).isEqualTo(1);
    assertThat(count("Measurement")).isEqualTo(3);
    try (final RemoteDatabase db = open("root", ServerUnderTest.PASSWORD)) {
      try (final ResultSet rs = db.query("sql", "SELECT median FROM Measurement WHERE metric = 'throughput'")) {
        assertThat(((Number) rs.next().getProperty("median")).doubleValue()).isEqualTo(999.0);
        assertThat(rs.hasNext()).isFalse();
      }
    }
  }

  @Test
  void failedUploadLeavesPreviousCopy() {
    final RunResult run = ExporterIT.sample();
    Uploader.upload(url, "perf-writer", WRITER_PASSWORD, run);

    // the last case has no workload name: Measurement.workload is mandatory and not null, so the insert of its row fails
    // after the delete and the earlier inserts of the same transaction
    final List<CaseResult> cases = new ArrayList<>(withMedian(run, 999.0).results());
    final CaseResult last = cases.getLast();
    cases.set(cases.size() - 1, new CaseResult(null, last.caseName(), last.scale(), last.params(), last.reps(), last.warmup(),
        last.metrics(), last.status(), last.error()));
    final RunResult broken = new RunResult(run.info(), cases);

    assertThatThrownBy(() -> Uploader.upload(url, "perf-writer", WRITER_PASSWORD, broken)).isInstanceOf(RuntimeException.class)
        .hasMessageContaining("workload");

    assertThat(count("Run")).isEqualTo(1);
    assertThat(count("Measurement")).isEqualTo(3);
    try (final RemoteDatabase db = open("root", ServerUnderTest.PASSWORD)) {
      try (final ResultSet rs = db.query("sql", "SELECT median FROM Measurement WHERE metric = 'throughput'")) {
        assertThat(((Number) rs.next().getProperty("median")).doubleValue()).isEqualTo(1234.5);
      }
    }
  }

  @Test
  void writerCannotDropType() {
    try (final RemoteDatabase db = open("perf-writer", WRITER_PASSWORD)) {
      assertThatThrownBy(() -> db.command("sql", "DROP TYPE Run")).isInstanceOf(RuntimeException.class)
          .hasMessageContaining(DENIED);
    }
    assertThat(count("Run")).isZero();
    try (final RemoteDatabase db = open("root", ServerUnderTest.PASSWORD)) {
      assertThat(db.getSchema().existsType("Run")).isTrue();
    }
  }

  @Test
  void readerCannotDelete() {
    Uploader.upload(url, "perf-writer", WRITER_PASSWORD, ExporterIT.sample());
    try (final RemoteDatabase db = open("perf-reader", READER_PASSWORD)) {
      assertThatThrownBy(() -> db.command("sql", "DELETE FROM Run")).isInstanceOf(RuntimeException.class)
          .hasMessageContaining(DENIED);
    }
    assertThat(count("Run")).isEqualTo(1);
  }

  @Test
  void failedCaseVisible() {
    Uploader.upload(url, "perf-writer", WRITER_PASSWORD, ExporterIT.sample());
    try (final RemoteDatabase db = open("perf-reader", READER_PASSWORD)) {
      try (final ResultSet rs = db.query("sql", "SELECT FROM Measurement WHERE metric = '_status'")) {
        final Result r = rs.next();
        assertThat((String) r.getProperty("status")).isEqualTo("timeout");
        assertThat((String) r.getProperty("case")).isEqualTo("traverse");
        assertThat((Object) r.getProperty("median")).isNull();
        assertThat(rs.hasNext()).isFalse();
      }
    }
  }

  private static void assertDenied(final String user, final String password, final String sql) {
    try (final RemoteDatabase db = open(user, password)) {
      assertThatThrownBy(() -> db.command("sql", sql)).isInstanceOf(RuntimeException.class).hasMessageContaining("User '" + user + "' " + DENIED);
    }
  }

  @Test
  void writerCannotUpdateRecord() {
    Uploader.upload(url, "perf-writer", WRITER_PASSWORD, ExporterIT.sample());
    assertDenied("perf-writer", WRITER_PASSWORD, "UPDATE Run SET `trigger` = 'x'");
    try (final RemoteDatabase db = open("root", ServerUnderTest.PASSWORD)) {
      try (final ResultSet rs = db.query("sql", "SELECT FROM Run WHERE `trigger` = 'x'")) {
        assertThat(rs.hasNext()).isFalse();
      }
    }
  }

  @Test
  void writerCannotCreateType() {
    assertDenied("perf-writer", WRITER_PASSWORD, "CREATE DOCUMENT TYPE X");
  }

  @Test
  void writerCannotCreateIndex() {
    assertDenied("perf-writer", WRITER_PASSWORD, "CREATE INDEX IF NOT EXISTS ON Run (source) NOTUNIQUE");
  }

  @Test
  void writerCannotReadOtherType() {
    try (final RemoteDatabase db = open("root", ServerUnderTest.PASSWORD)) {
      db.command("sql", "CREATE DOCUMENT TYPE Secret IF NOT EXISTS").close();
      db.command("sql", "INSERT INTO Secret SET a = 1").close();
    }
    assertDenied("perf-writer", WRITER_PASSWORD, "SELECT FROM Secret");
    assertDenied("perf-reader", READER_PASSWORD, "SELECT FROM Secret");
  }

  @Test
  void readerCannotInsert() {
    assertDenied("perf-reader", READER_PASSWORD, "INSERT INTO Run SET runId = 'x'");
    assertThat(count("Run")).isZero();
  }

  @Test
  void mapFieldsReadBack() {
    Uploader.upload(url, "perf-writer", WRITER_PASSWORD, ExporterIT.sample());
    try (final RemoteDatabase db = open("perf-reader", READER_PASSWORD)) {
      try (final ResultSet rs = db.query("sql", "SELECT FROM Run")) {
        final Result r = rs.next();
        assertThat((Map<String, Object>) r.getProperty("arcadedb")).containsEntry("commit", "1e1ad51768").containsEntry("variant", "jvm")
            .containsKeys("version", "image", "imageDigest", "revision");
        assertThat((Map<String, Object>) r.getProperty("machine")).containsEntry("id", "perf-01").containsKeys("cpu", "cores", "os");
        assertThat((Map<String, Object>) r.getProperty("config")).containsEntry("heap", "8g").containsKeys("settings", "jvm");
      }
    }
  }

  /** The sample run as one run of a release: its own runId, trigger, tag, variant, source and scale for every case. */
  private static RunResult run(final String runId, final String trigger, final String tag, final String variant, final String source,
      final String scale) {
    final RunResult base = ExporterIT.sample();
    final JSONObject arcadedb = new JSONObject(base.info().arcadedb().toString()).put("variant", variant);
    if (tag != null)
      arcadedb.put("tag", tag);
    final Instant startedAt = Instant.parse("2026-11-20T19:18:00Z");
    final RunInfo info = new RunInfo(runId, startedAt, startedAt.plus(Duration.ofMinutes(40)), trigger, source, arcadedb,
        base.info().machine(), base.info().config());
    final List<CaseResult> cases = new ArrayList<>();
    for (final CaseResult c : base.results())
      cases.add(new CaseResult(c.workload(), c.caseName(), scale, c.params(), c.reps(), c.warmup(), c.metrics(), c.status(), c.error()));
    return new RunResult(info, cases);
  }

  private static RunResult release(final String runId, final String variant, final String scale) {
    return run(runId, "release", "26.11.1", variant, "bench", scale);
  }

  private static List<String> runIds() {
    try (final RemoteDatabase db = open("root", ServerUnderTest.PASSWORD)) {
      try (final ResultSet rs = db.query("sql", "SELECT runId FROM Run ORDER BY runId")) {
        final List<String> ids = new ArrayList<>();
        while (rs.hasNext())
          ids.add(rs.next().getProperty("runId"));
        return ids;
      }
    }
  }

  private static List<String> measurementRunIds() {
    try (final RemoteDatabase db = open("root", ServerUnderTest.PASSWORD)) {
      try (final ResultSet rs = db.query("sql", "SELECT DISTINCT(runId) AS runId FROM Measurement")) {
        final List<String> ids = new ArrayList<>();
        while (rs.hasNext())
          ids.add(rs.next().getProperty("runId"));
        return ids;
      }
    }
  }

  @Test
  void aReplacingUploadRemovesTheEarlierRunOfTheSameReleaseScaleAndVariant() {
    Uploader.upload(url, "perf-writer", WRITER_PASSWORD, release("old-m-jvm", "jvm", "M"));

    final List<String> replaced = Uploader.upload(url, "perf-writer", WRITER_PASSWORD, release("new-m-jvm", "jvm", "M"), true);

    assertThat(replaced).containsExactly("old-m-jvm");
    assertThat(runIds()).containsExactly("new-m-jvm");
    assertThat(measurementRunIds()).containsExactly("new-m-jvm");
  }

  @Test
  void aReplacingUploadKeepsOtherScalesVariantsSourcesTagsAndTriggers() {
    Uploader.upload(url, "perf-writer", WRITER_PASSWORD, release("old-l-jvm", "jvm", "L"));
    Uploader.upload(url, "perf-writer", WRITER_PASSWORD, release("old-m-native", "native", "M"));
    Uploader.upload(url, "perf-writer", WRITER_PASSWORD, run("old-ldbc", "release", "26.11.1", "jvm", "ldbc", "M"));
    Uploader.upload(url, "perf-writer", WRITER_PASSWORD, run("old-other-tag", "release", "26.10.2", "jvm", "bench", "M"));
    Uploader.upload(url, "perf-writer", WRITER_PASSWORD, run("old-nightly", "nightly", "26.11.1", "jvm", "bench", "M"));

    final List<String> replaced = Uploader.upload(url, "perf-writer", WRITER_PASSWORD, release("new-m-jvm", "jvm", "M"), true);

    assertThat(replaced).isEmpty();
    assertThat(runIds()).containsExactly("new-m-jvm", "old-l-jvm", "old-ldbc", "old-m-native", "old-nightly", "old-other-tag");
  }

  @Test
  void anUploadThatDoesNotReplaceKeepsBothRunsOfTheRelease() {
    Uploader.upload(url, "perf-writer", WRITER_PASSWORD, release("old-m-jvm", "jvm", "M"));

    assertThat(Uploader.upload(url, "perf-writer", WRITER_PASSWORD, release("new-m-jvm", "jvm", "M"), false)).isEmpty();

    assertThat(runIds()).containsExactly("new-m-jvm", "old-m-jvm");
  }

  @Test
  void onlyAReleaseRunWithATagCanReplace() {
    Uploader.upload(url, "perf-writer", WRITER_PASSWORD, release("old-m-jvm", "jvm", "M"));

    assertThatIllegalArgumentException().isThrownBy(
            () -> Uploader.upload(url, "perf-writer", WRITER_PASSWORD, run("n", "nightly", "26.11.1", "jvm", "bench", "M"), true))
        .withMessageContaining("release");
    assertThatIllegalArgumentException().isThrownBy(
            () -> Uploader.upload(url, "perf-writer", WRITER_PASSWORD, run("u", "release", null, "jvm", "bench", "M"), true))
        .withMessageContaining("tag");

    assertThat(runIds()).containsExactly("old-m-jvm");
  }

  @Test
  void aFailedReplacingUploadKeepsTheEarlierRun() {
    Uploader.upload(url, "perf-writer", WRITER_PASSWORD, release("old-m-jvm", "jvm", "M"));

    // the last case has no workload name: its insert fails after the replaced run was deleted in the same transaction
    final RunResult fresh = release("new-m-jvm", "jvm", "M");
    final List<CaseResult> cases = new ArrayList<>(fresh.results());
    final CaseResult last = cases.getLast();
    cases.set(cases.size() - 1, new CaseResult(null, last.caseName(), last.scale(), last.params(), last.reps(), last.warmup(),
        last.metrics(), last.status(), last.error()));

    assertThatThrownBy(() -> Uploader.upload(url, "perf-writer", WRITER_PASSWORD, new RunResult(fresh.info(), cases), true))
        .isInstanceOf(RuntimeException.class).hasMessageContaining("workload");

    assertThat(runIds()).containsExactly("old-m-jvm");
    assertThat(measurementRunIds()).containsExactly("old-m-jvm");
  }
}
