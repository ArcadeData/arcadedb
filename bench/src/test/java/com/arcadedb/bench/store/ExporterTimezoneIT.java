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

import com.arcadedb.bench.model.RunInfo;
import com.arcadedb.bench.model.RunResult;
import com.arcadedb.bench.server.ServerUnderTest;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteHttpComponent;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The exported {@code t} must be the uploaded instant exactly, whatever time zone the results server runs in.
 */
class ExporterTimezoneIT {
  private static final String WRITER_PASSWORD = "writer-secret-1";
  private static final String READER_PASSWORD = "reader-secret-1";

  @TempDir
  Path out;

  @Test
  void exportedInstantIsExactInANonUtcServerZone() throws IOException {
    // +05:30 in November, no DST: any assumption of UTC or of whole hours shifts the result
    try (final ServerUnderTest server = ServerUnderTest.start(System.getProperty("arcadedb.bench.image", "arcadedata/arcadedb:latest"),
        "", 4, "1g", Map.of("TZ", "Asia/Kolkata"))) {
      final String url = server.httpUrl();
      ResultsStore.init(url, "root", ServerUnderTest.PASSWORD, WRITER_PASSWORD, READER_PASSWORD);
      final Instant at = Instant.parse("2026-11-17T03:07:09Z");

      // prove the server JVM really is in the non-UTC zone, or the test would pass vacuously if the image ignored TZ
      try (final RemoteDatabase db = new RemoteDatabase(server.wrapper().host(), server.wrapper().httpPort(), ResultsStore.DATABASE,
          "root", ServerUnderTest.PASSWORD)) {
        db.setConnectionStrategy(RemoteHttpComponent.CONNECTION_STRATEGY.FIXED);
        try (final ResultSet rs = db.query("sql", "SELECT date.systemTimezone() AS z")) {
          final String zone = rs.next().getProperty("z");
          assertThat(zone).isEqualTo("Asia/Kolkata");
          assertThat(ZoneId.of(zone).getRules().getOffset(at)).isEqualTo(ZoneOffset.ofHoursMinutes(5, 30));
        }
      }

      final RunResult base = ExporterIT.sample();
      final RunInfo info = new RunInfo("tz1", at, at.plusSeconds(60), base.info().trigger(), base.info().source(),
          base.info().arcadedb(), base.info().machine(), base.info().config());
      Uploader.upload(url, "perf-writer", WRITER_PASSWORD, new RunResult(info, base.results()));
      Exporter.export(url, "perf-reader", READER_PASSWORD, out);
      final JSONObject s = new JSONObject(Files.readString(out.resolve("series/vector/knn-k10__M__throughput.json")));
      final String t = s.getJSONArray("lines").getJSONObject(0).getJSONArray("points").getJSONObject(0).getString("t");
      assertThat(Instant.parse(t)).isEqualTo(at);
    }
  }
}
