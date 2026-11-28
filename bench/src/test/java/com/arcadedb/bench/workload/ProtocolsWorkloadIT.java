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
package com.arcadedb.bench.workload;

import com.arcadedb.bench.model.CaseResult;
import com.arcadedb.bench.runner.BenchRunner;
import com.arcadedb.bench.runner.WorkloadContext;
import com.arcadedb.bench.server.ServerUnderTest;
import com.arcadedb.bench.workload.ProtocolsWorkload.Shape;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Every case of the protocols workload at XS against one server with the four protocol plugins, then the parity check proven
 * able to fail: one changed HTTP row of hop1 fails exactly the non-HTTP hop1 cases.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ProtocolsWorkloadIT {
  private static ServerUnderTest server;

  @BeforeAll
  static void start() {
    server = ServerUnderTest.start(ITSupport.IMAGE, "", 4, "2g", new ProtocolsWorkload().serverSettings());
  }

  @AfterAll
  static void stop() {
    if (server != null)
      server.close();
  }

  @Test
  @Order(1)
  void allCasesOk() {
    final ProtocolsWorkload workload = new ProtocolsWorkload();
    final List<CaseResult> results = ITSupport.run(workload, server, new JSONObject());
    assertThat(results).extracting(CaseResult::caseName).containsExactlyElementsOf(ProtocolsWorkload.caseNames(Scale.XS));
    for (final CaseResult r : results) {
      assertThat(r.status()).as(r.caseName() + ": " + r.error()).isEqualTo(CaseResult.Status.OK);
      assertThat(ITSupport.metric(r, "ops_per_s").median()).as(r.caseName()).isGreaterThan(0);
      assertThat(ITSupport.metric(r, "latency_p50_ms")).as(r.caseName()).isNotNull();
      assertThat(ITSupport.metric(r, "latency_p99_ms")).as(r.caseName()).isNotNull();
      assertThat(ITSupport.metric(r, "peak_rss_bytes")).as(r.caseName()).isNotNull();
    }
    // every write case's teardown removed what it wrote: the dataset is back to its users
    try (final RemoteDatabase db = RemoteClients.open(server, ProtocolsWorkload.DATABASE)) {
      assertThat(RemoteClients.count(db, "sql", "SELECT count(*) AS n FROM User WHERE id >= " + ProtocolsWorkload.WRITE_BASE))
          .isZero();
      assertThat(RemoteClients.count(db, "sql", "SELECT count(*) AS n FROM User")).isEqualTo(ProtocolsWorkload.users(Scale.XS));
    }
  }

  @Test
  @Order(2)
  void aChangedHttpRowFailsExactlyTheOtherProtocolsOfItsShape() {
    final ProtocolsWorkload workload = new ProtocolsWorkload();
    workload.corruptHttpReferenceOf = Shape.HOP1;
    final List<CaseResult> results = new BenchRunner(1, 0, Duration.ofMinutes(5)).run(
        workload, new WorkloadContext(server, Scale.XS, 42L, new JSONObject()),
        n -> n.startsWith("hop1-") || n.startsWith("point-"));
    assertThat(results).hasSize(16);
    for (final CaseResult r : results) {
      final boolean mustFail = r.caseName().startsWith("hop1-") && !r.caseName().startsWith("hop1-http-");
      if (mustFail) {
        assertThat(r.status()).as(r.caseName()).isEqualTo(CaseResult.Status.FAILED);
        assertThat(r.error()).as(r.caseName()).contains("parity with http failed for " + r.caseName());
      } else
        assertThat(r.status()).as(r.caseName() + ": " + r.error()).isEqualTo(CaseResult.Status.OK);
    }
  }
}
