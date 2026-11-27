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

import com.arcadedb.bench.runner.LatencyRecorder;
import com.arcadedb.bench.workload.ProtocolsWorkload.Shape;
import com.arcadedb.bench.workload.protocol.Protocol;
import com.arcadedb.bench.workload.protocol.ProtocolClient;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProtocolsWorkloadTest {
  private static final String CASE_NAME = "^(point|hop1|page1000|merge500|vector-param|tx10)-(http|grpc|bolt|pg)-(1|16)c$";

  @Test
  void caseNamesFollowTheSpecFormat() {
    for (final Scale scale : Scale.values()) {
      final List<String> names = ProtocolsWorkload.caseNames(scale);
      assertThat(names).as(scale.name()).isNotEmpty().doesNotHaveDuplicates();
      for (final String name : names)
        assertThat(name).as(scale.name()).matches(CASE_NAME);
    }
  }

  @Test
  void casesAreTheRunnersList() {
    assertThat(new ProtocolsWorkload().cases()).isEqualTo(ProtocolsWorkload.caseNames(Scale.S));
    assertThat(new ProtocolsWorkload().name()).isEqualTo("protocols");
  }

  @Test
  void casesOmitWhatAProtocolCannotCarry() {
    final List<String> all = ProtocolsWorkload.caseNames(Scale.XS, (protocol, shape) -> true);
    final List<String> without = ProtocolsWorkload.caseNames(Scale.XS,
        (protocol, shape) -> !(protocol == Protocol.BOLT && shape == Shape.POINT));
    // exactly the two names (1 and 16 clients) of the dropped pair are gone, nothing else
    assertThat(without).doesNotContain("point-bolt-1c", "point-bolt-16c");
    assertThat(all).contains("point-bolt-1c", "point-bolt-16c");
    assertThat(without).hasSize(all.size() - 2);
    final List<String> expected = new ArrayList<>(all);
    expected.removeAll(List.of("point-bolt-1c", "point-bolt-16c"));
    assertThat(without).isEqualTo(expected);
  }

  @Test
  void theRunnersListUsesTheRealCarriesRule() {
    assertThat(ProtocolsWorkload.caseNames(Scale.XS))
        .isEqualTo(ProtocolsWorkload.caseNames(Scale.XS, ProtocolsWorkload::carries));
  }

  @Test
  void everyClientServedOneQueryBeforeTheTimedOperationsStart() {
    final List<FakeClient> opened = new ArrayList<>();
    final List<ProtocolClient> clients = ProtocolsWorkload.openClients(16, () -> {
      final FakeClient client = new FakeClient();
      opened.add(client);
      return client;
    });
    assertThat(clients).hasSize(16);
    // connection setup of the lazily connecting protocols (http, grpc, bolt) must not land in the timed window
    assertThat(opened).hasSize(16).allSatisfy(client -> assertThat(client.queries.get()).isEqualTo(1));
  }

  @Test
  void aFailingWarmUpClosesTheClientsOpenedSoFar() {
    final List<FakeClient> opened = new ArrayList<>();
    assertThatThrownBy(() -> ProtocolsWorkload.openClients(4, () -> {
      final FakeClient client = new FakeClient();
      client.failQuery = opened.size() == 2;
      opened.add(client);
      return client;
    })).hasMessageContaining("warm-up failed");
    assertThat(opened).allSatisfy(client -> assertThat(client.closed).isTrue());
  }

  private static final class FakeClient implements ProtocolClient {
    final AtomicInteger queries = new AtomicInteger();
    volatile boolean    closed;
    boolean             failQuery;

    @Override
    public List<Map<String, Object>> query(final String cypher, final Map<String, Object> params) {
      if (failQuery)
        throw new IllegalStateException("warm-up failed");
      queries.incrementAndGet();
      return List.of();
    }

    @Override
    public void transaction(final List<String> cyphers, final List<Map<String, Object>> params) {
    }

    @Override
    public void close() {
      closed = true;
    }
  }

  @Test
  void everyCarriedPairIsACaseAtXs() {
    // XS sizes nothing down: every pair a protocol carries is there, at both client counts
    final List<String> names = ProtocolsWorkload.caseNames(Scale.XS);
    int carried = 0;
    for (final Shape shape : Shape.values())
      for (final Protocol protocol : Protocol.values())
        if (ProtocolsWorkload.carries(protocol, shape)) {
          carried++;
          assertThat(names).contains(shape.caseName() + "-" + protocol.caseName() + "-1c",
              shape.caseName() + "-" + protocol.caseName() + "-16c");
        }
    assertThat(names).hasSize(carried * 2);
  }

  @Test
  void httpCarriesEveryShape() {
    for (final Shape shape : Shape.values())
      assertThat(ProtocolsWorkload.carries(Protocol.HTTP, shape)).as(shape.name()).isTrue();
  }

  @Test
  void shapeCaseNames() {
    assertThat(Shape.POINT.caseName()).isEqualTo("point");
    assertThat(Shape.HOP1.caseName()).isEqualTo("hop1");
    assertThat(Shape.PAGE_1000.caseName()).isEqualTo("page1000");
    assertThat(Shape.UNWIND_MERGE_500.caseName()).isEqualTo("merge500");
    assertThat(Shape.VECTOR_PARAM.caseName()).isEqualTo("vector-param");
    assertThat(Shape.TX_10.caseName()).isEqualTo("tx10");
  }

  @Test
  void parseSplitsACaseName() {
    final ProtocolsWorkload.Case c = ProtocolsWorkload.Case.parse("vector-param-pg-16c");
    assertThat(c.shape()).isEqualTo(Shape.VECTOR_PARAM);
    assertThat(c.protocol()).isEqualTo(Protocol.POSTGRES);
    assertThat(c.clients()).isEqualTo(16);
    assertThatThrownBy(() -> ProtocolsWorkload.Case.parse("point-http-2c")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> ProtocolsWorkload.Case.parse("nope-http-1c")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void serverSettingsEnableTheFourProtocols() {
    final Map<String, String> settings = new ProtocolsWorkload().serverSettings();
    assertThat(settings).containsEntry("arcadedb.server.plugins",
        "PostgresProtocolPlugin,GrpcServerPlugin,BoltProtocolPlugin,PrometheusMetricsPlugin");
    assertThat(settings).containsEntry("arcadedb.grpc.enabled", "true");
    assertThat(settings).containsEntry("arcadedb.grpc.port", "50051");
    assertThat(settings).containsEntry("arcadedb.grpc.mode", "standard");
  }

  @Test
  void sixteenClientsRecordEveryOperation() throws Exception {
    final AtomicInteger calls = new AtomicInteger();
    final Set<Integer> seen = ConcurrentHashMap.newKeySet();
    final LatencyRecorder recorder = ProtocolsWorkload.runClients(16, 1_000, op -> {
      calls.incrementAndGet();
      seen.add(op);
    });
    assertThat(recorder.count()).isEqualTo(1_000);
    assertThat(calls.get()).isEqualTo(1_000);
    // every operation index exactly once
    assertThat(seen).hasSize(1_000);
  }

  @Test
  void eachClientThreadGetsItsOwnOperations() throws Exception {
    // operation i runs on thread i % clients: the op can index a per-client resource without sharing it
    final Map<Integer, Long> threadOfClient = new ConcurrentHashMap<>();
    final AtomicInteger clash = new AtomicInteger();
    ProtocolsWorkload.runClients(16, 1_000, op -> {
      final long thread = Thread.currentThread().threadId();
      final Long previous = threadOfClient.putIfAbsent(op % 16, thread);
      if (previous != null && previous != thread)
        clash.incrementAndGet();
    });
    assertThat(threadOfClient).hasSize(16);
    assertThat(clash.get()).isZero();
  }

  @Test
  void aFailingClientFailsTheRunAfterEveryThreadEnded() {
    final AtomicInteger calls = new AtomicInteger();
    assertThatThrownBy(() -> ProtocolsWorkload.runClients(16, 1_000, op -> {
      calls.incrementAndGet();
      if (op == 17)
        throw new IllegalStateException("boom at 17");
    })).hasMessageContaining("boom at 17");
    // every thread was joined: none of the client threads is alive once runClients has thrown
    assertThat(Thread.getAllStackTraces().keySet()).noneMatch(
        t -> t.getName().startsWith(ProtocolsWorkload.CLIENT_THREAD) && t.isAlive());
    // the others stop at the failure instead of running their share to the end
    assertThat(calls.get()).isLessThan(1_000);
  }

  @Test
  void oneClientRunsInline() throws Exception {
    final Thread caller = Thread.currentThread();
    final AtomicInteger onCaller = new AtomicInteger();
    final LatencyRecorder recorder = ProtocolsWorkload.runClients(1, 50, op -> {
      if (Thread.currentThread() == caller)
        onCaller.incrementAndGet();
    });
    assertThat(recorder.count()).isEqualTo(50);
    assertThat(onCaller.get()).isEqualTo(50);
  }

  @Test
  void operationsPerRepAtSAreOneThousand() {
    for (final Shape shape : Shape.values())
      assertThat(ProtocolsWorkload.operations(Scale.S, shape)).as(shape.name()).isEqualTo(1_000);
  }

  @Test
  void usersAtXsFillAPage() {
    assertThat(ProtocolsWorkload.users(Scale.XS)).isGreaterThanOrEqualTo(1_000);
    for (final Scale scale : Scale.values())
      assertThat(ProtocolsWorkload.users(scale)).isLessThan((int) ProtocolsWorkload.WRITE_BASE);
  }
}
