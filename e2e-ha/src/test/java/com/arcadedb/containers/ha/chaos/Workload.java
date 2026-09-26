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

package com.arcadedb.containers.ha.chaos;

import com.arcadedb.serializer.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * K writer threads, each a closed loop: pick a node, send one single-vertex insert (80%) or one vertex-plus-edge
 * transaction (20%), record the outcome, think briefly. Each operation is attempted exactly once. Writers pick any node
 * whatever its state: writes to a stopped node fail fast, writes to a paused one time out as UNKNOWN, writes to a
 * follower exercise leader forwarding. Each writer has its own Random derived from the seed.
 */
public final class Workload implements LoadGenerator {
  private static final int  DEFAULT_CONNECT_TIMEOUT_MS = 2_000;
  private static final int  DEFAULT_READ_TIMEOUT_MS    = 15_000;
  private static final int  DEFAULT_THINK_MS           = 5;
  private static final int  PAIR_PERCENT               = 20;
  private static final long WRITER_SEED_MIX            = 0x9E3779B97F4A7C15L;

  private final ChaosConfig            config;
  private final Ledger                 ledger;
  private final Endpoints              endpoints;
  private final String                 commandPath;
  private final int                    connectTimeoutMs;
  private final int                    readTimeoutMs;
  private final int                    thinkMs;
  private final ReentrantReadWriteLock gate    = new ReentrantReadWriteLock(true);
  private final AtomicBoolean          running = new AtomicBoolean();
  private final List<Thread>           threads = new ArrayList<>();

  public Workload(final ChaosConfig config, final Ledger ledger, final Endpoints endpoints, final String database) {
    this(config, ledger, endpoints, database, DEFAULT_CONNECT_TIMEOUT_MS, DEFAULT_READ_TIMEOUT_MS, DEFAULT_THINK_MS);
  }

  public Workload(final ChaosConfig config, final Ledger ledger, final Endpoints endpoints, final String database,
      final int connectTimeoutMs, final int readTimeoutMs, final int thinkMs) {
    this.config = config;
    this.ledger = ledger;
    this.endpoints = endpoints;
    this.commandPath = "/api/v1/command/" + database;
    this.connectTimeoutMs = connectTimeoutMs;
    this.readTimeoutMs = readTimeoutMs;
    this.thinkMs = thinkMs;
  }

  @Override
  public void start() {
    running.set(true);
    for (int w = 0; w < config.writers(); w++) {
      final int writer = w;
      final Random random = new Random(config.seed() ^ (WRITER_SEED_MIX * (writer + 1)));
      final Thread thread = new Thread(() -> loop(writer, random), "chaos-writer-" + writer);
      thread.setDaemon(true);
      threads.add(thread);
      thread.start();
    }
  }

  private void loop(final int writer, final Random random) {
    while (running.get()) {
      gate.readLock().lock();
      try {
        if (!running.get())
          return;
        operation(writer, random);
      } finally {
        gate.readLock().unlock();
      }
      try {
        Thread.sleep(thinkMs);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  private void operation(final int writer, final Random random) {
    final int node = random.nextInt(endpoints.size());
    final long target = random.nextInt(100) < PAIR_PERCENT ? ledger.randomAckedKey(writer, random) : -1;
    final boolean pair = target >= 0;
    final long key = ledger.reserve(writer, pair);
    final JSONObject payload = new JSONObject()
        .put("language", pair ? "sqlscript" : "sql")
        .put("command", pair ? ChaosSchema.INSERT_PAIR : ChaosSchema.INSERT_SINGLE)
        .put("params", new JSONObject(pair ? ChaosSchema.pairParams(key, target) : ChaosSchema.singleParams(key)));
    byte outcome;
    try {
      final Endpoint endpoint = endpoints.endpoint(node);
      final ChaosHttp.Response response = ChaosHttp.post(endpoint.host(), endpoint.port(), commandPath, payload.toString(),
          connectTimeoutMs, readTimeoutMs);
      outcome = OpOutcome.fromStatus(response.status());
    } catch (final IOException e) {
      outcome = OpOutcome.fromException(e);
    } catch (final RuntimeException e) {
      outcome = Ledger.UNKNOWN;
    }
    ledger.record(key, outcome);
  }

  @Override
  public void quiesce() throws InterruptedException {
    gate.writeLock().lockInterruptibly();
  }

  @Override
  public void resume() {
    if (gate.isWriteLockedByCurrentThread())
      gate.writeLock().unlock();
  }

  @Override
  public long acked() {
    return ledger.count(Ledger.ACKED) + ledger.count(Ledger.ACKED_LATE);
  }

  @Override
  public void close() {
    running.set(false);
    resume();
    for (final Thread thread : threads)
      try {
        thread.join(readTimeoutMs + 5_000L);
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    threads.clear();
  }
}
