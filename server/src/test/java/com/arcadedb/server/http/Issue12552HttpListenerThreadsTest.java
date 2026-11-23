/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.http;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.xnio.OptionMap;
import org.xnio.Options;
import org.xnio.Xnio;
import org.xnio.XnioWorker;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #12552: an {@code OutOfMemoryError} ended the thread {@code XNIO-1 Accept} and the node kept
 * accepting TCP connections on its HTTP port that nobody answered. The HTTP server now keeps the XNIO threads it runs on
 * and reports one that has ended, which the readiness probe reads.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class Issue12552HttpListenerThreadsTest {

  // Regression for #12552: the accept and I/O threads of a worker are found, and their end is seen
  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES) // hang detector only
  void theXnioAcceptAndIoThreadsAreFoundAndTheirEndIsSeen() throws Exception {
    final XnioWorker worker = Xnio.getInstance().createWorker(OptionMap.builder().set(Options.WORKER_IO_THREADS, 2).getMap());
    final List<Thread> threads;
    try {
      threads = HttpServer.xnioListenerThreads(worker);
      assertThat(threads).hasSize(3);
      assertThat(threads).extracting(Thread::getName).contains(worker.getName() + " Accept");
      assertThat(threads).allMatch(Thread::isAlive);
    } finally {
      worker.shutdownNow();
      worker.awaitTermination(30, TimeUnit.SECONDS);
    }
    for (final Thread thread : threads)
      thread.join(TimeUnit.SECONDS.toMillis(30));
    assertThat(threads).noneMatch(Thread::isAlive);
  }

  // Regression for #12552
  @Test
  void noWorkerHasNoThreads() {
    assertThat(HttpServer.xnioListenerThreads(null)).isEmpty();
  }
}
