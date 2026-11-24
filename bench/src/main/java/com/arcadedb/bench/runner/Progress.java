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
package com.arcadedb.bench.runner;

import java.util.concurrent.atomic.LongAdder;

/**
 * A run-wide count of progress, read by the stall watchdog: every latency sample a workload records and every step the runner
 * moves its worker to. A worker whose stack has not changed is stalled only when this count has not moved either: a worker
 * waiting in {@code Future.get} on its insert threads, or a query loop repeating the same frames, keeps it moving.
 * <p>
 * A {@link LongAdder}, so the threads of a multi-threaded workload add to it without contending on one cache line.
 */
public final class Progress {
  private static final LongAdder COUNT = new LongAdder();

  private Progress() {
  }

  public static void advance() {
    COUNT.increment();
  }

  public static long count() {
    return COUNT.sum();
  }
}
