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
package com.arcadedb.index.columnar;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Named kill points of the columnar projection, for crash tests. In production {@link #killPoint} is null and {@link #reach} costs one
 * volatile read. A test installs a consumer (the {@code KillPoint} fixture) that may block the reaching thread and then throw
 * {@link SimulatedCrash}, which stops the operation exactly at the point as a crash would.
 * <p>
 * Points of the spec 5.5 publish order: {@code publish.segmentsSynced} and {@code publish.dictionariesSynced}, reached by the
 * publishing operation (flush, merge, rebuild, compaction) after its segment and dead files and after its dictionaries are durable,
 * then {@code publish.manifestTmpWritten} and {@code publish.manifestRenamed}, reached by {@link ColumnarManifest#publish}. Also
 * {@code dictionary.appendPartial}: a dictionary record header written without its payload. Later tasks add their own.
 */
public final class ColumnarTestHooks {
  /** Called with the name of every point reached; null in production. */
  public static volatile Consumer<String> killPoint;

  /**
   * Calls that reached a write method of a {@link ColumnarIndex} (put, remove and their replays), JVM-wide. No write path should
   * reach one (the indexer and the graph batch skip the type), so the opt-out tests read it to prove the skip, not only the no-op.
   */
  public static final AtomicLong INDEX_WRITES = new AtomicLong();

  /** Thrown by a test hook to stop an operation at a kill point. */
  public static final class SimulatedCrash extends RuntimeException {
    public SimulatedCrash() {
      super("Simulated crash at a columnar kill point");
    }
  }

  private ColumnarTestHooks() {
  }

  static void reach(final String name) {
    final Consumer<String> hook = killPoint;
    if (hook != null)
      hook.accept(name);
  }
}
