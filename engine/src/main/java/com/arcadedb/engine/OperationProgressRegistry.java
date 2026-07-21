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
package com.arcadedb.engine;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * JVM-wide registry of the long-running maintenance operations currently in progress (CHECK DATABASE first;
 * COMPACT INDEX, index rebuilds, backups and imports can publish here too). Producers register an
 * {@link OperationProgress} when the operation starts, feed it as their progress callback, and MUST unregister
 * it in a finally block. Readers (HTTP progress endpoint, console/Studio pollers) get a weakly-consistent
 * snapshot. The registry is deliberately process-local: in an HA cluster each server reports what it is doing,
 * which is what an operator polling that node wants to see.
 * <p>
 * Operations are keyed by DATABASE NAME, JVM-globally: in an embedded process hosting two databases that share
 * a name (different directories), {@link #getOperations} merges their operations in one snapshot. Accepted for
 * now - the server never hosts two same-named databases - revisit with identity-based keying if that topology
 * ever becomes supported.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class OperationProgressRegistry {
  private static final OperationProgressRegistry INSTANCE = new OperationProgressRegistry();

  private final AtomicLong                                idCounter  = new AtomicLong();
  private final ConcurrentHashMap<Long, OperationProgress> operations = new ConcurrentHashMap<>();

  private OperationProgressRegistry() {
  }

  public static OperationProgressRegistry instance() {
    return INSTANCE;
  }

  public OperationProgress register(final String databaseName, final String operation) {
    final OperationProgress progress = new OperationProgress(idCounter.incrementAndGet(), databaseName, operation);
    operations.put(progress.getId(), progress);
    return progress;
  }

  public void unregister(final OperationProgress progress) {
    if (progress != null)
      operations.remove(progress.getId());
  }

  /** Snapshot of the running operations for a database, oldest first. */
  public List<OperationProgress> getOperations(final String databaseName) {
    final List<OperationProgress> result = new ArrayList<>();
    for (final OperationProgress op : operations.values())
      if (op.getDatabaseName().equals(databaseName))
        result.add(op);
    result.sort(Comparator.comparingLong(OperationProgress::getId));
    return result;
  }

  public int size() {
    return operations.size();
  }
}
