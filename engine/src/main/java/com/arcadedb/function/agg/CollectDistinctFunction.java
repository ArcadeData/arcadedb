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
package com.arcadedb.function.agg;

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * collect(DISTINCT ...) aggregation function - collects unique values into a list.
 * Uses LinkedHashSet to maintain insertion order while eliminating duplicates.
 * Example: MATCH (n:Person) RETURN collect(DISTINCT n.name)
 */
public class CollectDistinctFunction implements StatelessFunction {
  private final Set<Object> distinctValues = new LinkedHashSet<>();

  @Override
  public String getName() {
    return "collect";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    if (args.length != 1) {
      throw new CommandExecutionException("collect(DISTINCT ...) requires exactly one argument");
    }
    final Object value = args[0];
    // Only add non-null values, and use identity for Identifiable objects
    if (value != null) {
      if (value instanceof Identifiable)
        // Use RID as the key for deduplication to handle proxies and loaded records
        distinctValues.add(new IdentifiableWrapper((Identifiable) value));
      else
        distinctValues.add(value);
    }
    return null; // Intermediate result doesn't matter
  }

  @Override
  public boolean aggregateResults() {
    return true;
  }

  @Override
  public Object getAggregatedResult() {
    // Unwrap IdentifiableWrapper back to the original objects
    final List<Object> result = new ArrayList<>(distinctValues.size());
    for (final Object value : distinctValues) {
      if (value instanceof IdentifiableWrapper)
        result.add(((IdentifiableWrapper) value).getIdentifiable());
      else
        result.add(value);
    }
    return result;
  }

  /**
   * Wrapper for Identifiable objects that uses RID for equals/hashCode.
   * This ensures proper deduplication even when the same record is loaded multiple times.
   */
  private static class IdentifiableWrapper {
    private final Identifiable identifiable;

    IdentifiableWrapper(final Identifiable identifiable) {
      this.identifiable = identifiable;
    }

    Identifiable getIdentifiable() {
      return identifiable;
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj)
        return true;
      if (obj == null || getClass() != obj.getClass())
        return false;
      final IdentifiableWrapper other = (IdentifiableWrapper) obj;
      return identifiable.getIdentity().equals(other.identifiable.getIdentity());
    }

    @Override
    public int hashCode() {
      return identifiable.getIdentity().hashCode();
    }
  }
}
