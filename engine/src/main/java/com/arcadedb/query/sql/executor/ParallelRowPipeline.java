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
package com.arcadedb.query.sql.executor;

import com.arcadedb.query.sql.parser.Projection;
import com.arcadedb.query.sql.parser.WhereClause;
import com.arcadedb.schema.DocumentType;

import java.util.ArrayList;
import java.util.List;

/**
 * What sits between a {@link ParallelAggregationSource} and a step that consumes its rows in the workers of the parallel scan
 * (an aggregation, #8523, or a bounded ORDER BY, #8802): the type check, the conditions an index fetch leaves behind it
 * (#8333) and the projection that computes the consumer's arguments. A worker applies them to its own copies, as the
 * sequential steps would.
 *
 * @param source     the parallel scan source
 * @param types      the types a row must belong to (a {@link FilterByTypeStep} each)
 * @param conditions the conditions a row must satisfy (a {@link FilterStep} each)
 * @param projection the projection computed on every row that passes, or null
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
record ParallelRowPipeline(ParallelAggregationSource source, List<String> types, List<WhereClause> conditions,
                           Projection projection) {

  /**
   * The pipeline feeding a step whose input is {@code prev}, or {@code null} when that input is not a parallel scan source
   * reached through nothing but the row filters above and at most one {@link ProjectionCalculationStep}.
   */
  static ParallelRowPipeline of(final ExecutionStepInternal prev) {
    ExecutionStepInternal step = prev;
    Projection projection = null;
    if (step != null && step.getClass() == ProjectionCalculationStep.class) {
      projection = ((ProjectionCalculationStep) step).projection;
      step = ((ProjectionCalculationStep) step).prev;
    }

    final List<String> types = new ArrayList<>();
    final List<WhereClause> conditions = new ArrayList<>();
    while (step != null) {
      if (step.getClass() == FilterByTypeStep.class)
        types.add(((FilterByTypeStep) step).getTypeName());
      else if (step.getClass() == FilterStep.class)
        conditions.add(((FilterStep) step).getWhereClause());
      else
        break;
      step = ((AbstractExecutionStep) step).prev;
    }
    if (!(step instanceof ParallelAggregationSource source))
      return null;

    return new ParallelRowPipeline(source, types, conditions, projection);
  }

  /** Whether the row passes the type checks and the conditions, as their steps would decide. */
  static boolean matches(final String[] types, final WhereClause[] conditions, final Result row, final CommandContext context) {
    if (types.length > 0) {
      final DocumentType type = row.isElement() ? row.getElement().get().getType() : null;
      if (type == null)
        return false;
      for (final String name : types)
        if (!type.isSubTypeOf(name))
          return false;
    }
    for (final WhereClause condition : conditions)
      if (!condition.matchesFilters(row, context))
        return false;
    return true;
  }

  /** A copy of the conditions for one worker: an expression is not shared between threads. */
  WhereClause[] copyConditions() {
    final WhereClause[] copies = new WhereClause[conditions.size()];
    for (int i = 0; i < copies.length; i++)
      copies[i] = conditions.get(i).copy();
    return copies;
  }
}
