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
package com.arcadedb.query.opencypher.executor.steps;

import com.arcadedb.query.opencypher.ast.BooleanExpression;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ParallelRecordScan;

import java.util.List;

/**
 * A step whose rows an aggregation downstream of it can consume in the workers of a parallel scan, rather than one at a
 * time on the thread consuming the query (issue #8797): a label scan, with the predicates of the filter steps between
 * it and the aggregation.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public interface ParallelRowSource {
  /**
   * @return the only variable the rows bind, or {@code null} when these rows are not the ones of a parallel-capable scan
   */
  String parallelVariable();

  /**
   * Plans the parallel scan of this step's rows, filtered by its own predicates and by {@code filters}, the ones of the
   * steps downstream of it that the caller takes over. Takes this execution's decision: a source that answers {@code null}
   * serves its rows sequentially, the way it would have without the call.
   *
   * @return the scan, or {@code null} when this execution cannot run in parallel or has already started
   */
  ParallelRecordScan planParallelRows(CommandContext context, List<BooleanExpression> filters);
}
