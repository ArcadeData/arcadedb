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
package com.arcadedb.query.facet;

import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

/**
 * Decides, per matched record, whether it passes the FILTER of one facet (disjunctive faceting, spec 7.5): the collect step sets bit
 * {@code i} of a record's fail mask when evaluator {@code i} refuses it. One instance serves one execution of a plan.
 */
public abstract class FacetFilterEvaluator implements AutoCloseable {
  /** Prepares one execution, before the first record. */
  public abstract void open(CommandContext ctx);

  /**
   * @param row the row of the record, as the plan produced it
   */
  public abstract boolean passes(RID rid, Result row, CommandContext ctx);

  /** How it decides, as EXPLAIN prints it: {@code INDEX}, {@code PROJECTION} or {@code RECORD}. */
  public abstract String kind();

  /** A fresh evaluator of the same filter, for another execution of a cached plan. */
  public abstract FacetFilterEvaluator copy();

  /** Releases what {@link #open} acquired; idempotent, and safe without a prior {@link #open}. */
  @Override
  public abstract void close();
}
