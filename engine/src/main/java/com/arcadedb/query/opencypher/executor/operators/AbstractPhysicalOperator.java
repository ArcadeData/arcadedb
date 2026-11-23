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
package com.arcadedb.query.opencypher.executor.operators;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.security.SecurityHelper;

import java.util.Locale;

/**
 * Abstract base class for physical operators.
 * Provides common functionality for cost tracking and tree structure.
 */
public abstract class AbstractPhysicalOperator implements PhysicalOperator {
  protected PhysicalOperator child;
  protected final double estimatedCost;
  protected final long estimatedCardinality;

  protected AbstractPhysicalOperator(final double estimatedCost, final long estimatedCardinality) {
    this.estimatedCost = estimatedCost;
    this.estimatedCardinality = estimatedCardinality;
    this.child = null;
  }

  protected AbstractPhysicalOperator(final PhysicalOperator child, final double estimatedCost,
                                    final long estimatedCardinality) {
    this.child = child;
    this.estimatedCost = estimatedCost;
    this.estimatedCardinality = estimatedCardinality;
  }

  @Override
  public double getEstimatedCost() {
    return estimatedCost;
  }

  @Override
  public long getEstimatedCardinality() {
    return estimatedCardinality;
  }

  @Override
  public PhysicalOperator getChild() {
    return child;
  }

  @Override
  public void setChild(final PhysicalOperator child) {
    this.child = child;
  }

  @Override
  public String explain(final int depth) {
    final StringBuilder sb = new StringBuilder();
    final String indent = "  ".repeat(depth);

    sb.append(indent).append("+ ").append(getOperatorType());
    sb.append(" [cost=").append(String.format(Locale.US, "%.2f", estimatedCost));
    sb.append(", rows=").append(estimatedCardinality);
    sb.append("]\n");

    if (child != null) {
      sb.append(child.explain(depth + 1));
    }

    return sb.toString();
  }

  @Override
  public String toString() {
    return getOperatorType() + "{cost=" + String.format(Locale.US, "%.2f", estimatedCost) +
           ", cardinality=" + estimatedCardinality + "}";
  }

  /**
   * Refuses the user who may not read the records of the type a name stands for, or of a type under it, as the walk of its buckets
   * does (issue #11904).
   * <p>
   * A walk opens every bucket of the type, and a bucket refuses a user who may not read it whatever the keys are. An index anchor
   * loads a record only when the index holds the key it was asked for, so left to itself it would refuse only for a key the denied
   * type holds, and the statement would tell a user who may not read the type whether it holds a key. The plan is not always built
   * for the user who runs it: the cache of Cypher plans belongs to the database, so a plan built for a user who may read the type is
   * run as it is by one who may not, and the check is the operator's to make, once per execution. The index of a type holds the
   * vertices of its subtypes too, which may carry an access of their own, so the buckets of every one of them are asked.
   *
   * @param context the context of the execution
   * @param name    the type, which is skipped when the schema has none of that name: the operator decides what that means
   *
   * @throws SecurityException when the current user may not read the type
   */
  protected static void requireReadable(final CommandContext context, final String name) {
    final DatabaseInternal database = context.getDatabase();
    SecurityHelper.checkReadOnTypeAndSubTypes(database, database.getSchema().getTypeOrNull(name));
  }

  /**
   * Helper method to format indentation for explain output.
   *
   * @param depth indentation depth
   * @return indentation string
   */
  protected String getIndent(final int depth) {
    return "  ".repeat(depth);
  }
}
