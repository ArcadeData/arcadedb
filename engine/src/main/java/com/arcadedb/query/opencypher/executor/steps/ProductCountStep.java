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

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.query.sql.executor.AbstractExecutionStep;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.IteratorResultSet;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.Collections;
import java.util.List;

/**
 * The row count of a MATCH made of parts that share no variable: the product of the row counts of the parts (issue
 * #9596). Every row of the whole is one row of each part, so {@code MATCH (a:Person), (b:Person), (c:Person)} over 1,700
 * persons is 1,700 cubed rows, and counting them by building them does not finish while three O(1) counts and two
 * multiplications do. Each part is counted by whatever its own statement takes - a count push-down, or the row pipeline
 * over that part alone - so the cost is the sum of the parts' costs rather than their product.
 * <p>
 * The parts are counted in order and the first one with no row ends the count, since the product is then 0 whatever the
 * others hold. A later part is then never run, so an error its own evaluation would raise is not raised either: the
 * row pipeline, which builds no row once a part is empty, does not evaluate it either.
 * <p>
 * A product that does not fit a {@code long} is an error rather than a wrapped number: those rows could not be counted
 * any other way either.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class ProductCountStep extends AbstractExecutionStep {
  /** One part of the product. */
  public interface Part {
    /** The number of rows the part produces on its own. */
    long count(CommandContext context);

    /** The part as EXPLAIN shows it. */
    String describe(int depth, int indent);
  }

  private final List<Part> parts;
  private final String     countAlias;
  private       boolean    executed = false;

  public ProductCountStep(final List<Part> parts, final String countAlias, final CommandContext context) {
    super(context);
    this.parts = parts;
    this.countAlias = countAlias;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    if (executed)
      return new IteratorResultSet(Collections.emptyIterator());
    executed = true;

    final long begin = context.isProfiling() ? System.nanoTime() : 0;
    try {
      long product = 1;
      boolean overflow = false;
      for (final Part part : parts) {
        final long count = part.count(context);
        if (count == 0) {
          product = 0;
          overflow = false; // an empty part makes the product 0, whatever an earlier part overflowed to
          break;
        }
        if (!overflow) {
          try {
            product = Math.multiplyExact(product, count);
          } catch (final ArithmeticException e) {
            // keep counting: a later part with no row still makes the product 0
            overflow = true;
          }
        }
      }
      if (overflow)
        throw new CommandExecutionException(
            "Row count overflow: the product of the counts of the " + parts.size() + " disconnected parts of the MATCH "
                + "does not fit a 64-bit integer");

      if (context.isProfiling())
        rowCount = 1;
      final ResultInternal result = new ResultInternal();
      result.setProperty(countAlias, product);
      return new IteratorResultSet(List.of((Result) result).iterator());
    } finally {
      if (context.isProfiling())
        cost = System.nanoTime() - begin;
    }
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String ind = "  ".repeat(Math.max(0, depth * indent));
    final StringBuilder sb = new StringBuilder();
    sb.append(ind).append("+ COUNT CARTESIAN PRODUCT (").append(parts.size()).append(" disconnected parts)");
    for (int i = 0; i < parts.size(); i++)
      sb.append('\n').append(ind).append("  part ").append(i).append(":\n").append(parts.get(i).describe(depth + 1, indent));
    return sb.toString();
  }
}
