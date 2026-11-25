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
package com.arcadedb.function.sql.math;

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.InvalidArgumentException;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.function.sql.SQLAggregatedFunction;
import com.arcadedb.serializer.BinaryComparator;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

/**
 * Computes the percentile for a field. Nulls are ignored in the calculation.
 *
 * @author Fabrizio Fortino
 */
public class SQLFunctionPercentile extends SQLAggregatedFunction {
  public static final String NAME = "percentile";

  protected final List<Double> quantiles = new ArrayList<Double>();
  private final   List<Number> values    = new ArrayList<Number>();

  public SQLFunctionPercentile() {
    this(NAME);
  }

  public SQLFunctionPercentile(final String iName) {
    super(iName);
  }

  @Override
  public int getMinArgs() {
    return 2;
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {

    if (quantiles.isEmpty()) { // set quantiles once
      for (int i = 1; i < params.length; ++i) {
        if (params[i] == null)
          throw new InvalidArgumentException(getName() + "() quantiles must not be null");
        this.quantiles.add(Double.parseDouble(params[i].toString()));
      }
    }

    accumulateNumeric(params[0], this::addValue);
    return null;
  }

  @Override
  public boolean aggregateResults() {
    return true;
  }

  @Override
  public Object getResult() {
    return this.evaluate(this.values);
  }

  @Override
  public String getSyntax() {
    return NAME + "(<field>, <quantile> [,<quantile>*])";
  }

  private void addValue(final Number value) {
    if (value != null) {
      this.values.add(value);
    }
  }

  private Object evaluate(final List<Number> iValues) {
    if (iValues.isEmpty())  // result set is empty
      return null;

    // SORTED ONCE FOR EVERY QUANTILE, BY VALUE: doubleValue() MAKES 2^53 AND 2^53 + 1 EQUAL AND LET THE STABLE SORT KEEP THEM IN
    // INSERTION ORDER, SO THE ENDPOINTS CAME OUT SWAPPED (ISSUE #12569)
    iValues.sort(SQLFunctionPercentile::compareByValue);

    if (quantiles.size() > 1) {
      final List<Number> results = new ArrayList<Number>(this.quantiles.size());
      for (final Double q : this.quantiles)
        results.add(this.evaluate(iValues, q));

      return results;
    } else
      return this.evaluate(iValues, this.quantiles.getFirst());
  }

  /** Expects {@code iValues} sorted by {@link #compareByValue(Number, Number)}. */
  private Number evaluate(final List<Number> iValues, final double iQuantile) {
    final double n = iValues.size();
    final double pos = iQuantile * (n + 1);

    if (pos < 1)
      return iValues.getFirst();

    if (pos >= n)
      return iValues.get((int) n - 1);

    final double fpos = Math.floor(pos);
    final int intPos = (int) fpos;
    final double dif = pos - fpos;

    final Number lowerValue = iValues.get(intPos - 1);
    final Number upperValue = iValues.get(intPos);
    if (dif == 0 || compareByValue(lowerValue, upperValue) == 0)
      // NOTHING TO INTERPOLATE: THE ANSWER IS ONE OF THE VALUES
      return valueAsDouble(lowerValue);

    final double lower = lowerValue.doubleValue();
    final double upper = upperValue.doubleValue();
    return lower + dif * (upper - lower);
  }

  /**
   * The numeric order of {@link BinaryComparator#compareTo(Object, Object)}, exact across every class (a LONG above 2^53 against
   * its neighbour or against a DOUBLE), with a direct arm for the pairs of one class a column usually holds.
   */
  private static int compareByValue(final Number a, final Number b) {
    if (a instanceof Long la && b instanceof Long lb)
      return Long.compare(la, lb);
    if (a instanceof Integer ia && b instanceof Integer ib)
      return Integer.compare(ia, ib);
    if (a instanceof Double da && b instanceof Double db)
      return BinaryComparator.compareDoubles(da, db);
    return BinaryComparator.compareTo(a, b);
  }

  /**
   * A quantile that lands on one value answers it as a double, as an interpolated one does, but only when the double IS that value:
   * a LONG above 2^53 (or a BigInteger, a BigDecimal) the double cannot hold is answered as it is, never as a number no row holds
   * (issue #12569).
   */
  private static Number valueAsDouble(final Number value) {
    final double asDouble = value.doubleValue();
    if ((value instanceof Long || value instanceof BigInteger || value instanceof BigDecimal)
        && BinaryComparator.compareTo(value, asDouble) != 0)
      return value;
    return asDouble;
  }

  /** Keeps every value it aggregates until the aggregation ends (issue #8591). */
  @Override
  public boolean holdsEveryValue() {
    return true;
  }

  @Override
  public long getHeldValues() {
    return values.size();
  }
}
