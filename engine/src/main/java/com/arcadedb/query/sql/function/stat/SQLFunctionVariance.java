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
package com.arcadedb.query.sql.function.stat;

import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.function.SQLFunctionAbstract;

/**
 * Compute the variance estimation for a given field.
 * <p>
 * This class uses the Weldford's algorithm (presented in Donald Knuth's Art of Computer Programming) to avoid multiple distribution
 * values' passes. When executed in distributed mode it uses the Chan at al. pairwise variance algorithm to merge the results.
 * <br>
 * <br>
 * <b>References</b>
 * </p>
 *
 * <ul>
 *
 * <li>Cook, John D. <a href="http://www.johndcook.com/standard_deviation.html">Accurately computing running variance</a>.</li>
 *
 * <li>Knuth, Donald E. (1998) <i>The Art of Computer Programming, Volume 2: Seminumerical Algorithms, 3rd Edition.</i></li>
 *
 * <li>Welford, B. P. (1962) Note on a method for calculating corrected sums of squares and products. <i>Technometrics</i></li>
 *
 * <li>Chan, Tony F.; Golub, Gene H.; LeVeque, Randall J. (1979), <a
 * href="http://cpsc.yale.edu/sites/default/files/files/tr222.pdf">Parallel Algorithm</a>.</li>
 *
 * </ul>
 *
 * @author Fabrizio Fortino
 */
public class SQLFunctionVariance extends SQLFunctionAbstract {

  public static final String NAME = "variance";

  private long   n;
  private double mean;
  private double m2;

  public SQLFunctionVariance() {
    super(NAME);
  }

  public SQLFunctionVariance(final String iName, final int iMinParams, final int iMaxParams) {
    super(iName);
  }

  @Override
  public Object execute( Object iThis, Identifiable iCurrentRecord, Object iCurrentResult,
      Object[] iParams, CommandContext iContext) {
    if (iParams[0] instanceof Number) {
      addValue((Number) iParams[0]);
    } else if (MultiValue.isMultiValue(iParams[0])) {
      for (Object n : MultiValue.getMultiValueIterable(iParams[0])) {
        addValue((Number) n);
      }
    }
    return null;
  }

  @Override
  public boolean aggregateResults() {
    return true;
  }

  @Override
  public Object getResult() {
    return this.evaluate();
  }

  @Override
  public String getSyntax() {
    return NAME + "(<field>)";
  }

  private void addValue(Number value) {
    if (value != null) {
      ++n;
      double doubleValue = value.doubleValue();
      double nextM = mean + (doubleValue - mean) / n;
      m2 += (doubleValue - mean) * (doubleValue - nextM);
      mean = nextM;
    }
  }

  private Double evaluate() {
    return n > 1 ? m2 / n : null;
  }

}
