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
package com.arcadedb.function.sql.vector;

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.Locale;

/**
 * Calculates a sparsity measure of a vector.
 * <p>
 * The threshold is optional and defaults to {@code sqrt(eps)} for float (~3.45e-4), i.e. values smaller
 * in magnitude than the default are treated as effectively zero.
 * <p>
 * An optional mode selects the measure:
 * <ul>
 *   <li>{@code FRACTION} (default): fraction of elements with {@code |x_i| < threshold}, in [0, 1].
 *       Example: {@code vectorSparsity([0.01, 0.1, 0.05, 0.02], 0.06) = 0.75}</li>
 *   <li>{@code L0}: the L0 pseudonorm, i.e. the count of "significant" elements with
 *       {@code |x_i| >= threshold} (returned as an integer).</li>
 *   <li>{@code GMEAN}: geometric mean of the absolute values, {@code exp(mean(ln|x_i|))}; returns 0 when
 *       any element is exactly 0.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLFunctionVectorSparsity extends SQLFunctionVectorAbstract {
  public static final String NAME = "vector.sparsity";

  /** Default threshold ~ sqrt(machine epsilon) for float (~3.45e-4). */
  private static final float DEFAULT_THRESHOLD = (float) Math.sqrt(Math.ulp(1.0f));

  public enum Measure {
    FRACTION,
    L0,
    GMEAN
  }

  public SQLFunctionVectorSparsity() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || params.length < 1 || params.length > 3)
      throw new CommandSQLParsingException(getSyntax());

    final Object vectorObj = params[0];
    if (vectorObj == null)
      return null;

    final float[] vector = toFloatArray(vectorObj);

    if (vector.length == 0)
      throw new CommandSQLParsingException("Vector cannot be empty");

    // Parse optional threshold (default sqrt(eps))
    float threshold = DEFAULT_THRESHOLD;
    if (params.length >= 2 && params[1] != null) {
      if (params[1] instanceof Number num)
        threshold = num.floatValue();
      else
        throw new CommandSQLParsingException("Threshold must be a number, found: " + params[1].getClass().getSimpleName());
      if (threshold < 0)
        throw new CommandSQLParsingException("Threshold must be >= 0, found: " + threshold);
    }

    // Parse optional measure (default FRACTION)
    Measure measure = Measure.FRACTION;
    if (params.length == 3 && params[2] != null) {
      if (!(params[2] instanceof String str))
        throw new CommandSQLParsingException("Mode must be a string, found: " + params[2].getClass().getSimpleName());
      try {
        measure = Measure.valueOf(str.toUpperCase(Locale.ROOT));
      } catch (final IllegalArgumentException e) {
        throw new CommandSQLParsingException("Unknown sparsity mode: " + str + ". Supported: FRACTION, L0, GMEAN");
      }
    }

    return switch (measure) {
      case FRACTION -> {
        int belowThreshold = 0;
        for (final float value : vector)
          if (Math.abs(value) < threshold)
            belowThreshold++;
        yield (float) belowThreshold / vector.length;
      }
      case L0 -> {
        int significant = 0;
        for (final float value : vector)
          if (Math.abs(value) >= threshold)
            significant++;
        yield significant;
      }
      case GMEAN -> {
        double sumLog = 0.0;
        for (final float value : vector) {
          final double abs = Math.abs(value);
          if (abs == 0.0)
            yield 0.0f;
          sumLog += Math.log(abs);
        }
        yield (float) Math.exp(sumLog / vector.length);
      }
    };
  }

  public String getSyntax() {
    return NAME + "(<vector> [, <threshold> [, <mode>]])";
  }
}
