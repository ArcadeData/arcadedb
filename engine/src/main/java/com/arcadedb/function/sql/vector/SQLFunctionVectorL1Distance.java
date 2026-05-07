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
import com.arcadedb.index.vector.VectorUtils;
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * Calculates the Manhattan (L1) distance between two vectors. Sum of absolute differences across
 * dimensions; lower values indicate greater similarity.
 * <p>
 * Closes the SQL-side gap with {@code vector.l2Distance}: the underlying
 * {@link VectorUtils#manhattanDistance(float[], float[])} was already on the engine and exposed
 * to Cypher via {@code vector.distance.manhattan} since the v1 vector ship, but the dot-notation
 * SQL surface had no equivalent. Some embedding models (notably image and audio models trained
 * with sparsity-friendly losses) prefer L1 over cosine/L2/dot. Registered with two aliases:
 * the explicit {@code vector.manhattanDistance} for discoverability, and the symmetric
 * {@code vector.l1Distance} matching {@code vector.l2Distance} naming.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SQLFunctionVectorL1Distance extends SQLFunctionVectorAbstract {
  public static final String NAME       = "vector.l1Distance";
  public static final String NAME_ALIAS = "vector.manhattanDistance";

  public SQLFunctionVectorL1Distance() {
    super(NAME);
  }

  public SQLFunctionVectorL1Distance(final String name) {
    super(name);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || params.length != 2)
      throw new CommandSQLParsingException(getSyntax());

    final Object vector1 = params[0];
    final Object vector2 = params[1];

    if (vector1 == null || vector2 == null)
      throw new CommandSQLParsingException("Vectors cannot be null");

    final float[] v1 = toFloatArray(vector1);
    final float[] v2 = toFloatArray(vector2);

    validateSameDimension(v1, v2);

    return VectorUtils.manhattanDistance(v1, v2);
  }

  public String getSyntax() {
    return getName() + "(<vector1>, <vector2>)";
  }
}
