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
import com.arcadedb.index.vector.VectorArgumentCache;
import com.arcadedb.index.vector.VectorUtils;
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * Calculates the cosine similarity between two vectors.
 * Returns a value between -1 and 1, where 1 means identical direction,
 * 0 means perpendicular, and -1 means opposite direction.
 * Useful for comparing normalized embeddings.
 * <p>
 * Delegates to {@link VectorUtils#cosineSimilarity(float[], float[])} which uses
 * JVector's SIMD-optimized implementation when available, with a scalar fallback.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLFunctionVectorCosineSimilarity extends SQLFunctionVectorAbstract {
  public static final String NAME = "vector.cosineSimilarity";

  public SQLFunctionVectorCosineSimilarity() {
    super(NAME);
  }

  @Override
  public int getMinArgs() {
    return 2;
  }

  @Override
  public int getMaxArgs() {
    return 2;
  }

  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    checkArity(params);

    final Object vector1 = params[0];
    final Object vector2 = params[1];

    if (vector1 == null || vector2 == null)
      return null;

    // The two arguments are prepared through the command's cache: a scan scores every row against
    // the same query vector, and preparing that vector is the same work every row (issue #12568)
    final VectorUtils.PreparedVector v1 = VectorArgumentCache.prepare(vector1, context);
    final VectorUtils.PreparedVector v2 = VectorArgumentCache.prepare(vector2, context);

    validateSameDimension(v1.values(), v2.values());

    if (v1.values().length == 0)
      throw new CommandSQLParsingException("Vectors cannot be empty");

    return VectorUtils.cosineSimilarity(v1, v2);
  }

  public String getSyntax() {
    return NAME + "(<vector1>, <vector2>)";
  }
}
