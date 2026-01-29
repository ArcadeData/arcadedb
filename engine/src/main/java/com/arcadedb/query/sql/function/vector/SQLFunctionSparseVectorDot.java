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
package com.arcadedb.query.sql.function.vector;

import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * Computes dot product (inner product) between two sparse vectors.
 * Efficiently handles sparse representation by only iterating non-zero elements.
 *
 * Result: ∑(v1[i] * v2[i]) for common indices
 *
 * Example: sparseVectorDot(
 *   sparseVectorCreate([0, 2], [0.5, 0.3]),
 *   sparseVectorCreate([1, 2], [0.2, 0.8])
 * ) → 0.3 * 0.8 = 0.24
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLFunctionSparseVectorDot extends SQLFunctionVectorAbstract {
  public static final String NAME = "vectorSparseDot";

  public SQLFunctionSparseVectorDot() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || params.length != 2)
      throw new CommandSQLParsingException(getSyntax());

    final Object v1Obj = params[0];
    final Object v2Obj = params[1];

    if (v1Obj == null || v2Obj == null)
      return null;

    final SparseVector v1 = toSparseVector(v1Obj);
    final SparseVector v2 = toSparseVector(v2Obj);

    return v1.dotProduct(v2);
  }

  private SparseVector toSparseVector(final Object vector) {
    if (vector instanceof SparseVector sv) {
      return sv;
    } else {
      throw new CommandSQLParsingException("Expected SparseVector, found: " + vector.getClass().getSimpleName());
    }
  }

  public String getSyntax() {
    return NAME + "(<sparse_vector1>, <sparse_vector2>)";
  }
}
