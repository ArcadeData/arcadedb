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
package com.arcadedb.query.sql.method.conversion;

import com.arcadedb.database.Identifiable;
import com.arcadedb.function.sql.vector.SQLFunctionVectorDenseToSparse;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.method.AbstractSQLMethod;

/**
 * Converts a dense vector value into a sparse vector, dropping elements whose absolute value is at or
 * below an optional threshold (default {@code 0.0}, i.e. drops exact zeros). The method form of
 * {@code vector.denseToSparse()}: {@code embedding.asSparse()} / {@code embedding.asSparse(0.01)},
 * consistent with the {@code asString()} / {@code asVector()} conversion-method family.
 *
 * Example: {@code [0.5, 0.0, 0.3].asSparse()} → SparseVector indices=[0, 2] values=[0.5, 0.3]
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public class SQLMethodAsSparse extends AbstractSQLMethod {

  public static final String NAME = "assparse";

  // Shared instance reused across concurrent queries (no per-call allocation). Safe because
  // SQLFunctionVectorDenseToSparse is documented and required to be stateless - see its class Javadoc.
  private static final SQLFunctionVectorDenseToSparse DENSE_TO_SPARSE = new SQLFunctionVectorDenseToSparse();

  public SQLMethodAsSparse() {
    super(NAME, 0, 1);
  }

  @Override
  public Object execute(final Object value, final Identifiable currentRecord, final CommandContext context,
      final Object[] params) {
    if (value == null)
      return null;

    final Object[] fnParams = params != null && params.length > 0 && params[0] != null ?
        new Object[] { value, params[0] } :
        new Object[] { value };

    // Reuse the function implementation so dense->sparse logic lives in one place.
    return DENSE_TO_SPARSE.execute(null, currentRecord, null, fnParams, context);
  }
}
