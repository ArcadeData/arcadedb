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
package com.arcadedb.function.sql.graph;

import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.graph.IdFunction;
import com.arcadedb.function.sql.SQLFunctionConfigurableAbstract;
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * Converts a Cypher-style numeric id (the value returned by the OpenCypher {@code id()} function and the SQL {@code .asCypherRID()} method) back into a native
 * {@link RID}, so it can be used as the target of a SQL {@code SELECT}/{@code UPDATE}/{@code DELETE} and resolved to the record in O(1) (e.g.
 * {@code SELECT FROM cypherRID(:id)}). It is the inverse of {@code .asCypherRID()} and shares its configurable bit split
 * ({@code arcadedb.opencypher.idBucketBits}).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SQLFunctionCypherRID extends SQLFunctionConfigurableAbstract {
  public static final String NAME = "cypherrid";

  public SQLFunctionCypherRID() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    if (params == null || params.length != 1)
      throw new CommandExecutionException(NAME + "() requires exactly one argument: the numeric Cypher id");

    final Object value = params[0];
    if (value == null)
      return null;

    final long encoded;
    if (value instanceof Number number)
      encoded = number.longValue();
    else {
      try {
        encoded = Long.parseLong(value.toString());
      } catch (final NumberFormatException e) {
        throw new CommandExecutionException(NAME + "() requires a numeric Cypher id, but got: " + value);
      }
    }

    if (encoded < 0)
      throw new CommandExecutionException(NAME + "() received a negative id (" + encoded + "): a valid Cypher id is never negative");

    return IdFunction.decodeLongToRid(context.getDatabase(), encoded);
  }

  @Override
  public String getSyntax() {
    return "cypherRID(<cypherId>)";
  }
}
