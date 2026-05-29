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
import com.arcadedb.database.RID;
import com.arcadedb.function.graph.IdFunction;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.method.AbstractSQLMethod;

/**
 * Converts a record/RID into the numeric (Long) form used by the native OpenCypher engine's {@code id()} function, so SQL queries can return values that round-trip with Cypher. The encoding matches {@link IdFunction#encodeRidAsLong(RID)}: the bucketId in the upper bits and the offset in the lower bits, with the split governed by {@code arcadedb.opencypher.idBucketBits}.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SQLMethodAsCypherRID extends AbstractSQLMethod {

  public static final String NAME = "ascypherrid";

  public SQLMethodAsCypherRID() {
    super(NAME);
  }

  @Override
  public Object execute(final Object value, final Identifiable currentRecord, final CommandContext context,
      final Object[] params) {
    if (value == null)
      return null;
    if (value instanceof Number number)
      return number.longValue();
    if (value instanceof Identifiable identifiable) {
      final RID rid = identifiable.getIdentity();
      return rid != null ? IdFunction.encodeRidAsLong(rid) : null;
    }
    if (value instanceof String string && RID.is(string))
      return IdFunction.encodeRidAsLong(RID.create(context != null ? context.getDatabase() : null, string));
    return null;
  }
}
