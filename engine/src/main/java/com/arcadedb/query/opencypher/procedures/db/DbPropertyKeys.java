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
package com.arcadedb.query.opencypher.procedures.db;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Record;
import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.security.SecurityHelper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Neo4j-compatible procedure: {@code db.propertyKeys()}
 * <p>
 * Returns one row per distinct property key of the database, under the field {@code propertyKey}: the properties its types declare,
 * then the ones its records carry. The engine keeps no register of the keys written by plain Cypher, which declares nothing, so
 * the records are read, at most {@link #SAMPLE_PER_TYPE} of each type (a key that only a later record carries is not listed).
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class DbPropertyKeys implements CypherProcedure {
  public static final String NAME = "db.propertyKeys";
  /** How many records of a type are read for the keys they carry: a bound on the cost of the call, not on the answer of a small database. */
  static final int SAMPLE_PER_TYPE = 1000;

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public int getMinArgs() {
    return 0;
  }

  @Override
  public int getMaxArgs() {
    return 0;
  }

  @Override
  public String getDescription() {
    return "Returns every distinct property key declared across all types in the database and carried by the first " + SAMPLE_PER_TYPE
        + " records of each type (Neo4j-compatible)";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("propertyKey");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    final Set<String> propertyKeys = new LinkedHashSet<>();
    final DatabaseInternal database = (DatabaseInternal) context.getDatabase();
    // Types the current user cannot read are hidden, as schema:types hides them in SQL
    final Collection<? extends DocumentType> readable = SecurityHelper.readableTypes(database);
    for (final DocumentType type : readable)
      if (!type.getName().contains("~"))
        propertyKeys.addAll(type.getPropertyNames());

    // What the records carry that nothing declared. The composite types of a multi-label node ('A~B') hold the records of those
    // nodes, so they are read too, though they declare nothing of their own
    for (final DocumentType type : readable) {
      if (type.getBuckets(false).isEmpty())
        continue;
      final Iterator<Record> records = database.iterateType(type.getName(), false);
      for (int read = 0; read < SAMPLE_PER_TYPE && records.hasNext(); ++read)
        propertyKeys.addAll(records.next().asDocument().getPropertyNames());
    }

    final List<Result> results = new ArrayList<>(propertyKeys.size());
    for (final String key : propertyKeys) {
      final ResultInternal result = new ResultInternal();
      result.setProperty("propertyKey", key);
      results.add(result);
    }
    return results.stream();
  }
}
