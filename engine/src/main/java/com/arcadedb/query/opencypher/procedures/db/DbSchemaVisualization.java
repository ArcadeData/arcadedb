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

import com.arcadedb.query.opencypher.procedures.CypherProcedure;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.VertexType;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Neo4j-compatible procedure: {@code db.schema.visualization()}, also registered as {@code db.schema()}.
 * <p>
 * Returns one row per type declared in the database, under the fields {@code name}, {@code type}
 * ({@code "node"}, {@code "relationship"} or {@code "document"}) and {@code properties}.
 * </p>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class DbSchemaVisualization implements CypherProcedure {
  private final String name;

  public DbSchemaVisualization(final String name) {
    this.name = name;
  }

  @Override
  public String getName() {
    return name;
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
    return "Returns the database schema (type name, kind and properties) for visualization (Neo4j-compatible)";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("name", "type", "properties");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    final List<Result> results = new ArrayList<>();
    for (final DocumentType type : context.getDatabase().getSchema().getTypes()) {
      if (type.getName().contains("~"))
        continue;

      final ResultInternal result = new ResultInternal();
      result.setProperty("name", type.getName());
      result.setProperty("type", type instanceof VertexType ? "node" : type instanceof EdgeType ? "relationship" : "document");
      result.setProperty("properties", new ArrayList<>(type.getPropertyNames()));
      results.add(result);
    }
    return results.stream();
  }
}
