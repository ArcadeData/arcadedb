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
package com.arcadedb.query.opencypher.procedures.meta;

import com.arcadedb.database.Database;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Procedure: meta.nodeTypeProperties()
 * <p>
 * Returns information about properties for each node label/type.
 * </p>
 * <p>
 * Example Cypher usage:
 * <pre>
 * CALL meta.nodeTypeProperties()
 * YIELD nodeType, propertyName, propertyTypes, mandatory
 * </pre>
 * </p>
 *
 * @author ArcadeDB Team
 */
public class MetaNodeTypeProperties extends AbstractMetaProcedure {
  public static final String NAME = "meta.nodetypeproperties";

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
    return "Returns information about properties for each node label/type";
  }

  @Override
  public List<String> getYieldFields() {
    return List.of("nodeType", "propertyName", "propertyTypes", "mandatory");
  }

  @Override
  public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
    final Database database = context.getDatabase();
    final Schema schema = database.getSchema();

    final List<Result> results = new ArrayList<>();

    for (final DocumentType type : schema.getTypes()) {
      if (type instanceof VertexType) {
        for (final String propName : type.getPropertyNames()) {
          final Property property = type.getProperty(propName);

          final ResultInternal result = new ResultInternal();
          result.setProperty("nodeType", type.getName());
          result.setProperty("propertyName", propName);
          result.setProperty("propertyTypes", List.of(mapPropertyType(property.getType())));
          result.setProperty("mandatory", property.isMandatory());

          results.add(result);
        }
      }
    }

    return results.stream();
  }
}
