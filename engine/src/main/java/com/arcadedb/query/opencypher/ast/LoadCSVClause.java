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
package com.arcadedb.query.opencypher.ast;

/**
 * Represents a LOAD CSV clause in a Cypher query.
 * Reads CSV data from a URL and binds each row to a variable.
 * <p>
 * Examples:
 * - LOAD CSV FROM 'file:///path/to/data.csv' AS row RETURN row
 * - LOAD CSV WITH HEADERS FROM 'file:///path/to/data.csv' AS row RETURN row.name
 * - LOAD CSV WITH HEADERS FROM 'file:///path/to/data.csv' AS row FIELDTERMINATOR ';'
 */
public class LoadCSVClause {
  private final Expression urlExpression;
  private final String variable;
  private final boolean withHeaders;
  private final String fieldTerminator;

  public LoadCSVClause(final Expression urlExpression, final String variable, final boolean withHeaders,
      final String fieldTerminator) {
    this.urlExpression = urlExpression;
    this.variable = variable;
    this.withHeaders = withHeaders;
    this.fieldTerminator = fieldTerminator;
  }

  public Expression getUrlExpression() {
    return urlExpression;
  }

  public String getVariable() {
    return variable;
  }

  public boolean isWithHeaders() {
    return withHeaders;
  }

  public String getFieldTerminator() {
    return fieldTerminator;
  }
}
