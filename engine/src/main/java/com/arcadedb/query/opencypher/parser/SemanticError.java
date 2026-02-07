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
package com.arcadedb.query.opencypher.parser;

/**
 * Represents a semantic error in a Cypher query with context and helpful messages.
 * Provides structured error information including error type, location, context, and suggestions.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class SemanticError {

  /**
   * Category of semantic errors in Cypher queries.
   */
  public enum ErrorType {
    // Variable errors
    UNDEFINED_VARIABLE("Variable not defined"),
    VARIABLE_ALREADY_BOUND("Variable already bound"),
    VARIABLE_TYPE_CONFLICT("Variable type conflict"),

    // Type errors
    TYPE_MISMATCH("Type mismatch"),
    INVALID_BOOLEAN_OPERAND("Invalid boolean operand"),
    INVALID_ARITHMETIC_OPERAND("Invalid arithmetic operand"),

    // Scope errors
    VARIABLE_NOT_IN_SCOPE("Variable not in scope"),
    AGGREGATION_IN_WHERE("Aggregation not allowed in WHERE"),
    NESTED_AGGREGATION("Nested aggregation not allowed"),

    // Structural errors
    INVALID_CREATE_PATTERN("Invalid CREATE pattern"),
    INVALID_MERGE_PATTERN("Invalid MERGE pattern"),
    INVALID_DELETE_TARGET("Invalid DELETE target"),

    // UNION errors
    UNION_COLUMN_MISMATCH("UNION column mismatch"),
    MIXED_UNION_TYPES("Cannot mix UNION and UNION ALL"),

    // Expression errors
    MISSING_ALIAS("Missing alias for expression"),
    DUPLICATE_ALIAS("Duplicate alias"),
    INVALID_EXPRESSION("Invalid expression"),

    // Constraint errors
    INVALID_SKIP_LIMIT("Invalid SKIP/LIMIT value"),
    CONSTRAINT_VIOLATION("Constraint violation");

    private final String description;

    ErrorType(final String description) {
      this.description = description;
    }

    public String getDescription() {
      return description;
    }
  }

  private final ErrorType type;
  private final String message;
  private final String context;
  private final String suggestion;
  private final Integer line;
  private final Integer column;

  private SemanticError(final ErrorType type, final String message, final String context,
                        final String suggestion, final Integer line, final Integer column) {
    this.type = type;
    this.message = message;
    this.context = context;
    this.suggestion = suggestion;
    this.line = line;
    this.column = column;
  }

  /**
   * Builder for creating semantic errors with optional context and suggestions.
   */
  public static class Builder {
    private final ErrorType type;
    private String message;
    private String context;
    private String suggestion;
    private Integer line;
    private Integer column;

    public Builder(final ErrorType type) {
      this.type = type;
    }

    public Builder message(final String message) {
      this.message = message;
      return this;
    }

    public Builder context(final String context) {
      this.context = context;
      return this;
    }

    public Builder suggestion(final String suggestion) {
      this.suggestion = suggestion;
      return this;
    }

    public Builder location(final int line, final int column) {
      this.line = line;
      this.column = column;
      return this;
    }

    public SemanticError build() {
      if (message == null)
        message = type.getDescription();

      return new SemanticError(type, message, context, suggestion, line, column);
    }
  }

  /**
   * Create a formatted error message with all available context.
   */
  public String format() {
    final StringBuilder sb = new StringBuilder();

    // Error type and location
    sb.append(type.name()).append(": ");
    if (line != null && column != null)
      sb.append("[line ").append(line).append(", column ").append(column).append("] ");

    // Main error message
    sb.append(message);

    // Context (which clause, which expression, etc.)
    if (context != null) {
      sb.append("\n  Context: ").append(context);
    }

    // Suggestion for fix
    if (suggestion != null) {
      sb.append("\n  Suggestion: ").append(suggestion);
    }

    return sb.toString();
  }

  @Override
  public String toString() {
    return format();
  }

  // Getters
  public ErrorType getType() {
    return type;
  }

  public String getMessage() {
    return message;
  }

  public String getContext() {
    return context;
  }

  public String getSuggestion() {
    return suggestion;
  }

  public Integer getLine() {
    return line;
  }

  public Integer getColumn() {
    return column;
  }
}
