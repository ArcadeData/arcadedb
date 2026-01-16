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

import com.arcadedb.database.Document;
import com.arcadedb.query.opencypher.query.OpenCypherQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Expression representing a map projection from a node/variable.
 * Syntax: variable{.property1, .property2, key: expression, ...}
 * Examples:
 * - n{.name, .age} -> {name: n.name, age: n.age}
 * - n{.name, fullAge: n.age * 2} -> {name: n.name, fullAge: n.age * 2}
 * - n{.*} -> all properties of n
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class MapProjectionExpression implements Expression {

  /**
   * Represents a single element in the map projection.
   */
  public static class ProjectionElement {
    private final String propertyName;    // For .property syntax (without leading dot)
    private final String key;             // For key: expression syntax
    private final Expression expression;  // For key: expression syntax
    private final boolean allProperties;  // For .* syntax

    // Constructor for .property syntax
    public ProjectionElement(final String propertyName) {
      this.propertyName = propertyName;
      this.key = null;
      this.expression = null;
      this.allProperties = false;
    }

    // Constructor for key: expression syntax
    public ProjectionElement(final String key, final Expression expression) {
      this.propertyName = null;
      this.key = key;
      this.expression = expression;
      this.allProperties = false;
    }

    // Constructor for .* syntax
    public ProjectionElement(final boolean allProperties) {
      this.propertyName = null;
      this.key = null;
      this.expression = null;
      this.allProperties = allProperties;
    }

    public boolean isPropertySelector() {
      return propertyName != null;
    }

    public boolean isKeyExpression() {
      return key != null && expression != null;
    }

    public boolean isAllProperties() {
      return allProperties;
    }

    public String getPropertyName() {
      return propertyName;
    }

    public String getKey() {
      return key;
    }

    public Expression getExpression() {
      return expression;
    }
  }

  private final String variableName;
  private final List<ProjectionElement> elements;
  private final String text;

  public MapProjectionExpression(final String variableName, final List<ProjectionElement> elements, final String text) {
    this.variableName = variableName;
    this.elements = elements;
    this.text = text;
  }

  @Override
  public Object evaluate(final Result result, final CommandContext context) {
    final Object variable = result.getProperty(variableName);

    if (variable == null)
      return null;

    final Map<String, Object> projectedMap = new LinkedHashMap<>();

    for (final ProjectionElement element : elements) {
      if (element.isAllProperties()) {
        // Add all properties from the source
        addAllProperties(variable, projectedMap);
      } else if (element.isPropertySelector()) {
        // Add single property: .propertyName
        final Object value = getProperty(variable, element.getPropertyName());
        projectedMap.put(element.getPropertyName(), value);
      } else if (element.isKeyExpression()) {
        // Add computed value: key: expression
        final Object value = OpenCypherQueryEngine.getExpressionEvaluator().evaluate(element.getExpression(), result, context);
        projectedMap.put(element.getKey(), value);
      }
    }

    return projectedMap;
  }

  private void addAllProperties(final Object source, final Map<String, Object> target) {
    if (source instanceof Document) {
      final Document doc = (Document) source;
      for (final String prop : doc.getPropertyNames())
        target.put(prop, doc.get(prop));
    } else if (source instanceof Map) {
      final Map<?, ?> map = (Map<?, ?>) source;
      for (final Map.Entry<?, ?> entry : map.entrySet())
        target.put(String.valueOf(entry.getKey()), entry.getValue());
    } else if (source instanceof Result) {
      final Result res = (Result) source;
      for (final String prop : res.getPropertyNames())
        target.put(prop, res.getProperty(prop));
    }
  }

  private Object getProperty(final Object source, final String propertyName) {
    if (source instanceof Document)
      return ((Document) source).get(propertyName);
    else if (source instanceof Map)
      return ((Map<?, ?>) source).get(propertyName);
    else if (source instanceof Result)
      return ((Result) source).getProperty(propertyName);
    return null;
  }

  @Override
  public boolean isAggregation() {
    for (final ProjectionElement element : elements)
      if (element.isKeyExpression() && element.getExpression().isAggregation())
        return true;
    return false;
  }

  @Override
  public String getText() {
    return text;
  }

  public String getVariableName() {
    return variableName;
  }

  public List<ProjectionElement> getElements() {
    return elements;
  }
}
