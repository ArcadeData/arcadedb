package com.arcadedb.query.nativ;/*
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
 */

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.Record;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.DocumentType;

import java.util.*;

/**
 * Native Query engine is a simple query engine that covers most of the classic use cases, such as the retrieval of records
 * with a where condition. It could be much faster than the same SQL query because it does not use any parser and it is very
 * JIT friendly. Future versions could translate the query into bytecode to have an even faster execution.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class NativeSelect {
  private final DatabaseInternal database;

  enum STATE {DEFAULT, WHERE, COMPILED}

  Map<String, Object> parameters;

  private NativeTreeNode     rootTreeElement;
  private NativeTreeNode     treeElement;
  private DocumentType       from;
  private NativeOperator     operator;
  private NativeRuntimeValue property;
  private Object             propertyValue;
  private boolean            polymorphic = true;
  private int                limit       = -1;
  private STATE              state       = STATE.DEFAULT;

  public NativeSelect(final DatabaseInternal database) {
    this.database = database;
  }

  public NativeSelect from(final String from) {
    checkNotCompiled();
    if (this.from != null)
      throw new IllegalArgumentException("From has already been set");

    this.from = database.getSchema().getType(from);
    return this;
  }

  public NativeSelect eq() {
    return setOperator(NativeOperator.eq);
  }

  public NativeSelect lt() {
    return setOperator(NativeOperator.lt);
  }

  public NativeSelect le() {
    return setOperator(NativeOperator.le);
  }

  public NativeSelect gt() {
    return setOperator(NativeOperator.gt);
  }

  public NativeSelect ge() {
    return setOperator(NativeOperator.ge);
  }

  public NativeSelect and() {
    return setLogic(NativeOperator.and);
  }

  public NativeSelect or() {
    return setLogic(NativeOperator.or);
  }

  public NativeSelect property(final String name) {
    checkNotCompiled();
    if (property != null)
      throw new IllegalArgumentException("Property has already been set");
    if (state != STATE.WHERE)
      throw new IllegalArgumentException("No context was provided for the parameter");
    this.property = new NativePropertyValue(name);
    return this;
  }

  public NativeSelect value(final Object value) {
    checkNotCompiled();
    if (property == null)
      throw new IllegalArgumentException("Property has not been set");

    switch (state) {
    case WHERE:
      if (operator == null)
        throw new IllegalArgumentException("No operator has been set");
      if (propertyValue != null)
        throw new IllegalArgumentException("Property value has already been set");
      this.propertyValue = value;
      break;
    }

    return this;
  }

  public NativeSelect where() {
    checkNotCompiled();
    if (rootTreeElement != null)
      throw new IllegalArgumentException("Where has already been set");
    state = STATE.WHERE;
    return this;
  }

  public NativeSelect parameter(final String parameterName) {
    checkNotCompiled();
    this.propertyValue = new NativeParameterValue(this, parameterName);
    return this;
  }

  public NativeSelect parameter(final String paramName, final Object paramValue) {
    if (parameters == null)
      parameters = new HashMap<>();
    parameters.put(paramName, paramValue);
    return this;
  }

  public NativeSelect limit(final int limit) {
    checkNotCompiled();
    this.limit = limit;
    return this;
  }

  public NativeSelect polymorphic(final boolean polymorphic) {
    checkNotCompiled();
    this.polymorphic = polymorphic;
    return this;
  }

  public QueryIterator<Vertex> vertices() {
    return run();
  }

  public QueryIterator<Edge> edges() {
    return run();
  }

  public QueryIterator<Document> documents() {
    return run();
  }

  private <T extends Document> QueryIterator<T> run() {
    if (from == null)
      throw new IllegalArgumentException("from has not been set");
    if (state != STATE.COMPILED) {
      setLogic(NativeOperator.run);
      state = STATE.COMPILED;
    }

    final int[] returned = new int[] { 0 };
    final Iterator<Record> iterator = database.iterateType(from.getName(), polymorphic);
    return new QueryIterator<>() {
      private T next = null;

      @Override
      public boolean hasNext() {
        if (limit > -1 && returned[0] >= limit)
          return false;
        if (next != null)
          return true;
        if (!iterator.hasNext())
          return false;

        next = fetchNext();
        return next != null;
      }

      @Override
      public T next() {
        if (next == null && !hasNext())
          throw new NoSuchElementException();
        try {
          return next;
        } finally {
          next = null;
        }
      }

      private T fetchNext() {
        do {
          final Document record = iterator.next().asDocument();
          if (evaluateWhere(record)) {
            ++returned[0];
            return (T) record;
          }

        } while (iterator.hasNext());

        // NOT FOUND
        return null;
      }
    };
  }

  private boolean evaluateWhere(final Document record) {
    final Object result = rootTreeElement.eval(record);
    if (result instanceof Boolean)
      return (Boolean) result;
    throw new IllegalArgumentException("A boolean result was expected but '" + result + "' was returned");
  }

  private NativeSelect setLogic(final NativeOperator newLogicOperator) {
    checkNotCompiled();
    if (operator == null)
      throw new IllegalArgumentException("Missing condition");

    final NativeTreeNode newTreeElement = new NativeTreeNode(property, operator, propertyValue);
    if (rootTreeElement == null) {
      // 1ST TIME ONLY
      rootTreeElement = new NativeTreeNode(newTreeElement, newLogicOperator, null);
      newTreeElement.setParent(rootTreeElement);
    } else {
      final NativeTreeNode parent = new NativeTreeNode(newTreeElement, newLogicOperator, null);
      // REPLACE THE NODE
      treeElement.getParent().setRight(parent);
    }
    treeElement = newTreeElement;

    operator = null;
    property = null;
    propertyValue = null;
    return this;
  }

  private NativeSelect setOperator(final NativeOperator nativeOperator) {
    checkNotCompiled();
    if (operator != null)
      throw new IllegalArgumentException("Operator has already been set (" + operator + ")");
    operator = nativeOperator;
    return this;
  }

  private void checkNotCompiled() {
    if (state == STATE.COMPILED)
      throw new IllegalArgumentException("Cannot modify the structure of a select what has been already compiled");
  }
}
