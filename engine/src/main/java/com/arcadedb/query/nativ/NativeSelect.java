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
import com.arcadedb.engine.Bucket;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

/**
 * Native Query engine is a simple query engine that covers most of the classic use cases, such as the retrieval of records
 * with a where condition. It could be much faster than the same SQL query because it does not use any parser and it is very
 * JIT friendly. Future versions could translate the query into bytecode to have an even faster execution.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class NativeSelect {
  final DatabaseInternal database;

  enum STATE {DEFAULT, WHERE, COMPILED}

  Map<String, Object> parameters;

  NativeTreeNode     rootTreeElement;
  DocumentType       fromType;
  List<Bucket>       fromBuckets;
  NativeOperator     operator;
  NativeRuntimeValue property;
  Object             propertyValue;
  boolean            polymorphic = true;
  int                limit       = -1;
  long               timeoutValue;
  TimeUnit           timeoutUnit;
  boolean            exceptionOnTimeout;

  private STATE          state = STATE.DEFAULT;
  private NativeTreeNode lastTreeElement;

  public NativeSelect(final DatabaseInternal database) {
    this.database = database;
  }

  public NativeSelect fromType(final String fromType) {
    checkNotCompiled();
    if (this.fromType != null)
      throw new IllegalArgumentException("From type has already been set");
    if (this.fromBuckets != null)
      throw new IllegalArgumentException("From bucket(s) has already been set");

    this.fromType = database.getSchema().getType(fromType);
    return this;
  }

  public NativeSelect fromBuckets(final String... fromBucketNames) {
    checkNotCompiled();
    if (this.fromType != null)
      throw new IllegalArgumentException("From type has already been set");
    if (this.fromBuckets != null)
      throw new IllegalArgumentException("From bucket(s) has already been set");

    this.fromBuckets = Arrays.stream(fromBucketNames).map(b -> database.getSchema().getBucketByName(b))
        .collect(Collectors.toList());
    return this;
  }

  public NativeSelect fromBuckets(final Integer... fromBucketIds) {
    checkNotCompiled();
    if (this.fromType != null)
      throw new IllegalArgumentException("From type has already been set");
    if (this.fromBuckets != null)
      throw new IllegalArgumentException("From bucket(s) has already been set");

    this.fromBuckets = Arrays.stream(fromBucketIds).map(b -> database.getSchema().getBucketById(b)).collect(Collectors.toList());
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

  public NativeSelect timeout(final long timeoutValue, final TimeUnit timeoutUnit, final boolean exceptionOnTimeout) {
    checkNotCompiled();
    this.timeoutValue = timeoutValue;
    this.timeoutUnit = timeoutUnit;
    this.exceptionOnTimeout = exceptionOnTimeout;
    return this;
  }

  public NativeSelect polymorphic(final boolean polymorphic) {
    checkNotCompiled();
    if (fromType == null)
      throw new IllegalArgumentException("FromType was not set");
    this.polymorphic = polymorphic;
    return this;
  }

  public NativeSelect json(final JSONObject json) {
    checkNotCompiled();
    if (json.has("fromType")) {
      fromType(json.getString("fromType"));
      if (json.has("polymorphic"))
        polymorphic(json.getBoolean("polymorphic"));
    } else if (json.has("fromBuckets")) {
      final JSONArray buckets = json.getJSONArray("fromBuckets");
      fromBuckets(
          buckets.toList().stream().map(b -> database.getSchema().getBucketByName(b.toString())).collect(Collectors.toList())
              .toArray(new String[buckets.length()]));
    }

    if (json.has("where")) {
      where();
      parseJsonCondition(json.getJSONArray("where"));
    }

    if (json.has("limit"))
      limit(json.getInt("limit"));

    return this;
  }

  private void parseJsonCondition(final JSONArray condition) {
    if (condition.length() != 3)
      throw new IllegalArgumentException("Invalid condition " + condition);

    final Object parsedLeft = condition.get(0);
    if (parsedLeft instanceof JSONArray)
      parseJsonCondition((JSONArray) parsedLeft);
    else if (parsedLeft instanceof String && ((String) parsedLeft).startsWith(":"))
      property(((String) parsedLeft).substring(1));
    else if (parsedLeft instanceof String && ((String) parsedLeft).startsWith("#"))
      parameter(((String) parsedLeft).substring(1));
    else
      throw new IllegalArgumentException("Unsupported value " + parsedLeft);

    final NativeOperator parsedOperator = NativeOperator.byName(condition.getString(1));

    if (parsedOperator.logicOperator)
      setLogic(parsedOperator);
    else
      setOperator(parsedOperator);

    final Object parsedRight = condition.get(2);
    if (parsedRight instanceof JSONArray)
      parseJsonCondition((JSONArray) parsedRight);
    else if (parsedRight instanceof String && ((String) parsedRight).startsWith(":"))
      property(((String) parsedRight).substring(1));
    else if (parsedRight instanceof String && ((String) parsedRight).startsWith("#"))
      parameter(((String) parsedRight).substring(1));
    else
      value(parsedRight);
  }

  public JSONObject json() {
    if (state != STATE.COMPILED)
      parse();

    final JSONObject json = new JSONObject();

    if (fromType != null) {
      json.put("fromType", fromType.getName());
      if (!polymorphic)
        json.put("polymorphic", polymorphic);
    } else if (fromBuckets != null)
      json.put("fromBuckets", fromBuckets.stream().map(b -> b.getName()).collect(Collectors.toList()));

    if (rootTreeElement != null)
      json.put("where", rootTreeElement.toJSON());

    if (limit > -1)
      json.put("limit", limit);

    return json;
  }

  public NativeSelect parse() {
    if (fromType == null && fromBuckets == null)
      throw new IllegalArgumentException("from (type or buckets) has not been set");
    if (state != STATE.COMPILED) {
      setLogic(NativeOperator.run);
      state = STATE.COMPILED;
    }
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
    parse();
    return new NativeSelectExecutor(this).execute();
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
      lastTreeElement = newTreeElement;
    } else {
      if (newLogicOperator.equals(NativeOperator.run)) {
        // EXECUTION = LAST NODE: APPEND TO THE RIGHT OF THE LATEST
        lastTreeElement.getParent().setRight(newTreeElement);
      } else if (lastTreeElement.getParent().operator.precedence < newLogicOperator.precedence) {
        // AND+ OPERATOR
        final NativeTreeNode newNode = new NativeTreeNode(newTreeElement, newLogicOperator, null);
        lastTreeElement.getParent().setRight(newNode);
        lastTreeElement = newTreeElement;
      } else {
        // OR+ OPERATOR
        final NativeTreeNode currentParent = lastTreeElement.getParent();
        currentParent.setRight(newTreeElement);
        final NativeTreeNode newNode = new NativeTreeNode(currentParent, newLogicOperator, null);
        if (rootTreeElement.equals(currentParent))
          rootTreeElement = newNode;
        else
          newNode.setParent(currentParent.getParent());
        lastTreeElement = currentParent;
      }
    }

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
