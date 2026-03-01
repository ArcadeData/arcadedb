package com.arcadedb.query.select;/*
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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.engine.Bucket;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.Pair;

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
public class Select {
  final DatabaseInternal database;

  enum STATE {DEFAULT, WHERE, COMPILED}

  Map<String, Object>              parameters;
  SelectTreeNode                   rootTreeElement;
  DocumentType                     fromType;
  List<Bucket>                     fromBuckets;
  SelectOperator                   operator;
  SelectRuntimeValue               property;
  Object                           propertyValue;
  boolean                          polymorphic = true;
  int                              limit       = -1;
  int                              skip        = 0;
  long                             timeoutInMs = 0;
  boolean                          exceptionOnTimeout;
  ArrayList<Pair<String, Boolean>> orderBy;
  boolean                          parallel    = false;

  // Vector k-NN search fields
  String                           vectorProperty;
  float[]                          vectorQuery;
  int                              vectorK;
  boolean                          vectorApproximate;

  STATE state = STATE.DEFAULT;
  private SelectTreeNode lastTreeElement;

  public Select(final DatabaseInternal database) {
    this.database = database;
  }

  public Select fromType(final String fromType) {
    checkNotCompiled();
    if (this.fromType != null)
      throw new IllegalArgumentException("From type has already been set");
    if (this.fromBuckets != null)
      throw new IllegalArgumentException("From bucket(s) has already been set");

    this.fromType = database.getSchema().getType(fromType);
    return this;
  }

  public Select fromBuckets(final String... fromBucketNames) {
    checkNotCompiled();
    if (this.fromType != null)
      throw new IllegalArgumentException("From type has already been set");
    if (this.fromBuckets != null)
      throw new IllegalArgumentException("From bucket(s) has already been set");

    this.fromBuckets = Arrays.stream(fromBucketNames).map(b -> database.getSchema().getBucketByName(b))
        .collect(Collectors.toList());
    return this;
  }

  public Select fromBuckets(final Integer... fromBucketIds) {
    checkNotCompiled();
    if (this.fromType != null)
      throw new IllegalArgumentException("From type has already been set");
    if (this.fromBuckets != null)
      throw new IllegalArgumentException("From bucket(s) has already been set");

    this.fromBuckets = Arrays.stream(fromBucketIds).map(b -> database.getSchema().getBucketById(b)).collect(Collectors.toList());
    return this;
  }

  public Select property(final String name) {
    checkNotCompiled();
    if (property != null)
      throw new IllegalArgumentException("Property has already been set");
    if (state != STATE.WHERE)
      throw new IllegalArgumentException("No context was provided for the parameter");
    this.property = new SelectPropertyValue(name);
    return this;
  }

  public Select value(final Object value) {
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

  public SelectWhereLeftBlock where() {
    checkNotCompiled();
    if (rootTreeElement != null)
      throw new IllegalArgumentException("Where has already been set");
    state = STATE.WHERE;
    return new SelectWhereLeftBlock(this);
  }

  public Select parameter(final String parameterName) {
    checkNotCompiled();
    this.propertyValue = new SelectParameterValue(this, parameterName);
    return this;
  }

  public Select limit(final int limit) {
    checkNotCompiled();
    this.limit = limit;
    return this;
  }

  public Select skip(final int skip) {
    checkNotCompiled();
    this.skip = skip;
    return this;
  }

  public Select timeout(final long timeoutValue, final TimeUnit timeoutUnit, final boolean exceptionOnTimeout) {
    checkNotCompiled();
    this.timeoutInMs = timeoutUnit.toMillis(timeoutValue);
    this.exceptionOnTimeout = exceptionOnTimeout;
    return this;
  }

  public Select polymorphic(final boolean polymorphic) {
    checkNotCompiled();
    if (fromType == null)
      throw new IllegalArgumentException("FromType was not set");
    this.polymorphic = polymorphic;
    return this;
  }

  public Select orderBy(final String property, final boolean ascending) {
    checkNotCompiled();
    if (this.orderBy == null)
      this.orderBy = new ArrayList<>();
    this.orderBy.add(new Pair<>(property, ascending));
    return this;
  }

  public Select json(final JSONObject json) {
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
    if (json.has("skip"))
      skip(json.getInt("skip"));

    return this;
  }

  private void parseJsonCondition(final JSONArray condition) {
    if (condition.length() != 3)
      throw new IllegalArgumentException("Invalid condition " + condition);

    final Object parsedLeft = condition.get(0);
    if (parsedLeft instanceof JSONArray array)
      parseJsonCondition(array);
    else if (parsedLeft instanceof String string && string.startsWith(":"))
      property(string.substring(1));
    else if (parsedLeft instanceof String string && string.startsWith("#"))
      parameter(string.substring(1));
    else
      throw new IllegalArgumentException("Unsupported value " + parsedLeft);

    final SelectOperator parsedOperator = SelectOperator.byName(condition.getString(1));

    if (parsedOperator.logicOperator)
      setLogic(parsedOperator);
    else
      setOperator(parsedOperator);

    final Object parsedRight = condition.get(2);
    if (parsedRight instanceof JSONArray array)
      parseJsonCondition(array);
    else if (parsedRight instanceof String string && string.startsWith(":"))
      property(string.substring(1));
    else if (parsedRight instanceof String string && string.startsWith("#"))
      parameter(string.substring(1));
    else
      value(parsedRight);
  }

  public SelectCompiled compile() {
    if (fromType == null && fromBuckets == null)
      throw new IllegalArgumentException("from (type or buckets) has not been set");
    if (state == STATE.WHERE) {
      setLogic(SelectOperator.run);
    }
    state = STATE.COMPILED;
    return new SelectCompiled(this);
  }

  public SelectIterator<Vertex> vertices() {
    return run();
  }

  public SelectIterator<Edge> edges() {
    return run();
  }

  public SelectIterator<Document> documents() {
    return run();
  }

  public long count() {
    compile();
    return new SelectExecutor(this).executeCount();
  }

  public boolean exists() {
    compile();
    return new SelectExecutor(this).executeExists();
  }

  public java.util.stream.Stream<Document> stream() {
    return documents().stream();
  }

  public SelectVectorBuilder nearestTo(final String property, final float[] queryVector, final int k) {
    checkNotCompiled();
    if (fromType == null)
      throw new IllegalArgumentException("FromType must be set before calling nearestTo()");
    this.vectorProperty = property;
    this.vectorQuery = queryVector;
    this.vectorK = k;
    return new SelectVectorBuilder(this);
  }

  <T extends Document> SelectIterator<T> run() {
    compile();
    return new SelectExecutor(this).execute();
  }

  SelectWhereLeftBlock setLogic(final SelectOperator newLogicOperator) {
    checkNotCompiled();
    if (operator == null)
      throw new IllegalArgumentException("Missing condition");

    final SelectTreeNode newTreeElement = new SelectTreeNode(property, operator, propertyValue);
    if (rootTreeElement == null) {
      // 1ST TIME ONLY
      rootTreeElement = new SelectTreeNode(newTreeElement, newLogicOperator, null);
      newTreeElement.setParent(rootTreeElement);
      lastTreeElement = newTreeElement;
    } else {
      if (newLogicOperator.equals(SelectOperator.run)) {
        // EXECUTION = LAST NODE: APPEND TO THE RIGHT OF THE LATEST
        lastTreeElement.getParent().setRight(newTreeElement);
      } else if (lastTreeElement.getParent().operator.precedence < newLogicOperator.precedence) {
        // AND+ OPERATOR
        final SelectTreeNode newNode = new SelectTreeNode(newTreeElement, newLogicOperator, null);
        lastTreeElement.getParent().setRight(newNode);
        lastTreeElement = newTreeElement;
      } else {
        // OR+ OPERATOR
        final SelectTreeNode currentParent = lastTreeElement.getParent();
        currentParent.setRight(newTreeElement);
        final SelectTreeNode newNode = new SelectTreeNode(currentParent, newLogicOperator, null);
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
    return new SelectWhereLeftBlock(this);
  }

  void checkNotCompiled() {
    if (state == STATE.COMPILED)
      throw new IllegalArgumentException("Cannot modify the structure of a select what has been already compiled");
  }

  SelectWhereRightBlock setOperator(final SelectOperator selectOperator) {
    checkNotCompiled();
    if (operator != null)
      throw new IllegalArgumentException("Operator has already been set (" + operator + ")");
    operator = selectOperator;
    return new SelectWhereRightBlock(this);
  }
}
