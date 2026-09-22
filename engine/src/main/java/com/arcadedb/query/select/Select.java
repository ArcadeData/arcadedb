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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

  /**
   * Bounds how long one execution of this select may take. The budget covers the whole answer - index lookup
   * included - and is enforced by every plan shape, the vector k-NN search of {@link #nearestTo} among them.
   *
   * @param exceptionOnTimeout {@code true} to raise a {@link com.arcadedb.exception.TimeoutException} when the budget
   *                           runs out, {@code false} to stop and hand back whatever was <b>already produced</b>: the
   *                           records a {@link SelectIterator} had yielded, the tally {@link #count()} had reached.
   *                           #6873: on the {@code nearestTo()} path "already produced" means the results already
   *                           assembled, so an expiry before assembly starts answers with an <b>empty</b> list rather
   *                           than a truncated one - the neighbours the index searches found are RIDs and distances,
   *                           and turning any of them into a result would mean loading a record past a deadline that
   *                           has already gone.
   */
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
    } else if (json.has("fromBuckets"))
      // #6817: fromBuckets(String...) RESOLVES THE NAMES ITSELF, SO MAPPING THEM TO Bucket OBJECTS FIRST WAS BOTH
      // POINTLESS AND FATAL: List<Bucket>.toArray(new String[size]) COMPILES - <T> T[] toArray(T[])'S TYPE VARIABLE
      // IS INDEPENDENT OF THE ELEMENT TYPE - AND THEN TAKES ArrayList'S System.arraycopy BRANCH, WHICH RAISES
      // ArrayStoreException FOR EVERY NON-EMPTY BUCKET LIST. PASS THE NAMES STRAIGHT THROUGH INSTEAD
      fromBuckets(json.getJSONArray("fromBuckets").toList().stream().map(Object::toString).toArray(String[]::new));

    if (json.has("where")) {
      checkNotCompiled();
      if (rootTreeElement != null)
        throw new IllegalArgumentException("Where has already been set");
      // #8167: BUILD THE TREE THE JSON DESCRIBES INSTEAD OF REPLAYING ITS LEAVES THROUGH THE FLUENT BUILDER. THE
      // BUILDER'S setLogic() IS A PRECEDENCE MACHINE: IT DECIDES WHERE A CONDITION LANDS FROM OPERATOR PRECEDENCE
      // ALONE, SO AN 'or' ARRIVING FROM INSIDE A NESTED ARRAY WAS INDISTINGUISHABLE FROM ONE TYPED AT THE TOP LEVEL
      // AND PUSHED THE WHOLE TREE BUILT SO FAR DOWN AS ITS LEFT CHILD. THE GROUPING THE CALLER WROTE WAS GONE BEFORE
      // THE FIRST ROW WAS READ, SILENTLY: a = 2 and (b = 1 or b = 3) RAN AS (a = 2 and b = 1) or b = 3. A NESTED
      // ARRAY IS A PARENTHESIS, SO IT IS PARSED INTO ITS OWN SUBTREE AND GRAFTED IN AS ONE OPAQUE OPERAND
      rootTreeElement = parseJsonCondition(json.getJSONArray("where"));
    }

    if (json.has("limit"))
      limit(json.getInt("limit"));
    if (json.has("skip"))
      skip(json.getInt("skip"));

    // #6817: THE STATE BELOW IS WRITTEN BY SelectCompiled.json() BUT USED TO BE DROPPED HERE, SO json() AND
    // json(JSONObject) WERE NOT INVERSE - A ROUND-TRIPPED SELECT RAN WITH NO DEADLINE AND NO ORDER BY
    if (json.has("orderBy")) {
      final JSONArray parsedOrderBy = json.getJSONArray("orderBy");
      for (int i = 0; i < parsedOrderBy.length(); i++) {
        final JSONObject entry = parsedOrderBy.getJSONObject(i);
        orderBy(entry.getString("property"), entry.getBoolean("ascending", true));
      }
    }
    if (json.has("timeoutInMs"))
      timeout(json.getLong("timeoutInMs"), TimeUnit.MILLISECONDS, json.getBoolean("exceptionOnTimeout", false));
    if (json.getBoolean("parallel", false))
      // NOT VIA SelectCompiled.parallel(): THAT IS ONLY REACHABLE AFTER compile(), AND THIS SELECT IS STILL BEING BUILT
      parallel = true;

    return this;
  }

  /**
   * Parses one JSON condition - {@code [left, operator, right]}, the unary {@code [left, operator]}, or the
   * one-element {@code [inner]} that {@link SelectCompiled#json()} writes for the synthetic {@code run} root - into
   * the subtree it denotes, and answers that subtree's root. Nesting is structural: a nested array becomes a node,
   * never a replay of its leaves through the precedence rules (#8167).
   */
  private SelectTreeNode parseJsonCondition(final JSONArray condition) {
    // #6817: A SELECT WITH A SINGLE WHERE LEAF SERIALIZES AS A ONE-ELEMENT ARRAY - compile() WRAPS THE LEAF IN A
    // SYNTHETIC `run` ROOT WHOSE OPERATOR SelectTreeNode.toJSON() DELIBERATELY OMITS AND WHOSE RIGHT SIDE IS null.
    // REJECTING THAT SHAPE MADE THE COMMONEST SELECT OF ALL IMPOSSIBLE TO READ BACK, SO UNWRAP IT INSTEAD: THE SAME
    // `run` ROOT IS REBUILT HERE, WHICH IS WHAT MAKES THE TWO json METHODS INVERSE FOR THIS SHAPE
    if (condition.length() == 1 && condition.get(0) instanceof JSONArray nested)
      return new SelectTreeNode(parseJsonCondition(nested), SelectOperator.run, null);

    if (condition.length() != 2 && condition.length() != 3)
      throw new IllegalArgumentException("Invalid condition " + condition
          + ": expected [left, operator, right], the unary [left, operator], or a single nested condition");

    final String parsedOperatorName = condition.getString(1);
    final SelectOperator parsedOperator = SelectOperator.byName(parsedOperatorName);
    if (parsedOperator == null)
      throw new IllegalArgumentException("Unsupported operator '" + parsedOperatorName + "' in condition " + condition);

    // #8173: THE ARITY GATE USED TO COME FIRST AND REFUSE EVERY NON-TRIPLE WITH A GENERIC MESSAGE, WHICH IS WHAT
    // SelectCompiled.json() ITSELF EMITS FOR A `not` NODE (ITS RIGHT OPERAND IS null, SO toJSON() WRITES TWO
    // ELEMENTS). THE HELPFUL MESSAGE #8059 ADDED FOR `not` SAT BELOW THE GATE AND WAS THEREFORE UNREACHABLE FOR THE
    // ONE SHAPE THAT NEEDED IT. THE OPERATOR IS NOW READ BEFORE THE ARITY IS JUDGED, SO EACH ARITY ERROR NAMES ITS
    // OWN CAUSE - AND THE UNARY SHAPE IS ACCEPTED, WHICH CLOSES THE json() -> json(JSONObject) ROUND TRIP #6817
    // ESTABLISHED FOR EVERY OPERATOR INSTEAD OF ALL BUT ONE
    final boolean unary = parsedOperator == SelectOperator.not;
    if (unary && condition.length() == 3)
      throw new IllegalArgumentException(
          "Operator 'not' is unary and takes no right operand: write it as [left, \"not\"], not as "
              + "[left, \"not\", right]. To negate a comparison, use the complementary operator instead "
              + "(for example '<>' for '=', 'is null' for 'is not null')");
    if (!unary && condition.length() == 2)
      throw new IllegalArgumentException("Operator '" + parsedOperatorName
          + "' is binary and requires a right operand: " + condition);

    final Object left = parseJsonOperand(condition.get(0), true);
    final Object right = unary ? null : parseJsonOperand(condition.get(2), false);

    return new SelectTreeNode(left, parsedOperator, adaptRightOperand(parsedOperator, right));
  }

  /**
   * One operand of a JSON condition: a nested CONDITION array is its own subtree, {@code ":name"} a property,
   * {@code "#name"} a parameter, anything else a literal. A literal is only legal on the right: the left of a
   * condition is what is being tested.
   */
  private Object parseJsonOperand(final Object operand, final boolean leftSide) {
    if (operand instanceof JSONArray array) {
      if (leftSide || isConditionArray(array))
        return parseJsonCondition(array);
      // A VALUE array, not a condition: the range of a `between`, the candidates of an `in`. SelectTreeNode.toJSON()
      // writes both as a plain JSON array of literals, and treating every right-hand array as a nested condition is
      // what stopped those two operators round-tripping at all (found alongside #8173).
      return array.toList();
    }
    if (operand instanceof String string && string.startsWith(":"))
      return new SelectPropertyValue(string.substring(1));
    if (operand instanceof String string && string.startsWith("#"))
      // #8167: THE LEFT SIDE USED TO ROUTE THIS THROUGH parameter(), WHICH ASSIGNS THE FLUENT BUILDER'S
      // propertyValue - THE *RIGHT*-HAND SLOT - SO A PARAMETER WRITTEN ON THE LEFT LANDED ON THE WRONG SIDE
      return new SelectParameterValue(this, string.substring(1));
    if (leftSide)
      throw new IllegalArgumentException("Unsupported value " + operand);
    return operand;
  }

  /**
   * Whether a JSON array is a CONDITION - the one-element {@code run} wrapper, or a 2/3-element array whose middle
   * element names an operator - rather than a list of values. Only the right-hand side of a condition can hold
   * either, and the two are told apart by shape because the format gives them the same syntax.
   */
  private static boolean isConditionArray(final JSONArray array) {
    if (array.length() == 1)
      return array.get(0) instanceof JSONArray;
    if (array.length() != 2 && array.length() != 3)
      return false;
    return array.get(1) instanceof String operatorName && SelectOperator.byName(operatorName) != null;
  }

  /**
   * {@code between} evaluates its right operand as an {@code Object[]} of exactly two bounds
   * ({@link SelectWhereBetweenBlock#values}), which is the one operator whose in-memory value shape is not what a
   * JSON array parses to. Every other operator taking a list - {@code in} - is happy with a {@link List}.
   */
  private static Object adaptRightOperand(final SelectOperator operator, final Object right) {
    if (operator == SelectOperator.between && right instanceof List<?> list)
      return list.toArray();
    return right;
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

  public Stream<Document> stream() {
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
        // OR+ OPERATOR: THE NEW OPERATOR DOES NOT BIND MORE TIGHTLY THAN THE ONE ALREADY THERE, SO THE CURRENT
        // SUBTREE IS PUSHED DOWN AND BECOMES THE LEFT CHILD OF A NODE CARRYING THE NEW OPERATOR
        final SelectTreeNode currentParent = lastTreeElement.getParent();
        // #8047: READ THE GRANDPARENT *BEFORE* THE SelectTreeNode CONSTRUCTOR RE-PARENTS currentParent UNDER newNode.
        // THE CONSTRUCTOR ALREADY DOES TWO OF THE THREE THINGS THIS BRANCH NEEDS - IT SETS currentParent.parent TO
        // newNode AND, THROUGH SelectTreeNode.setParent, MOVES THE GRANDPARENT'S OWN CHILD POINTER OVER TO newNode -
        // SO THE ONLY THING LEFT IS newNode'S UPWARD LINK. READING currentParent.getParent() *AFTER* THE CONSTRUCTOR
        // HANDED BACK newNode ITSELF, SO THE CALL REDUCED TO newNode.setParent(newNode) AND INSTALLED A SELF-PARENT
        // CYCLE. THAT CYCLE THEN DEFEATED THE *NEXT* RE-PARENTING - setParent REWIRES THE GRANDPARENT BY TESTING
        // this.parent.left/right == this, AND ON A SELF-PARENTED NODE BOTH TESTS COMPARE THE NODE AGAINST ITS OWN
        // CHILDREN AND FAIL - SO THE NEXT NODE, AND EVERYTHING APPENDED TO IT, DANGLED OFF THE TREE rootTreeElement
        // REFERS TO AND THE EXECUTOR NEVER SAW IT. FROM THE FIFTH CONDITION ON, AN 'OR' FOLLOWED BY THREE OR MORE
        // 'AND's SILENTLY DISCARDED EVERY REMAINING CONDITION AND THE QUERY RETURNED MORE ROWS THAN THE PREDICATE
        // ALLOWS
        final SelectTreeNode grandParent = currentParent.getParent();
        currentParent.setRight(newTreeElement);
        final SelectTreeNode newNode = new SelectTreeNode(currentParent, newLogicOperator, null);
        if (rootTreeElement.equals(currentParent))
          rootTreeElement = newNode;
        else
          newNode.setParent(grandParent);
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
