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
package com.arcadedb.mongo;

import de.bwaldvogel.mongo.backend.BsonType;
import de.bwaldvogel.mongo.backend.Missing;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.backend.ValueComparator;
import de.bwaldvogel.mongo.backend.aggregation.Expression;
import de.bwaldvogel.mongo.bson.BinData;
import de.bwaldvogel.mongo.bson.BsonRegularExpression;
import de.bwaldvogel.mongo.bson.BsonTimestamp;
import de.bwaldvogel.mongo.bson.Decimal128;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.LegacyUUID;
import de.bwaldvogel.mongo.bson.MaxKey;
import de.bwaldvogel.mongo.bson.MinKey;
import de.bwaldvogel.mongo.bson.ObjectId;
import de.bwaldvogel.mongo.exception.BadValueException;
import de.bwaldvogel.mongo.exception.FailedToParseException;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.exception.MongoServerNotYetImplementedException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Tests a document against a MongoDB query filter with the rules of {@code mongod} (issue #12074). The plugin used the
 * {@code DefaultQueryMatcher} of the embedded mongo-java-server, which answers differently from {@code mongod} on paths through
 * arrays and on several operators, and whose private methods leave nothing to override, so the matching is done here.
 * <p>
 * The model is the one of {@code mongod}:
 * <ul>
 *   <li>a path is expanded into the values it reaches: an embedded document is entered by name; an array is entered through
 *   each of its embedded documents, and a numeric segment also reads the element at that position; a scalar of an array is not
 *   entered (it reaches nothing), a scalar or a null outside an array reaches a missing value;</li>
 *   <li>a predicate holds when one of those values satisfies it, and a value that is an array also offers each of its elements
 *   (one level) and, for an equality or a comparison with an array operand, the array itself;</li>
 *   <li>a negation ({@code $ne}, {@code $nin}, {@code $not}) is the negation of the positive predicate over the whole path;</li>
 *   <li>equality to {@code null} matches a null and a missing value, and so do {@code $gte} / {@code $lte} with {@code null};</li>
 *   <li>a comparison only compares values of the same type bracket (every number is one bracket, compared exactly across int,
 *   long, double and decimal; NaN is only equal to NaN and neither greater nor less than anything); arrays and documents are
 *   compared element by element, in order;</li>
 *   <li>a regular expression ({@code /x/}, {@code $regex}, an item of {@code $in}, {@code $nin} or {@code $all}, {@code $not})
 *   matches a string it is found in, and a stored regular expression with the same pattern and options; a regular expression
 *   operand of {@code $eq}, or inside an array or an embedded document operand, is a value, equal only to such a stored regular
 *   expression; {@code $ne} and the range operators refuse a regular expression operand (issue #12594);</li>
 *   <li>{@code $mod} computes on 64-bit integers, truncating a fractional value, and a value whose integer part does not fit
 *   in them matches nothing;</li>
 *   <li>an embedded document operand is equal to a document with the same fields in the same order, {@code {a: {}}} only to
 *   an empty document;</li>
 *   <li>the condition of a value-form {@code $elemMatch} applies to the element itself, an array element is not entered.</li>
 * </ul>
 * A malformed filter is refused by {@link #validate(Document)} before any document is tested, as {@code mongod} refuses it when
 * it parses the filter: an evaluation that stops at the first false condition never reaches the unknown operator after it.
 * Not thread-safe: one instance belongs to one filter, evaluated by one thread.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class MongoQueryMatcher {
  private static final Object      MISSING    = Missing.getInstance();
  private static final Set<String> QUERY_FORM = Set.of("$and", "$or", "$nor", "$expr", "$comment");
  private static final Set<String> REFERENCE  = Set.of("$ref", "$id", "$db");

  // the predicates a value is tested with, kept as constants rather than lambdas: no allocation per document
  private static final int EQUALS  = 0;
  private static final int NULLISH = 1;
  private static final int REGEX   = 2;
  private static final int GT      = 3;
  private static final int GTE     = 4;
  private static final int LT      = 5;
  private static final int LTE     = 6;
  private static final int TYPE    = 7;
  private static final int SIZE    = 8;
  private static final int EXISTS  = 9;
  private static final int MOD     = 10;

  // the order of the type brackets of mongod, MinKey first and MaxKey last
  private static final int MIN_KEY = 0;
  private static final int OTHER   = 99;
  private static final int MAX_KEY = 127;

  // the bits of a Decimal128 that mark NaN and the infinities, in its high word
  private static final long DECIMAL_SPECIAL  = 0x7C00000000000000L;
  private static final long DECIMAL_INFINITY = 0x7800000000000000L;

  private final Map<String, String[]> paths = new HashMap<>();

  /** True while the condition of a value-form {@code $elemMatch} is tested on one element: the element is not entered. */
  private boolean element;

  // ── Validation ─────────────────────────────────────────────────────────────

  /** Refuses a malformed filter, whatever a document would answer: an unknown operator, an operand of the wrong shape. */
  void validate(final Document query) {
    for (final Map.Entry<String, Object> entry : query.entrySet()) {
      final String key = entry.getKey();
      final Object operand = entry.getValue();
      switch (key) {
      case "$and", "$or", "$nor" -> {
        for (final Document branch : branches(key, operand))
          validate(branch);
      }
      case "$expr", "$comment" -> {
        // an expression is evaluated by the library, a comment selects nothing
      }
      default -> {
        if (key.startsWith("$"))
          throw unknownTopLevelOperator(key);
        if (operand instanceof Document document && isOperatorDocument(document))
          validateOperators(key, document);
      }
      }
    }
  }

  /**
   * @param field the path the operators apply to, named by mongod's refusal of a regular expression under a range operator; for the
   *              operators of a value-form {@code $elemMatch}, which apply to an element, the {@code (None)} mongod names
   */
  private void validateOperators(final String field, final Document operators) {
    for (final Map.Entry<String, Object> entry : operators.entrySet()) {
      final String operator = entry.getKey();
      final Object operand = entry.getValue();
      switch (operator) {
      case "$eq", "$exists" -> {
        // any operand
      }
      case "$ne" -> {
        // mongod refuses it, although $eq compares one as a value
        if (operand instanceof BsonRegularExpression)
          throw new BadValueException("Can't have regex as arg to $ne.");
      }
      case "$gt", "$gte", "$lt", "$lte" -> {
        if (operand instanceof BsonRegularExpression)
          throw new BadValueException("Can't have RegEx as arg to non-equality predicate over field '" + field + "'.");
      }
      case "$in" -> list(operand, "$in needs an array");
      case "$nin" -> list(operand, "$nin needs an array");
      case "$all" -> {
        for (final Object item : list(operand, "$all needs an array"))
          if (item instanceof Document document && document.size() == 1 && document.containsKey("$elemMatch"))
            validateElemMatch(document.get("$elemMatch"));
      }
      case "$type" -> validateTypes(types(operand));
      case "$size" -> size(operand);
      case "$elemMatch" -> validateElemMatch(operand);
      case "$mod" -> modArguments(operand);
      case "$not" -> {
        if (operand instanceof Document document && isOperatorDocument(document))
          validateOperators(field, document);
        else if (!(operand instanceof BsonRegularExpression))
          throw new BadValueException("$not needs a regex or a document");
      }
      case "$regex" -> regexOf(operators);
      case "$options" -> {
        if (!operators.containsKey("$regex"))
          throw new BadValueException("$options needs a $regex");
      }
      case "$nearSphere", "$near", "$geoWithin", "$geoIntersects" -> throw new MongoServerNotYetImplementedException(132, operator);
      default -> throw new BadValueException("unknown operator: " + operator);
      }
    }
  }

  private void validateElemMatch(final Object operand) {
    final Document condition = elemMatchCondition(operand);
    if (isValueForm(condition))
      validateOperators("(None)", condition);
    else
      validate(condition);
  }

  private static void validateTypes(final Object types) {
    if (types instanceof Collection<?> collection) {
      for (final Object type : collection)
        bsonTypeOf(type);
    } else
      bsonTypeOf(types);
  }

  // ── Evaluation ─────────────────────────────────────────────────────────────

  boolean matches(final Document document, final Document query) {
    return matchesTarget(document, query);
  }

  /**
   * Tests a document, or an array element of an object-form {@code $elemMatch}: an array element is read through the numeric
   * names of its positions, as {@code mongod} does.
   */
  private boolean matchesTarget(final Object target, final Document query) {
    for (final Map.Entry<String, Object> entry : query.entrySet()) {
      final String key = entry.getKey();
      final Object operand = entry.getValue();
      switch (key) {
      case "$and" -> {
        for (final Document branch : branches(key, operand))
          if (!matchesTarget(target, branch))
            return false;
      }
      case "$or" -> {
        boolean any = false;
        for (final Document branch : branches(key, operand))
          if (matchesTarget(target, branch)) {
            any = true;
            break;
          }
        if (!any)
          return false;
      }
      case "$nor" -> {
        for (final Document branch : branches(key, operand))
          if (matchesTarget(target, branch))
            return false;
      }
      case "$expr" -> {
        if (!(target instanceof Document document) || !Utils.isTrue(Expression.evaluateDocument(operand, document)))
          return false;
      }
      case "$comment" -> {
        // a comment selects nothing
      }
      default -> {
        if (key.startsWith("$"))
          throw unknownTopLevelOperator(key);
        if (!matchesPath(values(target, path(key)), operand))
          return false;
      }
      }
    }
    return true;
  }

  private static BadValueException unknownTopLevelOperator(final String key) {
    return new BadValueException("unknown top level operator: " + key
        + ". If you have a field name that starts with a '$' symbol, consider using $getField or $setField.");
  }

  private static List<Document> branches(final String operator, final Object operand) {
    if (!(operand instanceof List<?> list) || list.isEmpty())
      throw new BadValueException(operator + " must be a nonempty array");
    final List<Document> branches = new ArrayList<>(list.size());
    for (final Object item : list) {
      if (!(item instanceof Document document))
        throw new MongoServerError(14817, operator + " elements must be objects");
      branches.add(document);
    }
    return branches;
  }

  /** The condition of one path: a regular expression, an operator document, or a value the path must be equal to. */
  private boolean matchesPath(final List<Object> values, final Object operand) {
    if (operand instanceof BsonRegularExpression regex)
      return any(values, REGEX, regex, false);
    if (operand instanceof Document document && isOperatorDocument(document))
      return matchesOperators(values, document);
    return equalTo(values, operand);
  }

  private boolean matchesOperators(final List<Object> values, final Document operators) {
    for (final Map.Entry<String, Object> entry : operators.entrySet()) {
      final String operator = entry.getKey();
      final Object operand = entry.getValue();
      final boolean holds = switch (operator) {
        case "$eq" -> equalTo(values, operand);
        case "$ne" -> !equalTo(values, operand);
        case "$gt" -> compared(values, GT, operand);
        case "$gte" -> compared(values, GTE, operand);
        case "$lt" -> compared(values, LT, operand);
        case "$lte" -> compared(values, LTE, operand);
        case "$in" -> in(values, list(operand, "$in needs an array"));
        case "$nin" -> !in(values, list(operand, "$nin needs an array"));
        case "$exists" -> any(values, EXISTS, null, false) == Utils.isTrue(operand);
        case "$type" -> any(values, TYPE, types(operand), true);
        case "$size" -> any(values, SIZE, size(operand), false);
        case "$all" -> all(values, list(operand, "$all needs an array"));
        case "$elemMatch" -> elemMatch(values, operand);
        case "$mod" -> any(values, MOD, modArguments(operand), false);
        case "$not" -> !not(values, operand);
        case "$regex" -> any(values, REGEX, regexOf(operators), false);
        case "$options" -> {
          if (!operators.containsKey("$regex"))
            throw new BadValueException("$options needs a $regex");
          yield true;
        }
        case "$nearSphere", "$near", "$geoWithin", "$geoIntersects" -> throw new MongoServerNotYetImplementedException(132, operator);
        default -> throw new BadValueException("unknown operator: " + operator);
      };
      if (!holds)
        return false;
    }
    return true;
  }

  private boolean not(final List<Object> values, final Object operand) {
    if (operand instanceof BsonRegularExpression regex)
      return any(values, REGEX, regex, false);
    if (operand instanceof Document document && isOperatorDocument(document))
      return matchesOperators(values, document);
    throw new BadValueException("$not needs a regex or a document");
  }

  private static BsonRegularExpression regexOf(final Document operators) {
    final Object regex = operators.get("$regex");
    final Object options = operators.get("$options");
    if (regex instanceof BsonRegularExpression literal)
      return new BsonRegularExpression(literal.getPattern(), options != null ? options.toString() : literal.getOptions());
    if (!(regex instanceof String pattern))
      throw new BadValueException("$regex has to be a string");
    return new BsonRegularExpression(pattern, options != null ? options.toString() : null);
  }

  private static List<?> list(final Object operand, final String message) {
    if (!(operand instanceof List<?> list))
      throw new BadValueException(message);
    return list;
  }

  /** Equality to a value: null also matches a missing value, a list operand matches the array itself or an element equal to it. */
  private boolean equalTo(final List<Object> values, final Object operand) {
    if (operand == null || operand instanceof Missing)
      return any(values, NULLISH, null, false);
    return any(values, EQUALS, operand, true);
  }

  private boolean compared(final List<Object> values, final int comparison, final Object operand) {
    if (operand == null || operand instanceof Missing)
      // null is equal to null and to a missing value, and nothing is greater or less than it
      return (comparison == GTE || comparison == LTE) && any(values, NULLISH, null, false);
    return any(values, comparison, operand, operand instanceof List);
  }

  private boolean in(final List<Object> values, final List<?> operands) {
    for (final Object operand : operands)
      if (operand instanceof BsonRegularExpression regex ? any(values, REGEX, regex, false) : equalTo(values, operand))
        return true;
    return false;
  }

  /** {@code $all}: every item holds, an equality or an {@code $elemMatch}; no item at all selects nothing. */
  private boolean all(final List<Object> values, final List<?> operands) {
    if (operands.isEmpty())
      return false;
    for (final Object operand : operands) {
      final boolean holds;
      if (operand instanceof Document document && document.size() == 1 && document.containsKey("$elemMatch"))
        holds = elemMatch(values, document.get("$elemMatch"));
      else if (operand instanceof BsonRegularExpression regex)
        holds = any(values, REGEX, regex, false);
      else
        holds = equalTo(values, operand);
      if (!holds)
        return false;
    }
    return true;
  }

  /**
   * {@code $elemMatch}: a value that is an array has an element satisfying the condition. A condition made of operators applies
   * to the element itself, which is not entered when it is an array; any other condition is a query on the element, a document
   * or an array read through the numeric names of its positions (a scalar element has no field).
   */
  private boolean elemMatch(final List<Object> values, final Object operand) {
    final Document condition = elemMatchCondition(operand);
    final boolean onTheValue = isValueForm(condition);
    final boolean wasElement = element;
    try {
      for (final Object value : values) {
        if (!(value instanceof List<?> elements))
          continue;
        for (final Object candidate : elements) {
          if (onTheValue) {
            element = true;
            if (matchesOperators(singleValue(candidate), condition))
              return true;
          } else if (candidate instanceof Document || candidate instanceof List) {
            element = false;
            if (matchesTarget(candidate, condition))
              return true;
          }
        }
      }
      return false;
    } finally {
      element = wasElement;
    }
  }

  private static Document elemMatchCondition(final Object operand) {
    if (!(operand instanceof Document condition))
      throw new BadValueException("$elemMatch needs an Object");
    return condition;
  }

  private static boolean isValueForm(final Document condition) {
    return isOperatorDocument(condition) && !QUERY_FORM.contains(condition.keySet().iterator().next());
  }

  private static List<Object> singleValue(final Object value) {
    final List<Object> values = new ArrayList<>(1);
    values.add(value);
    return values;
  }

  /** The {@code [divisor, remainder]} of a {@code $mod}, as 64-bit integers truncated toward zero as {@code mongod} does. */
  private static long[] modArguments(final Object operand) {
    if (!(operand instanceof List<?> arguments))
      throw new BadValueException("malformed mod, needs to be an array");
    if (arguments.size() < 2)
      throw new BadValueException("malformed mod, not enough elements");
    if (arguments.size() > 2)
      throw new BadValueException("malformed mod, too many elements");
    if (!(arguments.get(0) instanceof Number divisorNumber) || !(arguments.get(1) instanceof Number remainderNumber))
      throw new BadValueException("malformed mod, divisor and remainder must be numbers");
    final Long divisor = truncate(divisorNumber);
    final Long remainder = truncate(remainderNumber);
    if (divisor == null)
      throw new BadValueException("malformed mod, divisor value is invalid :: caused by :: " + divisorNumber
          + " is not a finite number that fits in a 64-bit integer");
    if (remainder == null)
      throw new BadValueException("malformed mod, remainder value is invalid :: caused by :: " + remainderNumber
          + " is not a finite number that fits in a 64-bit integer");
    if (divisor == 0)
      throw new BadValueException("divisor cannot be 0");
    return new long[] { divisor, remainder };
  }

  private static boolean modMatches(final Object value, final long[] arguments) {
    if (!(value instanceof Number number))
      return false;
    final Long truncated = truncate(number);
    // Long.MIN_VALUE % -1 is 0 in Java, as it is in mongod
    return truncated != null && truncated % arguments[0] == arguments[1];
  }

  /** The integer part of a number, or null for NaN, the infinities and an integer part that does not fit in 64 bits. */
  private static Long truncate(final Number number) {
    if (isIntegral(number))
      return number.longValue();
    if (number instanceof BigDecimal || number instanceof Decimal128) {
      final BigDecimal decimal = exactValue(number);
      if (decimal == null)
        return null;
      final BigInteger whole = decimal.toBigInteger();
      return whole.bitLength() <= 63 ? whole.longValue() : null;
    }
    final double value = number.doubleValue();
    // the range of a long is [-2^63, 2^63): a cast would saturate anything past it
    return value >= -0x1p63 && value < 0x1p63 ? (long) value : null;
  }

  private static Object types(final Object operand) {
    if (operand instanceof Collection<?> collection && collection.isEmpty())
      throw new FailedToParseException("$type must match at least one type");
    return operand;
  }

  private static Number size(final Object operand) {
    if (!(operand instanceof Number number))
      throw new BadValueException("$size needs a number");
    final double size = number.doubleValue();
    if (size != Math.rint(size) || size < 0)
      throw new BadValueException("Failed to parse $size. Expected a non-negative whole number, got " + operand);
    return number;
  }

  /**
   * Whether one of the values satisfies the predicate. A value that is an array offers its elements (one level), and the array
   * itself when {@code arrayItself}; within a value-form {@code $elemMatch} only the element itself is tested.
   */
  private boolean any(final List<Object> values, final int predicate, final Object operand, final boolean arrayItself) {
    for (int i = 0; i < values.size(); i++) {
      final Object value = values.get(i);
      if (!element && value instanceof List<?> elements && predicate != SIZE && predicate != EXISTS) {
        if (arrayItself && test(predicate, value, operand))
          return true;
        for (final Object item : elements)
          if (test(predicate, item, operand))
            return true;
      } else if (test(predicate, value, operand))
        return true;
    }
    return false;
  }

  private static boolean test(final int predicate, final Object value, final Object operand) {
    return switch (predicate) {
      case EQUALS -> valuesEqual(value, operand);
      case NULLISH -> value == null || value instanceof Missing;
      case REGEX -> regexMatches(value, (BsonRegularExpression) operand);
      case GT, GTE, LT, LTE -> ordered(predicate, value, operand);
      case TYPE -> !(value instanceof Missing) && typeMatches(value, operand);
      case SIZE -> value instanceof List<?> list && list.size() == ((Number) operand).doubleValue();
      case EXISTS -> !(value instanceof Missing);
      case MOD -> modMatches(value, (long[]) operand);
      default -> throw new IllegalStateException("Unknown predicate " + predicate);
    };
  }

  /** A pattern matches a string it is found in, and a stored regular expression with the same pattern and options, as in mongod. */
  private static boolean regexMatches(final Object value, final BsonRegularExpression regex) {
    if (value instanceof String string)
      return regex.matcher(string).find();
    return value instanceof BsonRegularExpression stored && sameRegex(stored, regex);
  }

  /** A range predicate: NaN is neither greater nor less than anything, and is greater or less than or equal to NaN only. */
  private static boolean ordered(final int predicate, final Object value, final Object operand) {
    if (!comparable(value, operand))
      return false;
    if (value instanceof Number a && operand instanceof Number b) {
      final boolean aNaN = isNaN(a);
      final boolean bNaN = isNaN(b);
      if (aNaN || bNaN)
        return aNaN && bNaN && (predicate == GTE || predicate == LTE);
    }
    final int comparison = compareValues(value, operand);
    return switch (predicate) {
      case GT -> comparison > 0;
      case GTE -> comparison >= 0;
      case LT -> comparison < 0;
      default -> comparison <= 0;
    };
  }

  private static boolean typeMatches(final Object value, final Object type) {
    if (type instanceof Collection<?> types) {
      for (final Object item : types)
        if (typeMatches(value, item))
          return true;
      return false;
    }
    if ("number".equals(type))
      return value instanceof Number;
    final BsonType bsonType = bsonTypeOf(type);
    if (bsonType == BsonType.DATE && value instanceof Date)
      return true;
    return bsonType.matches(value);
  }

  /** The type a {@code $type} operand names, null for the {@code number} alias, which stands for every numeric type. */
  private static BsonType bsonTypeOf(final Object type) {
    if ("number".equals(type))
      return null;
    if (type instanceof String alias)
      return BsonType.forString(alias);
    if (type instanceof Number code)
      return BsonType.forNumber(code);
    throw new MongoServerError(14, "TypeMismatch", "type must be represented as a number or a string");
  }

  // ── Paths ─────────────────────────────────────────────────────────────────

  private String[] path(final String key) {
    String[] path = paths.get(key);
    if (path == null) {
      path = key.split("\\.", -1);
      paths.put(key, path);
    }
    return path;
  }

  /** The values a path reaches in a document (or an array element), a missing value where it reaches nothing outside an array. */
  private static List<Object> values(final Object target, final String[] path) {
    final List<Object> values = new ArrayList<>(2);
    expand(target, path, 0, values);
    return values;
  }

  private static void expand(final Object current, final String[] path, final int index, final List<Object> values) {
    if (index == path.length) {
      values.add(current);
      return;
    }
    final String segment = path[index];
    if (current instanceof Document document) {
      if (!document.containsKey(segment)) {
        values.add(MISSING);
        return;
      }
      expand(document.get(segment), path, index + 1, values);
    } else if (current instanceof List<?> elements) {
      // a numeric segment reads the element at that position, and is also the name of a field of each embedded document
      final int position = position(segment);
      if (position >= 0 && position < elements.size())
        expand(elements.get(position), path, index + 1, values);
      for (final Object item : elements)
        if (item instanceof Document)
          expand(item, path, index, values);
    } else
      // a scalar or a null has no field
      values.add(MISSING);
  }

  private static int position(final String segment) {
    final int length = segment.length();
    if (length == 0 || length > 9)
      return -1;
    for (int i = 0; i < length; i++)
      if (segment.charAt(i) < '0' || segment.charAt(i) > '9')
        return -1;
    return Integer.parseInt(segment);
  }

  private static boolean isOperatorDocument(final Document document) {
    if (document.isEmpty())
      return false;
    final String first = document.keySet().iterator().next();
    return first.startsWith("$") && !REFERENCE.contains(first);
  }

  // ── Values ────────────────────────────────────────────────────────────────

  /** Equality as {@code mongod} sees it: numbers by value whatever their type, documents field by field in order. */
  static boolean valuesEqual(final Object a, final Object b) {
    final boolean aNull = a == null || a instanceof Missing;
    final boolean bNull = b == null || b instanceof Missing;
    if (aNull || bNull)
      return aNull && bNull;
    if (a instanceof Number na && b instanceof Number nb)
      return compareNumbers(na, nb) == 0;
    if (a instanceof Document da && b instanceof Document db) {
      if (da.size() != db.size())
        return false;
      final Iterator<Map.Entry<String, Object>> ia = da.entrySet().iterator();
      final Iterator<Map.Entry<String, Object>> ib = db.entrySet().iterator();
      while (ia.hasNext()) {
        final Map.Entry<String, Object> ea = ia.next();
        final Map.Entry<String, Object> eb = ib.next();
        if (!ea.getKey().equals(eb.getKey()) || !valuesEqual(ea.getValue(), eb.getValue()))
          return false;
      }
      return true;
    }
    if (a instanceof List<?> la && b instanceof List<?> lb) {
      if (la.size() != lb.size())
        return false;
      for (int i = 0; i < la.size(); i++)
        if (!valuesEqual(la.get(i), lb.get(i)))
          return false;
      return true;
    }
    if (a instanceof byte[] ba && b instanceof byte[] bb)
      return Arrays.equals(ba, bb);
    if (a instanceof Date || b instanceof Date)
      return instantOf(a).equals(instantOf(b));
    // the class of the BSON library compares by identity: the value read back from storage is never the operand of the filter
    if (a instanceof BsonRegularExpression ra && b instanceof BsonRegularExpression rb)
      return sameRegex(ra, rb);
    return a.equals(b);
  }

  /**
   * Two regular expressions are the same value with the same pattern and the same options in any order, as {@link MongoKeys} keys
   * them; a repeated option counts, as it does in mongod: {@code ii} is not {@code i}.
   */
  private static boolean sameRegex(final BsonRegularExpression a, final BsonRegularExpression b) {
    return a.getPattern().equals(b.getPattern()) && sortedOptions(a.getOptions()).equals(sortedOptions(b.getOptions()));
  }

  private static String sortedOptions(final String options) {
    if (options == null)
      return "";
    for (int i = 1; i < options.length(); i++)
      if (options.charAt(i) < options.charAt(i - 1)) {
        final char[] chars = options.toCharArray();
        Arrays.sort(chars);
        return new String(chars);
      }
    // already in order, the usual case: the BSON specification has the options written sorted
    return options;
  }

  private static Object instantOf(final Object value) {
    return value instanceof Date date ? date.toInstant() : value;
  }

  /** Whether a comparison operator compares the two: values of the same type bracket, or an operand that is MinKey or MaxKey. */
  private static boolean comparable(final Object value, final Object operand) {
    if (operand instanceof MinKey || operand instanceof MaxKey)
      return true;
    if (value == null || value instanceof Missing)
      return false;
    final int bracket = bracket(value);
    return bracket == bracket(operand) && (bracket != OTHER || value.getClass() == operand.getClass());
  }

  /** The order of {@code mongod} over any two values: by type bracket first, then within it; arrays and documents in order. */
  static int compareValues(final Object a, final Object b) {
    final int bracketA = bracket(a);
    final int bracketB = bracket(b);
    if (bracketA != bracketB)
      return Integer.compare(bracketA, bracketB);
    if (a instanceof Number na && b instanceof Number nb)
      return compareNumbers(na, nb);
    if (a instanceof String sa && b instanceof String sb)
      return compareStrings(sa, sb);
    if (a instanceof Document da && b instanceof Document db)
      return compareDocuments(da, db);
    if (a instanceof List<?> la && b instanceof List<?> lb)
      return compareLists(la, lb);
    if (a instanceof Boolean ba && b instanceof Boolean bb)
      return Boolean.compare(ba, bb);
    if (bracketA == 1 || bracketA == MIN_KEY || bracketA == MAX_KEY)
      return 0;
    if (bracketA == 9)
      return ((Instant) instantOf(a)).compareTo((Instant) instantOf(b));
    return ValueComparator.ascWithoutListHandling().compare(a, b);
  }

  /** Field by field in order: the type bracket of the values, then the names, then the values; a prefix sorts first. */
  private static int compareDocuments(final Document a, final Document b) {
    final Iterator<Map.Entry<String, Object>> ia = a.entrySet().iterator();
    final Iterator<Map.Entry<String, Object>> ib = b.entrySet().iterator();
    while (ia.hasNext() && ib.hasNext()) {
      final Map.Entry<String, Object> ea = ia.next();
      final Map.Entry<String, Object> eb = ib.next();
      int comparison = Integer.compare(bracket(ea.getValue()), bracket(eb.getValue()));
      if (comparison == 0)
        comparison = compareStrings(ea.getKey(), eb.getKey());
      if (comparison == 0)
        comparison = compareValues(ea.getValue(), eb.getValue());
      if (comparison != 0)
        return comparison;
    }
    return Integer.compare(a.size(), b.size());
  }

  /** Element by element in order; a prefix sorts first. */
  private static int compareLists(final List<?> a, final List<?> b) {
    final int common = Math.min(a.size(), b.size());
    for (int i = 0; i < common; i++) {
      final int comparison = compareValues(a.get(i), b.get(i));
      if (comparison != 0)
        return comparison;
    }
    return Integer.compare(a.size(), b.size());
  }

  /**
   * Strings in the order of their UTF-8 bytes, which is the order of their code points, as {@code mongod} compares them without a
   * collation. {@link String#compareTo} compares UTF-16 units instead, which differs only between a surrogate (a code point above
   * U+FFFF) and a unit from U+E000 to U+FFFF: the surrogate is the larger code point.
   */
  static int compareStrings(final String a, final String b) {
    final int common = Math.min(a.length(), b.length());
    for (int i = 0; i < common; i++) {
      final char ca = a.charAt(i);
      final char cb = b.charAt(i);
      if (ca != cb) {
        final boolean surrogateA = Character.isSurrogate(ca);
        if (surrogateA != Character.isSurrogate(cb))
          return surrogateA ? 1 : -1;
        return ca - cb;
      }
    }
    return a.length() - b.length();
  }

  /** The type bracket of a value, in the order {@code mongod} sorts them: it compares only within one. */
  private static int bracket(final Object value) {
    if (value instanceof MinKey)
      return MIN_KEY;
    if (value == null || value instanceof Missing)
      return 1;
    if (value instanceof Number)
      return 2;
    if (value instanceof String)
      return 3;
    if (value instanceof Document)
      return 4;
    if (value instanceof List)
      return 5;
    if (value instanceof BinData || value instanceof byte[] || value instanceof LegacyUUID || value instanceof UUID)
      return 6;
    if (value instanceof ObjectId)
      return 7;
    if (value instanceof Boolean)
      return 8;
    if (value instanceof Instant || value instanceof Date)
      return 9;
    if (value instanceof BsonTimestamp)
      return 10;
    if (value instanceof BsonRegularExpression)
      return 11;
    if (value instanceof MaxKey)
      return MAX_KEY;
    return OTHER;
  }

  /** Numbers by value across int, long, double and decimal: NaN equals NaN and sorts below every other number. */
  static int compareNumbers(final Number a, final Number b) {
    if (isIntegral(a) && isIntegral(b))
      return Long.compare(a.longValue(), b.longValue());
    final boolean aNaN = isNaN(a);
    final boolean bNaN = isNaN(b);
    if (aNaN || bNaN)
      return aNaN && bNaN ? 0 : aNaN ? -1 : 1;
    if (a instanceof Decimal128 || b instanceof Decimal128 || a instanceof BigDecimal || b instanceof BigDecimal) {
      final int infinityA = infinitySign(a);
      final int infinityB = infinitySign(b);
      if (infinityA != 0 || infinityB != 0)
        return Integer.compare(infinityA, infinityB);
      // finite on both sides, a Decimal128 past the range of a double included
      return exactValue(a).compareTo(exactValue(b));
    }
    final double da = a.doubleValue();
    final double db = b.doubleValue();
    if (isIntegral(a))
      return compareLongToDouble(a.longValue(), db);
    if (isIntegral(b))
      return -compareLongToDouble(b.longValue(), da);
    // == first, so that -0.0 equals 0.0
    return da == db ? 0 : da < db ? -1 : 1;
  }

  /** A long against a double without the rounding of the long to a double (2^53 + 1 is not 2^53). */
  private static int compareLongToDouble(final long l, final double d) {
    if (d >= 0x1p63)
      return -1;
    if (d < -0x1p63)
      return 1;
    final long whole = (long) d;
    if (l != whole)
      return l < whole ? -1 : 1;
    final double fraction = d - whole;
    return fraction > 0 ? -1 : fraction < 0 ? 1 : 0;
  }

  private static boolean isIntegral(final Number number) {
    return number instanceof Integer || number instanceof Long || number instanceof Short || number instanceof Byte;
  }

  /** NaN, read from the encoding of a Decimal128 rather than its conversion to a double. */
  private static boolean isNaN(final Number number) {
    if (number instanceof Decimal128 decimal)
      return (decimal.getHigh() & DECIMAL_SPECIAL) == DECIMAL_SPECIAL;
    if (number instanceof Double || number instanceof Float)
      return Double.isNaN(number.doubleValue());
    return false;
  }

  /** 1 for positive infinity, -1 for negative infinity, 0 for a finite number; a Decimal128 read from its encoding. */
  private static int infinitySign(final Number number) {
    if (number instanceof Decimal128 decimal) {
      if ((decimal.getHigh() & DECIMAL_SPECIAL) != DECIMAL_INFINITY)
        return 0;
      return decimal.getHigh() < 0 ? -1 : 1;
    }
    if (number instanceof Double || number instanceof Float) {
      final double value = number.doubleValue();
      return Double.isInfinite(value) ? value > 0 ? 1 : -1 : 0;
    }
    return 0;
  }

  /** A finite number as an exact BigDecimal, null for NaN and the infinities. */
  private static BigDecimal exactValue(final Number number) {
    if (number instanceof BigDecimal decimal)
      return decimal;
    if (number instanceof Decimal128 decimal)
      return isNaN(decimal) || infinitySign(decimal) != 0 ? null : decimal.toBigDecimal();
    if (isIntegral(number))
      return BigDecimal.valueOf(number.longValue());
    final double value = number.doubleValue();
    // the exact binary value, not BigDecimal.valueOf's shortest decimal: mongod compares a double and a decimal exactly
    return Double.isFinite(value) ? new BigDecimal(value) : null;
  }
}
