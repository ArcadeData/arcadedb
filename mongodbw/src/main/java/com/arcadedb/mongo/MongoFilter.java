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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.RID;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.Identifier;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.TimeBoundRegex;
import de.bwaldvogel.mongo.backend.DefaultQueryMatcher;
import de.bwaldvogel.mongo.backend.QueryMatcher;
import de.bwaldvogel.mongo.bson.BsonRegularExpression;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.bson.ObjectId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * A MongoDB filter and how it is evaluated against the stored documents.
 * <p>
 * The SQL comparisons the translator relies on are not MongoDB's: they coerce across types ({@code n = 5} matches {@code "5"}
 * and {@code true}), see an array as one opaque value (so {@code tags = 'a'} never matches {@code ['a', 'b']}, and
 * {@code tags <> 'a'} matches it) and answer {@code IS DEFINED} on a nested path from the wrong set. A filter that can hit any of
 * those is therefore evaluated on the document, by the matcher of the MongoDB emulation the plugin is built on, which implements
 * MongoDB's own rules: type brackets, array traversal, {@code $elemMatch}, {@code $all}, {@code $size}, dotted paths, ordering.
 * <p>
 * SQL still has a part: a filter that is on the {@code _id}, alone or under {@code $and} / {@code $or} (a batch lookup by key), or has
 * an {@code _id} conjunct, reads its candidates through the unique index on the {@code _id}, and the matcher then tests the whole
 * filter on them. SQL alone would not be exact even there: it coerces across the types of an {@code _id}, so {@code {_id: 1}} would
 * answer a document whose {@code _id} is the string {@code "1"}. Only an empty filter is answered by SQL alone.
 * <p>
 * A field other than the {@code _id} narrows the candidates too, but only where SQL provably cannot answer narrower than the matcher
 * (issue #9162): a field that an index starts with and whose declared type is a scalar, so that no array is stored in it, compared with an operand of that
 * kind by equality, {@code $in} or (for an integer field) a range. SQL then only coerces more values to equal, and a secondary index
 * on the field answers the lookup, which keeps a bulk upsert by a unique key from being O(n^2). A field the schema does not declare
 * is never narrowed, whatever index it has: an array stored in it is invisible to the index and to a SQL comparison, and an index
 * created by the MongoDB {@code createIndexes} command on such a field does not keep arrays out. The declared type is relied upon as a
 * contract of the schema: the engine converts what it writes to the type, but a record stored BEFORE the property was declared is not
 * converted, so an array or a value of another kind left in such a record by a late {@code CREATE PROPERTY} is not found through the
 * narrowed lookup (rebuild or rewrite the records when declaring a property on a type that holds data). The matcher always tests the whole
 * filter on the candidates, so a conjunct that is not narrowed here is still applied.
 * <p>
 * Every other filter reads the whole type.
 * <p>
 * Known leniency of the matcher: a regular expression is also tested against the string form of a number, which MongoDB does not.
 * <p>
 * Regular expressions are searched through {@link TimeBoundRegex}, bounded by {@code arcadedb.command.regexTimeout} with one
 * deadline per filter, exactly like the SQL {@code MATCHES} the plugin used to rely on.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class MongoFilter {
  private static final Pattern ALWAYS = Pattern.compile("(?s).*");
  private static final Pattern NEVER  = Pattern.compile("(?!)");
  private static final Set<String> NARROWING_OPERATORS = Set.of("$eq", "$in");

  private final boolean      empty;
  private final Document     filter;
  private final Document     idPart;
  private final Document     normalized;
  private final QueryMatcher matcher;
  private final Set<String>  properties;

  MongoFilter(final Database database, final Document filter) {
    this(database, filter, null);
  }

  /**
   * @param budget the regex budget of the whole command, shared by the filters of its entries (a bulk update or delete holds one
   *               filter per entry), or {@code null} for one of its own
   */
  MongoFilter(final Database database, final Document filter, final RegexBudget budget) {
    this.empty = filter == null || filter.isEmpty();
    this.filter = filter;
    // the part of the filter on the _id narrows the candidates through the unique index (an _id is never an array, so the SQL cannot
    // miss a match there), and the matcher tests the whole filter on what it returns
    this.idPart = empty ? null :
        onlyId(filter) && narrows(filter) ? filter :
            filter.containsKey("_id") && narrowsValue(filter.get("_id")) ? new Document("_id", filter.get("_id")) : null;
    if (empty) {
      this.normalized = null;
      this.matcher = null;
      this.properties = Set.of();
    } else {
      this.normalized = normalizeQuery(filter, budget != null ? budget : RegexBudget.of(database), true);
      this.matcher = new DefaultQueryMatcher();
      this.properties = new HashSet<>();
      collectProperties(normalized, properties);
    }
  }

  /**
   * The properties of a record a filter reads: its top-level field names (the first segment of a dotted path), through the logical
   * operators, which hold queries of their own. What an {@code $elemMatch} or an operator reads lies inside those properties.
   */
  private static void collectProperties(final Document query, final Set<String> properties) {
    for (final Map.Entry<String, Object> entry : query.entrySet()) {
      final String key = entry.getKey();
      if ("$and".equals(key) || "$or".equals(key) || "$nor".equals(key)) {
        if (entry.getValue() instanceof List<?> list)
          for (final Object item : list)
            if (item instanceof Document document)
              collectProperties(document, properties);
      } else if (!key.startsWith("$")) {
        final int dot = key.indexOf('.');
        properties.add(dot < 0 ? key : key.substring(0, dot));
      }
    }
  }

  /**
   * @return true when the filter selects everything
   */
  boolean isEmpty() {
    return empty;
  }

  /**
   * Whether a stored record matches the filter. Only meaningful for a filter that is not empty, because an empty one
   * has no clause left to test.
   */
  boolean matches(final Map<String, Object> storedProperties) {
    if (empty)
      throw new IllegalStateException("An empty filter has no clause to test a record against");
    return matcher.matches(MongoDBToSqlTranslator.toMatchDocument(storedProperties, properties), normalized);
  }

  // com.arcadedb.database.Document is spelled out in full: its simple name is the one of the MongoDB Document imported here
  boolean matches(final com.arcadedb.database.Document record) {
    return matches(record.toMap(false));
  }

  boolean matchesRow(final Result result) {
    return matches(result.toMap());
  }

  /**
   * @return true when the filter has a part on the {@code _id} that SQL can only answer wider than the matcher, so the candidates are
   * read through the index of the {@code _id} instead of the whole type (the matcher still tests the whole filter on them)
   */
  boolean narrowsById() {
    return idPart != null;
  }

  /**
   * Appends the {@code WHERE} clause that selects the candidates of the filter: its part on the {@code _id} for a filter that
   * {@link #narrowsById() narrows by _id}, joined with the conjuncts on declared scalar fields that
   * {@link #narrowingPart(Document, DocumentType) narrow} (issue #9162), nothing for a filter with neither (every record of the type is
   * a candidate).
   *
   * @param type the type of the collection, or {@code null} when it is not known: only the {@code _id} then narrows
   */
  void appendCandidateWhere(final StringBuilder sqlText, final Map<String, Object> params, final DocumentType type) {
    // a type with subtypes also reads them, and a subtype may redeclare a property with another type: not narrowed
    final Document fields = type != null && !empty && type.getSubTypes().isEmpty() ? narrowingPart(filter, type) : null;
    if (idPart == null && fields == null)
      return;

    sqlText.append(" WHERE ");
    if (idPart != null)
      MongoDBToSqlTranslator.buildExpression(sqlText, params, idPart);
    if (fields != null) {
      if (idPart != null)
        sqlText.append(" AND ");
      MongoDBToSqlTranslator.buildExpression(sqlText, params, fields);
    }
  }

  /**
   * The part of a query that SQL can use to narrow the candidates without ever dropping a document the matcher accepts, or
   * {@code null} when there is none. Each conjunct that qualifies is kept as it is, the others are left out: dropping a conjunct only
   * widens the candidates, the matcher tests the whole filter on them.
   * <ul>
   *   <li>a field is narrowed only when its declared type is one of {@link #narrowingType}, so it cannot hold an array, with a
   *   plain top-level name (a dotted path traverses arrays and embedded documents);</li>
   *   <li>an operator narrows only with an operand of the kind of the field ({@link #compatible}): MongoDB brackets the types, SQL
   *   coerces them, and a null operand also matches a missing field;</li>
   *   <li>{@code $and} narrows with the conjuncts of its items, {@code $or} only when every branch narrows, and {@code $nor} never.</li>
   * </ul>
   * The {@code _id} is not here: it has its own path ({@link #narrowsById()}).
   */
  private static Document narrowingPart(final Document query, final DocumentType type) {
    final List<Object> conjuncts = new ArrayList<>();
    for (final Map.Entry<String, Object> entry : query.entrySet()) {
      final String key = entry.getKey();
      final Object operand = entry.getValue();

      if ("$and".equals(key)) {
        if (operand instanceof List<?> items)
          for (final Object item : items)
            if (item instanceof Document document) {
              final Document part = narrowingPart(document, type);
              if (part != null)
                conjuncts.add(part);
            }
      } else if ("$or".equals(key)) {
        if (operand instanceof List<?> branches && !branches.isEmpty()) {
          final List<Object> narrowed = new ArrayList<>(branches.size());
          for (final Object branch : branches) {
            final Document part = branch instanceof Document document ? narrowingPart(document, type) : null;
            // a branch that does not narrow can hold any record: so can the $or
            if (part == null)
              break;
            narrowed.add(part);
          }
          if (narrowed.size() == branches.size())
            conjuncts.add(new Document("$or", narrowed));
        }
      } else if (!key.startsWith("$") && !"_id".equals(key) && key.indexOf('.') < 0) {
        final Property property = type.getPolymorphicPropertyIfExists(key);
        if (property != null && narrowingType(property.getType()) && leadsAnIndex(type, key)) {
          final Document operators = narrowingOperators(property.getType(), operand);
          if (operators != null)
            conjuncts.add(new Document(key, operators));
        }
      }
    }
    if (conjuncts.isEmpty())
      return null;
    return conjuncts.size() == 1 && conjuncts.getFirst() instanceof Document single ? single : new Document("$and", conjuncts);
  }

  /**
   * Whether an index of the type starts with the field: only then does SQL narrow the candidates, a comparison without an index is a
   * scan of the type like the matcher's own, so it would gain nothing and only expose a record that does not hold the declared type
   * (stored before the property was declared) to being missed.
   */
  private static boolean leadsAnIndex(final DocumentType type, final String field) {
    for (final TypeIndex index : TypeIndex.filterReadyForQueries(type.getAllIndexes(true)))
      if (field.equals(index.getPropertyNames().getFirst()))
        return true;
    return false;
  }

  /**
   * The types a declared property can have for SQL to narrow the candidates: the scalars that are compared the same way by SQL and by
   * the matcher. Not the floating point {@code FLOAT} (a stored single precision value is not the double the operand is), nor
   * {@code DECIMAL} (scale), nor the temporal types (zones and precision), nor the containers.
   */
  private static boolean narrowingType(final Type type) {
    return switch (type) {
      case STRING, BOOLEAN, BYTE, SHORT, INTEGER, LONG, DOUBLE -> true;
      default -> false;
    };
  }

  /**
   * The operators of a field that narrow, or {@code null} when none does. A plain operand is an equality.
   */
  private static Document narrowingOperators(final Type type, final Object operand) {
    if (operand instanceof Document document && isOperatorDocument(document)) {
      final Document result = new Document();
      for (final Map.Entry<String, Object> entry : document.entrySet())
        if (narrows(type, entry.getKey(), entry.getValue()))
          result.put(entry.getKey(), entry.getValue());
      return result.isEmpty() ? null : result;
    }
    return narrows(type, "$eq", operand) ? new Document("$eq", operand) : null;
  }

  private static boolean narrows(final Type type, final String operator, final Object operand) {
    return switch (operator) {
      case "$eq" -> compatible(type, operand);
      case "$in" -> {
        if (!(operand instanceof List<?> list) || list.isEmpty())
          yield false;
        for (final Object item : list)
          if (!compatible(type, item))
            yield false;
        yield true;
      }
      // a range only on an integer field: a string follows the collation of the index (not the code point order of MongoDB), and a
      // floating point one has NaN and the rounding of what is stored
      case "$gt", "$gte", "$lt", "$lte" -> isIntegral(type) && compatible(type, operand);
      default -> false;
    };
  }

  private static boolean isIntegral(final Type type) {
    return type == Type.BYTE || type == Type.SHORT || type == Type.INTEGER || type == Type.LONG;
  }

  /**
   * Whether an operand is of the kind of a field, so that SQL and the matcher compare the same two values: a string with a string, a
   * boolean with a boolean, an integral number that the integer type can hold with an integer field, a number a double holds exactly
   * with a double field.
   */
  private static boolean compatible(final Type type, final Object operand) {
    return switch (type) {
      case STRING -> operand instanceof String;
      case BOOLEAN -> operand instanceof Boolean;
      case BYTE -> integral(operand) && fits(operand, Byte.MIN_VALUE, Byte.MAX_VALUE);
      case SHORT -> integral(operand) && fits(operand, Short.MIN_VALUE, Short.MAX_VALUE);
      case INTEGER -> integral(operand) && fits(operand, Integer.MIN_VALUE, Integer.MAX_VALUE);
      case LONG -> integral(operand);
      // 2^53: every integer up to it is a double, past it a long is rounded and the two comparisons may part
      case DOUBLE -> operand instanceof Double d ? Double.isFinite(d) : integral(operand) && fits(operand, -(1L << 53), 1L << 53);
      default -> false;
    };
  }

  private static boolean integral(final Object operand) {
    return operand instanceof Integer || operand instanceof Long || operand instanceof Short || operand instanceof Byte;
  }

  private static boolean fits(final Object integral, final long min, final long max) {
    final long value = ((Number) integral).longValue();
    return value >= min && value <= max;
  }

  /**
   * Visits the identity of every record that matches the (non-empty) filter, until the visitor answers {@code false}.
   */
  void scanMatches(final Database database, final String collectionName, final Predicate<RID> visitor) {
    // always a SQL query, narrowed by the _id when the filter has a part on it: it is how the query is counted by the metrics of the
    // protocol and bounded by the command timeout, like every other query
    final Map<String, Object> params = new HashMap<>();
    final StringBuilder text = new StringBuilder("SELECT FROM ").append(Identifier.quote(collectionName));
    appendCandidateWhere(text, params, database.getSchema().getTypeOrNull(collectionName));
    try (final ResultSet rs = database.query("sql", text.toString(), params)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        if (matchesRow(row) && row.getIdentity().isPresent() && !visitor.test(row.getIdentity().get()))
          return;
      }
    }
  }

  /**
   * The identities of the records matching the filter, up to {@code limit} of them (0 for no limit). The selection is a snapshot:
   * the caller applies its change to these records without evaluating the filter again, and skips the ones deleted meanwhile. Both
   * steps run in the caller's transaction, which is what keeps a concurrent change from being applied half way.
   */
  List<RID> select(final Database database, final String collectionName, final int limit) {
    final List<RID> rids = new ArrayList<>();
    if (empty) {
      final Map<String, Object> params = new HashMap<>();
      final StringBuilder text = new StringBuilder("SELECT @rid FROM ").append(Identifier.quote(collectionName));
      if (limit > 0)
        text.append(" LIMIT ").append(limit);
      try (final ResultSet rs = database.query("sql", text.toString(), params)) {
        while (rs.hasNext())
          rs.next().getIdentity().ifPresent(rids::add);
      }
    } else
      scanMatches(database, collectionName, rid -> {
        rids.add(rid);
        return limit <= 0 || rids.size() < limit;
      });
    return rids;
  }

  /**
   * Whether the SQL answer for the {@code _id} conditions of a filter can only be wider than the matcher's, never narrower: the
   * matcher then verifies every candidate and nothing it would accept is lost. That holds for an equality and {@code $in}, where SQL
   * only coerces more values to equal. It does not for a negation ({@code $ne}, {@code $nin}, {@code $not}), which can drop a match
   * of a mixed-type {@code _id}, nor for a range: the index of a collection that mixes kinds of {@code _id} orders them as text, so
   * {@code {$gt: 5}} would miss {@code 10}.
   */
  private static boolean narrows(final Document filter) {
    for (final Map.Entry<String, Object> entry : filter.entrySet()) {
      final String key = entry.getKey();
      if ("_id".equals(key)) {
        if (!narrowsValue(entry.getValue()))
          return false;
      } else if (entry.getValue() instanceof List<?> list) {
        for (final Object item : list)
          if (!(item instanceof Document document) || !narrows(document))
            return false;
      }
    }
    return true;
  }

  /**
   * A regular expression operand (alone or in a list) is a pattern only the matcher evaluates, and an embedded document or a list
   * is compared by SQL in ways (key order) not proven to match MongoDB's: SQL compares them as a value, so none of them narrows.
   */
  private static boolean isNonScalarOperand(final Object operand) {
    if (operand instanceof BsonRegularExpression || operand instanceof Document)
      return true;
    if (operand instanceof List<?> list)
      for (final Object item : list)
        if (item instanceof BsonRegularExpression || item instanceof Document || item instanceof List)
          return true;
    return false;
  }

  private static boolean narrowsValue(final Object operand) {
    if (operand instanceof Document operators) {
      if (!isOperatorDocument(operators))
        // a document _id, compared as a whole
        return false;
      for (final Map.Entry<String, Object> entry : operators.entrySet())
        if (!NARROWING_OPERATORS.contains(entry.getKey()) || isNonScalarOperand(entry.getValue()))
          return false;
      return true;
    }
    // a plain value is an equality, but not a regular expression nor a list, which only the matcher evaluates like MongoDB
    return !(operand instanceof BsonRegularExpression) && !(operand instanceof List);
  }

  /**
   * Whether every condition of the filter is on the {@code _id}, alone or under {@code $and} / {@code $or} (the shapes a driver
   * emits for a batch lookup by key).
   */
  private static boolean onlyId(final Document filter) {
    for (final Map.Entry<String, Object> entry : filter.entrySet())
      if (!"_id".equals(entry.getKey()) && !isLogicalOverId(entry.getKey(), entry.getValue()))
        return false;
    return true;
  }

  private static boolean isLogicalOverId(final String key, final Object operand) {
    if (!("$and".equals(key) || "$or".equals(key)) || !(operand instanceof List<?> list) || list.isEmpty())
      return false;
    for (final Object item : list)
      if (!(item instanceof Document document) || document.isEmpty() || !onlyId(document))
        return false;
    return true;
  }

  /**
   * Prepares a query document for the matcher: a regular expression, however it is spelled, becomes a {@link BoundedRegex}, and
   * the {@code _id} operand takes the form the {@code _id} is stored in.
   */
  private static Document normalizeQuery(final Document query, final RegexBudget deadline, final boolean top) {
    final Document result = new Document();
    List<Object> lifted = null;
    for (final Map.Entry<String, Object> entry : query.entrySet()) {
      final String key = entry.getKey();
      final Object value = entry.getValue();

      if ("$and".equals(key) || "$or".equals(key) || "$nor".equals(key)) {
        if (!(value instanceof List<?> list))
          throw new IllegalArgumentException("Operator " + key + " requires an array");
        final List<Object> converted = new ArrayList<>(list.size());
        for (final Object item : list)
          converted.add(logicalOperand(key, item, deadline, top));
        result.put(key, converted);
        continue;
      }
      if ("$expr".equals(key)) {
        // in a find filter the expression is evaluated through SQL, where it can carry a regular expression out of reach of the time
        // bound. An aggregation $match evaluates it with the library, whose expressions have no regular expression operator
        if (!deadline.allowsExpr)
          throw new IllegalArgumentException("The operator $expr is not supported");
        result.put(key, value);
        continue;
      }
      if (key.startsWith("$")) {
        result.put(key, value);
        continue;
      }

      // the _id operand takes its stored form first, then goes the way of any other field (its regular expressions are bounded too)
      final Object operand = top && "_id".equals(key) ? MongoBsonValues.idFilter(value) : value;

      if (operand instanceof BsonRegularExpression regex)
        result.put(key, new BoundedRegex(regex.getPattern(), regex.getOptions(), deadline));
      else if (operand instanceof ObjectId objectId)
        result.put(key, new Document("$in", bothForms(objectId)));
      else if (operand instanceof Document document && isOperatorDocument(document)) {
        if (document.containsKey("$regex")) {
          final Document rest = withoutRegex(document);
          final BoundedRegex regex = BoundedRegex.of(document, deadline);
          if (rest.isEmpty())
            result.put(key, regex);
          else {
            // {$regex, $ne, ...}: the regular expression becomes a condition of its own, next to the others
            result.put(key, normalizeOperators(rest, deadline));
            if (lifted == null)
              lifted = new ArrayList<>();
            lifted.add(new Document(key, regex));
          }
        } else
          result.put(key, normalizeOperators(document, deadline));
      } else
        result.put(key, operand);
    }

    if (lifted == null)
      return result;
    final List<Object> all = new ArrayList<>(lifted.size() + 1);
    all.add(result);
    all.addAll(lifted);
    return new Document("$and", all);
  }

  private static Document logicalOperand(final String operator, final Object item, final RegexBudget deadline, final boolean top) {
    if (!(item instanceof Document document))
      throw new IllegalArgumentException("Operator " + operator + " requires an array of documents");
    return normalizeQuery(document, deadline, top);
  }

  /**
   * An ObjectId stored outside the {@code _id} is tagged, but data written before the tagging holds the bare hex string: both forms
   * are the same value. Only for the operands that compare one value ({@code $eq}, {@code $ne}, {@code $in}, {@code $nin}, a plain
   * equality): an array operand ({@code {refs: [oid]}}) and {@code $all} match the tagged form only.
   */
  private static List<Object> bothForms(final ObjectId objectId) {
    return List.of(objectId, objectId.getHexData());
  }

  /**
   * The operators applied to one field: only the places a regular expression can sit need a look.
   */
  private static Document normalizeOperators(final Document operators, final RegexBudget deadline) {
    final Document result = new Document();
    for (final Map.Entry<String, Object> entry : operators.entrySet()) {
      final String operator = entry.getKey();
      final Object operand = entry.getValue();
      switch (operator) {
      case "$not" -> {
        if (operand instanceof BsonRegularExpression regex)
          result.put(operator, new BoundedRegex(regex.getPattern(), regex.getOptions(), deadline));
        else if (operand instanceof Document document && isOperatorDocument(document))
          result.put(operator, regexOnly(document, deadline));
        else
          result.put(operator, operand);
      }
      case "$eq", "$ne" -> {
        if (operand instanceof ObjectId objectId)
          result.put("$eq".equals(operator) ? "$in" : "$nin", bothForms(objectId));
        else
          result.put(operator, operand);
      }
      case "$in", "$nin", "$all" -> {
        if (operand instanceof List<?> list) {
          final List<Object> converted = new ArrayList<>(list.size());
          for (final Object item : list)
            if (item instanceof BsonRegularExpression regex)
              converted.add(new BoundedRegex(regex.getPattern(), regex.getOptions(), deadline));
            else if (item instanceof Document document && isOperatorDocument(document))
              // {$all: [{$elemMatch: ...}]}
              converted.add(normalizeOperators(document, deadline));
            else {
              converted.add(item);
              // not for $all, which asks for each element: the hex form of an ObjectId is not an extra one
              if (item instanceof ObjectId objectId && !"$all".equals(operator))
                converted.add(objectId.getHexData());
            }
          result.put(operator, converted);
        } else
          result.put(operator, operand);
      }
      case "$and", "$or", "$nor" -> {
        // inside an $elemMatch operator document, for one: the regular expressions of the sub-queries are bounded too
        if (operand instanceof List<?> list) {
          final List<Object> converted = new ArrayList<>(list.size());
          for (final Object item : list)
            converted.add(logicalOperand(operator, item, deadline, false));
          result.put(operator, converted);
        } else
          result.put(operator, operand);
      }
      case "$elemMatch" -> {
        if (operand instanceof Document document) {
          if (isOperatorDocument(document)) {
            // the matcher builds the expression of an operator document itself, out of reach of the time bound
            if (document.containsKey("$regex"))
              throw new IllegalArgumentException("$regex directly inside $elemMatch is not supported, match a field of the element");
            result.put(operator, normalizeOperators(document, deadline));
          } else
            result.put(operator, normalizeQuery(document, deadline, false));
        } else
          result.put(operator, operand);
      }
      default -> result.put(operator, operand);
      }
    }
    return result;
  }

  /**
   * An operator document that may be nothing but a regular expression, which is what the places that cannot take a second
   * condition (an operand of {@code $not} or {@code $elemMatch}) accept; any other operators are normalized as usual.
   */
  private static Object regexOnly(final Document document, final RegexBudget deadline) {
    if (!document.containsKey("$regex"))
      return normalizeOperators(document, deadline);
    if (!withoutRegex(document).isEmpty())
      throw new IllegalArgumentException("$regex cannot be combined with other operators here");
    return BoundedRegex.of(document, deadline);
  }

  private static Document withoutRegex(final Document document) {
    final Document rest = new Document();
    for (final Map.Entry<String, Object> entry : document.entrySet())
      if (!"$regex".equals(entry.getKey()) && !"$options".equals(entry.getKey()))
        rest.put(entry.getKey(), entry.getValue());
    return rest;
  }

  /**
   * Bounds the regular expressions of an aggregation pipeline: the mongo-java-server aggregation filters a {@code $match} stage with
   * its own matcher, whose expressions run with no deadline. Each {@code $match} query (also the ones of {@code $facet} branches,
   * {@code $lookup} and {@code $unionWith} sub-pipelines, and {@code $graphLookup.restrictSearchWithMatch}) is prepared as a find
   * filter is, so every regular expression of the pipeline draws on one budget. The library evaluates no other regular expression: it has no
   * {@code $expr}, {@code $regexMatch} or {@code $regexFind}, and {@code $merge}/{@code $out} cannot resolve a collection here. The stages are copied, the pipeline of the request is
   * left as it is.
   */
  static List<Document> boundPipeline(final List<Document> pipeline, final RegexBudget budget) {
    final List<Document> bounded = new ArrayList<>(pipeline.size());
    for (final Document stage : pipeline)
      bounded.add(boundStage(stage, budget));
    return bounded;
  }

  private static Document boundStage(final Document stage, final RegexBudget budget) {
    final Document result = new Document();
    for (final Map.Entry<String, Object> entry : stage.entrySet()) {
      final String name = entry.getKey();
      final Object value = entry.getValue();
      switch (name) {
      // inside a pipeline the _id is the one the previous stage produced, not the stored identity of a record: never the "top" form
      case "$match" -> result.put(name, value instanceof Document query ? normalizeQuery(query, budget, false) : value);
      case "$facet" -> {
        if (value instanceof Document branches) {
          final Document converted = new Document();
          for (final Map.Entry<String, Object> branch : branches.entrySet())
            converted.put(branch.getKey(), boundStages(branch.getValue(), budget));
          result.put(name, converted);
        } else
          result.put(name, value);
      }
      case "$lookup", "$unionWith" -> {
        if (value instanceof Document options && options.containsKey("pipeline")) {
          final Document converted = new Document(options);
          converted.put("pipeline", boundStages(options.get("pipeline"), budget));
          result.put(name, converted);
        } else
          result.put(name, value);
      }
      case "$graphLookup" -> {
        if (value instanceof Document options && options.get("restrictSearchWithMatch") instanceof Document query) {
          final Document converted = new Document(options);
          converted.put("restrictSearchWithMatch", normalizeQuery(query, budget, false));
          result.put(name, converted);
        } else
          result.put(name, value);
      }
      default -> result.put(name, value);
      }
    }
    return result;
  }

  private static Object boundStages(final Object stages, final RegexBudget budget) {
    if (!(stages instanceof List<?> list))
      return stages;
    final List<Object> converted = new ArrayList<>(list.size());
    for (final Object stage : list)
      converted.add(stage instanceof Document document ? boundStage(document, budget) : stage);
    return converted;
  }

  private static boolean isOperatorDocument(final Document document) {
    return !document.isEmpty() && document.keySet().iterator().next().startsWith("$");
  }

  /**
   * The time a filter may spend in regular expressions, shared by every search of the filter: {@code arcadedb.command.regexTimeout}
   * counts the time inside the expressions only, not the reading of the documents around them, so a long scan with a harmless
   * expression is not cut short while a pathological one still is. The clock is read every few hundred steps of a search, so a very
   * short value never trips it. Not thread-safe: a filter is built per command and evaluated by one thread.
   */
  static final class RegexBudget {
    private final long    timeoutNanos;
    private final boolean allowsExpr;
    private       long    remainingNanos;
    private       boolean exhausted;

    static RegexBudget ofMillis(final long timeoutMillis) {
      return new RegexBudget(timeoutMillis, false);
    }

    static RegexBudget of(final Database database) {
      return new RegexBudget(GlobalConfiguration.COMMAND_REGEX_TIMEOUT.getValueAsLong(database), false);
    }

    /** The budget of an aggregation pipeline, whose {@code $match} may carry {@code $expr}. */
    static RegexBudget ofAggregation(final Database database) {
      return new RegexBudget(GlobalConfiguration.COMMAND_REGEX_TIMEOUT.getValueAsLong(database), true);
    }

    private RegexBudget(final long timeoutMillis, final boolean allowsExpr) {
      this.allowsExpr = allowsExpr;
      // an oversized timeout saturates (it never expires) instead of wrapping around into a deadline in the past
      long nanos = 0;
      if (timeoutMillis > 0)
        try {
          nanos = Math.multiplyExact(timeoutMillis, 1_000_000L);
        } catch (final ArithmeticException e) {
          nanos = Long.MAX_VALUE;
        }
      // a budget of centuries is no budget: it saturates, so that start + remaining can never wrap around
      if (nanos > (Long.MAX_VALUE >> 2))
        nanos = Long.MAX_VALUE;
      this.timeoutNanos = nanos;
      this.remainingNanos = nanos;
    }

    boolean find(final Pattern pattern, final String input) {
      if (timeoutNanos <= 0 || timeoutNanos == Long.MAX_VALUE)
        return TimeBoundRegex.findUntil(pattern, input, Long.MAX_VALUE);
      if (exhausted)
        throw new TimeoutException(
            "Regular expression time of the command exhausted: arcadedb.command.regexTimeout is shared by every document the command scans");
      final long start = System.nanoTime();
      try {
        return TimeBoundRegex.findUntil(pattern, input, start + remainingNanos);
      } finally {
        remainingNanos -= System.nanoTime() - start;
        exhausted = remainingNanos <= 0;
      }
    }
  }

  /**
   * A regular expression whose search is bounded by a deadline shared by the whole filter. The matcher asks the expression for a
   * {@link Matcher} over the value; the answer is computed here, through {@link TimeBoundRegex}, and handed back as a matcher
   * that finds (or does not) whatever the value is.
   */
  private static final class BoundedRegex extends BsonRegularExpression {
    private final Pattern pattern;
    private final RegexBudget deadline;

    BoundedRegex(final String regex, final String options, final RegexBudget deadline) {
      super(regex, options);
      this.deadline = deadline;
      this.pattern = compile(regex, options);
    }

    static BoundedRegex of(final Document operators, final RegexBudget deadline) {
      final Object regex = operators.get("$regex");
      final Object options = operators.get("$options");
      // {$regex: /abc/i}: the literal brings its own flags, an explicit $options wins
      if (regex instanceof BsonRegularExpression literal)
        return new BoundedRegex(literal.getPattern(), options != null ? options.toString() : literal.getOptions(), deadline);
      return new BoundedRegex(String.valueOf(regex), options != null ? options.toString() : null, deadline);
    }

    private static Pattern compile(final String regex, final String options) {
      int flags = Pattern.UNICODE_CASE;
      if (options != null)
        for (int i = 0; i < options.length(); i++) {
          final char flag = options.charAt(i);
          switch (flag) {
          case 'i' -> flags |= Pattern.CASE_INSENSITIVE;
          case 'm' -> flags |= Pattern.MULTILINE;
          case 's' -> flags |= Pattern.DOTALL;
          case 'x' -> flags |= Pattern.COMMENTS;
          case 'u' -> {
            // always on
          }
          default -> throw new IllegalArgumentException("Unknown regular expression option '" + flag + "'");
          }
        }
      try {
        return Pattern.compile(regex, flags);
      } catch (final PatternSyntaxException e) {
        throw new IllegalArgumentException("Invalid regular expression '" + regex + "': " + e.getDescription(), e);
      }
    }

    /**
     * Assumes the matcher only calls {@link Matcher#find()} on what it gets back (true of the mongo-java-server this plugin is
     * built on): the answer is computed here, so only "found or not" is carried. Tests over every regex-bearing operator pin it
     * when the library is upgraded.
     */
    @Override
    public Matcher matcher(final String string) {
      return (deadline.find(pattern, string) ? ALWAYS : NEVER).matcher(string);
    }
  }
}
