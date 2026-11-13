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

import com.arcadedb.database.RID;
import com.arcadedb.function.graph.IdFunction;
import com.arcadedb.query.opencypher.InlineProperties;
import com.arcadedb.query.opencypher.query.OpenCypherQueryEngine;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.LongRangeList;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * IN expression for WHERE clauses.
 * Example: n.name IN ['Alice', 'Bob', 'Charlie']
 * <p>
 * There is no negated form of this node, because Cypher has no {@code NOT IN} operator - Neo4j has none either,
 * and this parser rejects {@code x NOT IN list} outright. The negation is written {@code NOT x IN list} and parses
 * as a {@code TernaryLogicalExpression(NOT)} around this node, so neither of the two places that build it ever had
 * a second shape to pass. It carried an {@code isNot} flag anyway, fixed at false at both, which made every
 * three-valued answer below read as a choice it was not making.
 */
public class InExpression implements BooleanExpression {
  /** {@code 2^53}: the first magnitude at which a double stops representing every long exactly. */
  private static final double EXACT_DOUBLE_LIMIT = 9007199254740992d;

  /**
   * The size above which a list met again on a later row is answered from a {@link ListLookup}. A short list is walked: the
   * walk costs less than the hash, and the memo is then never written, so a small list built per row ({@code x IN [a, b]})
   * pays nothing for it.
   */
  private static final int LOOKUP_MIN_SIZE = 16;

  /** The contexts a {@link ListMemo} remembers at most, beyond which it forgets them and they build their lookup again. */
  private static final int MAX_MEMO_OWNERS = 64;

  private final Expression expression;
  private final List<Expression> list;

  /**
   * The right side when it is a list literal of literals ({@code x IN ['a', 'b', ...]}): one list for every row, so the
   * lookup built for it is reused (issue #11682). Evaluating the literal builds a new list per row, which no lookup would
   * ever recognize. Null for any other right side.
   */
  private final List<Object> literalList;

  /**
   * The list the last row was asked about, and its lookup once built; see {@link #lookupFor}. One immutable holder in a
   * volatile field, because the node of a cached plan is shared by concurrent executions, and the rows of one execution can
   * be evaluated by several threads, each in its own context (the workers of a parallel scan) or in one (a parallel walk).
   */
  private volatile ListMemo listMemo;

  // Membership is equality against each element, so the element check reuses the = operator's
  // comparator (issue #5293) rather than a second, drifting implementation. Held per AST node so no
  // wrapper is allocated per element per row.
  private final ComparisonExpression equalityComparator = ComparisonExpression.valueComparator(ComparisonExpression.Operator.EQUALS);

  public InExpression(final Expression expression, final List<Expression> list) {
    this.expression = expression;
    this.list = list;
    this.literalList = literalList(list);
  }

  @Override
  public boolean evaluate(final Result result, final CommandContext context) {
    final Object ternary = evaluateTernary(result, context);
    return Boolean.TRUE.equals(ternary);
  }

  @Override
  public Object evaluateTernary(final Result result, final CommandContext context) {
    final Object value;

    if (expression instanceof FunctionCallExpression)
      value = OpenCypherQueryEngine.getExpressionEvaluator().evaluate(expression, result, context);
    else
      value = expression.evaluate(result, context);

    // Iterable of values to compare against. For lists/Object[]/collections this is a zero-copy view;
    // primitive arrays (long[], double[], ...) are auto-boxed by getMultiValueAsList in a single pass.
    final Iterable<?> valuesToCheck;

    if (list.size() == 1) {
      // Single expression on RHS (e.g., x IN listVar, x IN func(), x IN [1,2,3]). Evaluate and unwrap.
      final Expression listItem = list.get(0);
      final Object listValue;
      if (literalList != null)
        listValue = literalList;
      else if (listItem instanceof FunctionCallExpression)
        listValue = OpenCypherQueryEngine.getExpressionEvaluator().evaluate(listItem, result, context);
      else
        listValue = listItem.evaluate(result, context);

      if (listValue == null)
        return null; // x IN null -> null

      // A list that is the same on every row (a parameter, a literal, a value bound before the MATCH) is answered from a
      // hash built once, instead of walked once per row: an index seek on c.key IN $ids re-checks every id it found against
      // the whole list, which made the statement quadratic in the list (issue #11682)
      if (value != null) {
        final ListLookup lookup = lookupFor(listValue, context);
        if (lookup != null) {
          final Boolean known = lookup.contains(value);
          if (known != null)
            return known;
        }
      }
      valuesToCheck = listOperand(listValue);
    } else {
      // Multiple expressions (parsed list literal items)
      final List<Object> evaluated = new ArrayList<>(list.size());
      for (final Expression listItem : list) {
        final Object listValue;
        if (listItem instanceof FunctionCallExpression)
          listValue = OpenCypherQueryEngine.getExpressionEvaluator().evaluate(listItem, result, context);
        else
          listValue = listItem.evaluate(result, context);
        evaluated.add(listValue);
      }
      valuesToCheck = evaluated;
    }

    // Answers the walk below would pay O(n) for. Its cost is the POSITION of the match, and a miss walks all of
    // it, so on the lazy range() of advisory GHSA-xmjm-8q85-g778 an element near the end costs seconds (#6323).
    if (value == null)
      // Every comparison against null is null, whatever the elements are, so only their number matters: a
      // non-empty list makes this uncertain, an empty one leaves nothing to be uncertain about.
      return valuesToCheck.iterator().hasNext() ? null : Boolean.FALSE;

    // A declaration defines the text form of its values ('1' for 1 in a STRING, 5 for '5' in an INTEGER), so membership
    // compares the stored value with what each element would be stored as, like = does (issues #9695, #9794). The
    // declaration is looked up once, and only for a text met by a non-text
    Property declared = null;
    boolean declaredResolved = false;

    if (valuesToCheck instanceof LongRangeList range) {
      Boolean found = rangeMembership(range, value);
      // A range holds longs: a text declared STRING is also a member when it is the canonical spelling of one of them
      if (!Boolean.TRUE.equals(found) && value instanceof String text) {
        declared = declaredProperty(result, context);
        declaredResolved = true;
        if (declared != null && declared.getType() == Type.STRING) {
          final Object number = canonicalLong(text);
          if (number != value)
            found = rangeMembership(range, number);
        }
      }
      if (found != null)
        return found;
    }

    // 3VL: null IN [1,2,3] -> null, 5 IN [1,null,3] -> null (if not found otherwise)
    boolean foundNull = false;
    for (final Object element : valuesToCheck) {
      Boolean cmp = valuesCompare(value, element);
      if (!Boolean.TRUE.equals(cmp) && InlineProperties.isTextAgainstNonText(value, element)) {
        if (!declaredResolved) {
          declared = declaredProperty(result, context);
          declaredResolved = true;
        }
        final Object stored = declared != null ?
            InlineProperties.declaredValue(declared, context != null ? context.getDatabase() : null, element) :
            null;
        if (stored != null)
          cmp = valuesCompare(value, stored);
      }
      if (cmp == null)
        foundNull = true;
      else if (cmp)
        return Boolean.TRUE;
    }

    if (foundNull)
      return null;

    return Boolean.FALSE;
  }

  /**
   * The elements of the right side of an {@code IN} that is a single value (a variable, a parameter, a function call), or the refusal of
   * a value that is not a list. Shared with the index seek that answers an {@code IN} without evaluating this node, so that a right side
   * that is no list is refused by both and not answered with no rows by one of them (issue #10541).
   * <p>
   * For a collection or an array this is a zero-copy view; primitive arrays ({@code long[]}, {@code double[]}, ...) are auto-boxed in a
   * single pass.
   *
   * @param listValue the right side, not {@code null}: {@code x IN null} is null, which is decided before this
   *
   * @throws IllegalArgumentException when the value is not a list
   */
  public static Iterable<?> listOperand(final Object listValue) {
    if (listValue instanceof Collection<?> coll)
      return coll;
    if (MultiValue.isSequenceArray(listValue))
      // Includes primitive arrays (long[], int[], double[], ...) coming from JSON numeric-array query parameters (issue #4284). A
      // byte[] (BINARY) is deliberately excluded: it is one opaque value everywhere in openCypher, the same rule UNWIND already
      // applies (issue #8098).
      return MultiValue.getMultiValueAsList(listValue);
    throw new IllegalArgumentException(
        "InvalidArgumentType: IN requires a list on the right side, got " + listValue.getClass().getSimpleName());
  }

  /**
   * The declared property the left operand reads, when it reads a stored property of a record in any spelling
   * ({@link PropertyReadExpression}, #9701) and the property is declared with a scalar type; otherwise null.
   */
  private Property declaredProperty(final Result result, final CommandContext context) {
    final PropertyReadExpression.PropertyRead read = PropertyReadExpression.of(expression, result, context);
    return read != null && read.record() != null ? InlineProperties.declaredProperty(read.record(), read.key()) : null;
  }

  /**
   * The long a text is the canonical spelling of ({@code "12"}, not {@code "012"}, {@code "+12"} or {@code "-0"}), as the text an
   * integer is stored as; or the text itself, which no long equals, when it is not one. The stored text of an integer is
   * {@code Long.toString} because {@code Type.convert} to {@code String} is {@code toString()} for a number: this inverts it.
   */
  private static Object canonicalLong(final String text) {
    // Only digits after an optional sign are parsed, so an ordinary text never costs a thrown exception
    final int start = text.startsWith("-") ? 1 : 0;
    if (start == text.length() || text.length() - start > 19)
      return text;
    for (int i = start; i < text.length(); i++)
      if (text.charAt(i) < '0' || text.charAt(i) > '9')
        return text;
    try {
      final long parsed = Long.parseLong(text);
      return Long.toString(parsed).equals(text) ? parsed : text;
    } catch (final NumberFormatException e) {
      return text;
    }
  }

  /**
   * Membership in a lazy range, answered from its start, step and size, or null when it cannot be answered that
   * way and the walk has to run. Never returns the 3VL {@code null} answer: a range holds longs and no nulls, and
   * a null left operand is answered before this is called, so uncertainty is impossible here.
   * <p>
   * What the walk asks per element is {@link #valuesCompare}, i.e. the {@code =} operator against a {@code Long}.
   * The branches of {@code ComparisonExpression.compareValuesTernary} that a {@code Long} right operand can reach
   * are exactly the three below - the RID-string interop, the numeric comparison, and "different types are not
   * equal" for everything else (a Long is not {@code Identifiable}, not temporal, and
   * {@code MultiValue.getMultiValueAsList} does not turn it into a list, so those branches cannot fire). A new
   * coercion added there and not learned here would make this diverge, which is what
   * {@code CypherInRangeMembershipTest.answersExactlyAsTheWalkDoes} exists to catch.
   */
  private static Boolean rangeMembership(final LongRangeList range, final Object value) {
    if (range.isEmpty())
      return Boolean.FALSE;

    // Integral types, answered as longs. The = operator's own long-vs-long branch covers only a Long/Integer
    // pair, so a Short or a Byte is compared there through doubleValue() instead - which reaches the same answer:
    // a value of at most 15 bits converts to a double exactly, and so does any element that could equal it, since
    // an element large enough to lose precision is far larger than any Short or Byte.
    if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte)
      return range.containsLong(((Number) value).longValue());

    if (value instanceof Number number) {
      // Any other numeric pair is compared by = through doubleValue(), so this has to be too.
      final double asDouble = number.doubleValue();
      if (Double.isNaN(asDouble) || Double.isInfinite(asDouble))
        // NaN equals nothing, not even itself (issue #5293), and no long is infinite.
        return Boolean.FALSE;
      if (Math.abs(asDouble) >= EXACT_DOUBLE_LIMIT)
        // From 2^53 up a double no longer distinguishes adjacent longs, so which elements it equals stops being a
        // question of one value: leave it to the walk, which is the definition of the answer. The bound is
        // inclusive because 2^53 ITSELF is already ambiguous - 2^53+1 is not representable and rounds ties-to-even
        // down onto 2^53, so both longs convert to this same double, and picking one of them would answer FALSE
        // for a range that holds only the other.
        return null;
      if (asDouble != Math.rint(asDouble))
        return Boolean.FALSE;
      // Below 2^53 every element is converted to a double exactly, so equality of the doubles is equality of the
      // longs, and no element at or above 2^53 can equal a double below it.
      return range.containsLong((long) asDouble);
    }

    // The = operator reads a RID-shaped string against a number as the id it denotes (Neo4j-compatible id()
    // interop): the same coercion, not a second one.
    if (value instanceof String string)
      return RID.is(string) ? range.containsLong(IdFunction.encodeRidAsLong(new RID(string))) : Boolean.FALSE;

    // Anything else is of a different type than a Long, and for = that is simply not equal.
    return Boolean.FALSE;
  }

  /**
   * The lookup answering membership in {@code listValue}, or null when the list is walked. A list is hashed the second time a
   * row asks about it: a list built per row ({@code x IN n.tags}) is never met twice, and so never pays for a hash it would
   * not reuse.
   * <p>
   * A lookup is reused only in the contexts that built it or found it equal to what they built. A context is one execution,
   * or one worker of it, and a caller may hand the same list object, changed in between, to a later execution of this cached
   * plan: that execution builds its own lookup, once, and replaces a stale one. The workers of a parallel scan each build it
   * once too, and then share the memo instead of evicting each other's.
   */
  private ListLookup lookupFor(final Object listValue, final CommandContext context) {
    if (context == null || listValue instanceof LongRangeList || sizeOf(listValue) <= LOOKUP_MIN_SIZE)
      return null;

    final ListMemo memo = listMemo;
    if (memo == null || memo.list() != listValue) {
      listMemo = new ListMemo(listValue, null, new CommandContext[] { context });
      return null;
    }
    if (memo.lookup() != null && memo.isOwnedBy(context))
      return memo.lookup();

    // A benign race: two threads may both build it, and a context an update loses only builds it again
    final ListLookup built = ListLookup.of(listOperand(listValue));
    final boolean unchanged = memo.lookup() == null || built.sameAs(memo.lookup());
    listMemo = new ListMemo(listValue, built, unchanged ? memo.withOwner(context) : new CommandContext[] { context });
    return built;
  }

  /** The number of elements of a right side that is a list, or 0 for any other value, which {@link #listOperand} refuses. */
  private static int sizeOf(final Object listValue) {
    if (listValue instanceof Collection<?> collection)
      return collection.size();
    return MultiValue.isSequenceArray(listValue) ? Array.getLength(listValue) : 0;
  }

  /** The values of a right side written as a list literal of literals only, see {@link #literalList}; null otherwise. */
  private static List<Object> literalList(final List<Expression> list) {
    if (list.size() != 1 || !(list.get(0) instanceof ListExpression literal))
      return null;
    final List<Object> values = new ArrayList<>(literal.getElements().size());
    for (final Expression element : literal.getElements()) {
      if (!(element instanceof LiteralExpression constant))
        return null;
      values.add(constant.getValue());
    }
    return Collections.unmodifiableList(values);
  }

  /**
   * The key a value is hashed under when {@code =} compares it the way {@link Object#equals} compares the key: a boolean as
   * itself, an integral number as a {@code Long}, a {@code Double} holding a long exactly as that {@code Long}, since
   * {@code 1 = 1.0}. Null for anything else, a text included, which is hashed as itself apart from these.
   * <p>
   * Any two numbers keyed here meet in {@code =} either as longs or as doubles that are exact, below {@code 2^53}, so equal
   * keys are exactly the equal numbers. A {@code Float} is not keyed: {@code =} widens it through its decimal form.
   */
  private static Object hashKey(final Object value) {
    if (value instanceof Boolean || value instanceof Long)
      return value;
    if (value instanceof Integer || value instanceof Short || value instanceof Byte)
      return ((Number) value).longValue();
    if (value instanceof Double number) {
      final double asDouble = number;
      if (asDouble == Math.rint(asDouble) && Math.abs(asDouble) < EXACT_DOUBLE_LIMIT)
        return (long) asDouble;
    }
    return null;
  }

  /**
   * A list as met by {@link #lookupFor}, its lookup (null until the list is met a second time), and the contexts that met it
   * with the content the lookup was built from.
   */
  private record ListMemo(Object list, ListLookup lookup, CommandContext[] owners) {
    boolean isOwnedBy(final CommandContext context) {
      for (final CommandContext owner : owners)
        if (owner == context)
          return true;
      return false;
    }

    CommandContext[] withOwner(final CommandContext context) {
      if (isOwnedBy(context))
        return owners;
      if (owners.length >= MAX_MEMO_OWNERS)
        return new CommandContext[] { context };
      final CommandContext[] extended = Arrays.copyOf(owners, owners.length + 1);
      extended[owners.length] = context;
      return extended;
    }
  }

  /**
   * Membership in a list answered from a hash set of its elements (issue #11682), for the lists whose answer the hash can give
   * exactly as the walk does, and for the values it can give it for; {@link #contains} answers null for the rest, which the
   * walk then answers.
   * <p>
   * Only a list of texts, booleans and numbers keyed by {@link #hashKey} is hashed. A list holding a null is not: a miss there
   * is null, not false. Neither is one holding a NaN, a fraction, a list, a map, a temporal or any other value, whose equality
   * is not that of a hash key. A text against a list holding a non-text, and a non-text against a list holding a text, are
   * walked as well: the walk compares a stored value with what the element would be stored as in a declared property
   * (issues #9695, #9794), and a RID spelled as text with the id it denotes.
   */
  private static final class ListLookup {
    /** The lookup of a list that cannot be hashed: it answers nothing, and the list is walked. */
    private static final ListLookup UNHASHABLE = new ListLookup(null, false, false);

    private final Set<Object> members;
    private final boolean     hasText;
    private final boolean     hasNonText;

    private ListLookup(final Set<Object> members, final boolean hasText, final boolean hasNonText) {
      this.members = members;
      this.hasText = hasText;
      this.hasNonText = hasNonText;
    }

    static ListLookup of(final Iterable<?> elements) {
      final Set<Object> members = new HashSet<>();
      boolean hasText = false;
      boolean hasNonText = false;
      for (final Object element : elements) {
        if (element instanceof String) {
          hasText = true;
          members.add(element);
          continue;
        }
        final Object key = hashKey(element);
        if (key == null)
          return UNHASHABLE;
        hasNonText = true;
        members.add(key);
      }
      return new ListLookup(members, hasText, hasNonText);
    }

    /** Whether the two answer every value alike: they hold the same keys, or neither holds any. */
    boolean sameAs(final ListLookup other) {
      if (members == null || other.members == null)
        return members == null && other.members == null;
      return hasText == other.hasText && hasNonText == other.hasNonText && members.equals(other.members);
    }

    /** Whether the value is an element, or null when the walk has to answer it. */
    Boolean contains(final Object value) {
      if (members == null)
        return null;
      if (value instanceof String)
        return hasNonText ? null : members.contains(value);
      if (hasText)
        return null;
      final Object key = hashKey(value);
      return key == null ? null : members.contains(key);
    }
  }

  /**
   * Three-valued comparison of one list element against the left operand.
   * Returns Boolean.TRUE if definitely equal, Boolean.FALSE if definitely not equal,
   * null if uncertain (involves null comparisons where non-null elements match).
   * <p>
   * Membership is defined in terms of equality, so this delegates to the {@code =} operator's
   * comparator instead of re-implementing it (issue #5293). The previous hand-rolled version tried
   * {@code a.equals(b)} before dispatching on type, which imported Java's Double.equals contract
   * (NaN equals itself) and Java's Map.equals contract (null equals null) into Cypher, making
   * {@code NaN IN [NaN]} return true while {@code NaN = NaN} returns false. Delegating also gives IN
   * the temporal coercion, array coercion and map 3VL that {@code =} already implements.
   */
  private Boolean valuesCompare(final Object a, final Object b) {
    final Object cmp = equalityComparator.evaluateWithValues(a, b);
    return cmp == null ? null : (Boolean) cmp;
  }

  @Override
  public String getText() {
    final StringBuilder sb = new StringBuilder();
    sb.append(expression.getText());
    sb.append(" IN [");
    for (int i = 0; i < list.size(); i++) {
      if (i > 0) sb.append(", ");
      sb.append(list.get(i).getText());
    }
    sb.append("]");
    return sb.toString();
  }

  public Expression getExpression() {
    return expression;
  }

  public List<Expression> getList() {
    return list;
  }
}
