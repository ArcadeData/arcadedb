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
package com.arcadedb.query.opencypher.executor;

import com.arcadedb.database.Document;
import com.arcadedb.exception.InvalidPropertyTypeException;
import com.arcadedb.query.opencypher.ast.Expression;
import com.arcadedb.query.opencypher.temporal.TemporalUtil;

import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/**
 * Comparisons and validation shared by every openCypher write clause (CREATE, MERGE, SET) between a value stored on
 * a record and a value a query supplies.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class CypherValues {
  /** How many of a refused map's keys a message names before it stops; see {@link #describeKeys}. */
  private static final int MAX_DESCRIBED_KEYS = 5;

  /** How long an expression's text may be before a message stops naming it; see {@link #namesAValue}. */
  private static final int MAX_ECHOED_EXPRESSION_LENGTH = 100;

  /** How much of one key a message shows; see {@link #describeKeys}. */
  private static final int MAX_DESCRIBED_KEY_LENGTH = 40;

  private CypherValues() {
  }

  /**
   * Numeric-tolerant equality: a stored {@code Integer} 1 equals a supplied {@code Long} 1, which is what makes a
   * MERGE pattern match a record written with a different integral width. A {@code Float} and a {@code Double} may
   * still report unequal after widening (0.1f != 0.1d); that is conservative on purpose, since every caller pays for
   * a false negative with a redundant write or an extra probe, never with a wrong answer.
   */
  public static boolean equalValues(final Object a, final Object b) {
    if (a == null)
      return b == null;
    if (a.equals(b))
      return true;
    if (a instanceof Number numberA && b instanceof Number numberB)
      return numberA.longValue() == numberB.longValue()
          && Double.compare(numberA.doubleValue(), numberB.doubleValue()) == 0;
    return false;
  }

  /**
   * Coerces a property value to its stored Java type and rejects the ones a property cannot hold, matching Neo4j's
   * "Property values can only be of primitive types or arrays thereof". Every openCypher write clause (CREATE, MERGE,
   * SET) funnels its property values through here so a map - or a list containing one - is refused the same way
   * regardless of which clause, or which right-hand-side shape (dot property, {@code +=}/{@code =} map, bare
   * parameter), produced it (issue #7629).
   * <p>
   * A Point value is exempt: Neo4j treats Point as a primitive property type, ArcadeDB just has no dedicated
   * Geometry runtime type yet (issue #4870) and represents one as a map of coordinate keys under the hood, so
   * refusing every map would also reject {@code point()}'s own output. {@link #isPointShaped} recognises one
   * structurally - by the numeric {@code x}/{@code y} and a {@code crs} key every branch of
   * {@code CypherPointFunction} writes, the same keys {@code CypherPointDistanceFunction} and
   * {@code PointWithinBBoxFunction} already key off to read one back - rather than by class identity, because a
   * Point read back off storage is deserialized as a plain {@link Map} (issue #7629): identity would exempt a
   * point() call's immediate result but not a value copied from an already-stored Point property (e.g.
   * {@code MATCH (a) CREATE (b {loc: a.loc})}). The exemption only waives the "is a Map" check on the point-shaped
   * map itself - every one of its own entries is still validated, so a map smuggled in under some other key (e.g.
   * {@code {x: 1, y: 2, crs: 'x', payload: {secret: 1}}}) is still refused.
   * <p>
   * The two context arguments are what a refusal needs to be able to name (issue #7729). The message this used to
   * raise was written for the literal case - {@code SET n.x = {y: 1}} - where the offending value is right there in
   * the query text. It is just as reachable through a copy: {@code SET t = n} or {@code SET t.m2 = n.m} against a
   * record whose map property was written by SQL, which is allowed to store one because {@code MAP} is a
   * first-class ArcadeDB schema type. On those paths the caller wrote no map at all, and "Property values can not
   * be maps" named neither the property being written nor where the value came from. Both are passed raw rather
   * than pre-rendered, and are read only on the failure path: a property write is a hot path and must not pay for a
   * message no one will ever see.
   *
   * @param propertyName the property the value was about to be stored under, or null when the caller has no name
   *                     for it
   * @param valueOrigin  where the value came from, used only to describe it: a {@link Document} for a value copied
   *                     off another record, an {@link Expression} for one a query expression produced, a
   *                     {@link String} for the name of the parameter that supplied it, or null when unknown
   */
  public static Object coerceAndValidatePropertyValue(final Object value, final String propertyName,
      final Object valueOrigin) {
    if (value == null)
      return null; // a null value is a removal (SET) or simply not stored (CREATE/MERGE), not a stored value
    final Object coerced = TemporalUtil.toCoreJavaType(value);
    validatePropertyValue(coerced, propertyName, valueOrigin, false);
    return coerced;
  }

  private static void validatePropertyValue(final Object value, final String propertyName, final Object valueOrigin,
      final boolean insideList) {
    if (value instanceof List) {
      for (final Object element : (List<?>) value) {
        if (element instanceof Map<?, ?> map && isPointShaped(map)) {
          validatePointEntries(map, propertyName, valueOrigin, true);
          continue;
        }
        if (element instanceof Map<?, ?> map)
          throw new InvalidPropertyTypeException(refusal(map, propertyName, valueOrigin, true));
        if (element instanceof List)
          validatePropertyValue(element, propertyName, valueOrigin, true);
      }
    } else if (value instanceof Map<?, ?> map) {
      if (isPointShaped(map))
        validatePointEntries(map, propertyName, valueOrigin, insideList);
      else
        throw new InvalidPropertyTypeException(refusal(map, propertyName, valueOrigin, insideList));
    }
  }

  /**
   * Builds the refusal message. It answers three questions in order - what rule was broken, which value broke it,
   * and where that value came from - because the caller may have written none of them: on a copy path the only
   * thing the query text shows is the two variables.
   * <p>
   * The value is described by its keys and not by its contents. Neo4j prints the whole map, but this message
   * travels to a client and into the server log, and the keys are enough to identify which value was refused
   * without copying the data itself into either place. The list is capped for the same reason.
   */
  private static String refusal(final Map<?, ?> map, final String propertyName, final Object valueOrigin,
      final boolean insideList) {
    // No capacity hint: the finished length swings between roughly 120 and 300 characters depending on which
    // origin clause applies, so any single guess is wrong most of the time, and this runs only on the way to
    // throwing.
    final StringBuilder message = new StringBuilder();
    message.append("TypeError: InvalidPropertyType - Property values can only be of primitive types or arrays thereof. ")
        .append("Encountered a map ").append(describeKeys(map));
    if (insideList)
      message.append(" inside the list");
    message.append(" assigned to ");
    if (propertyName != null)
      message.append("property '").append(propertyName).append("'");
    else
      message.append("a property");

    if (valueOrigin instanceof Document source && source.getIdentity() != null) {
      // The one case nothing else explains: the caller named two variables and neither is a map. Say which record
      // holds the value, and why it was storable in the first place. A record with no identity yet cannot be one
      // the caller copied FROM, so it names no record rather than the string "null".
      message.append(", copied from record ").append(source.getIdentity())
          .append(". ArcadeDB SQL can store a map in a property and openCypher can not, so such a property can be"
              + " read and returned but never copied into another one");
    } else if (valueOrigin instanceof Expression expression) {
      final String text = expression.getText();
      if (namesAValue(text))
        message.append(", produced by the expression ").append(text);
    } else if (valueOrigin instanceof String parameterName)
      message.append(", supplied by parameter $").append(parameterName);

    return message.append(".").toString();
  }

  /**
   * Caps one key's own length. Bounding how MANY keys a message names is only half the bound: a single key is
   * caller-supplied text too, and one long enough would carry the same weight into the client response and the
   * server log that naming every key would.
   */
  private static String abbreviate(final String key) {
    return key.length() <= MAX_DESCRIBED_KEY_LENGTH ? key : key.substring(0, MAX_DESCRIBED_KEY_LENGTH) + "...";
  }

  /**
   * Whether an expression's own text may be echoed into the message: only when it NAMES the value rather than
   * spelling it out - {@code n.m}, {@code n.m.k}, {@code $p}.
   * <p>
   * This is a privacy bound, not a tidiness one. The rest of this message deliberately reports a refused map by its
   * keys and never its contents, because it travels to a client and into the server log; echoing the right-hand
   * side verbatim would put those contents back, since a map literal's text IS its values -
   * {@code SET n.x = {password: 'secret'}} would otherwise log the secret. A literal cannot match the shape below,
   * so it can never be echoed, and nothing is lost by refusing it: for a literal right-hand side the clause would
   * only repeat query text the caller just wrote, while for every other shape it is the only thing that says where
   * the value came from.
   * <p>
   * Matched by a character walk rather than a regex: this runs while building an exception message from
   * caller-supplied text, which is the last place to hand a backtracking matcher an unbounded string. The length
   * cap bounds the clause for the same reason.
   */
  private static boolean namesAValue(final String text) {
    if (text == null || text.isEmpty() || text.length() > MAX_ECHOED_EXPRESSION_LENGTH)
      return false;
    final char first = text.charAt(0);
    if (!Character.isLetter(first) && first != '_' && first != '$')
      return false;
    for (int i = 1; i < text.length(); i++) {
      final char c = text.charAt(i);
      if (!Character.isLetterOrDigit(c) && c != '_' && c != '.' && c != '$')
        return false;
    }
    return true;
  }

  /**
   * At most {@link #MAX_DESCRIBED_KEYS} keys, so a wide map cannot turn one refusal into a page of log. Which keys
   * those are follows the map's own iteration order: insertion order for a map the parser built, and whatever the
   * driver's map gives for a bound parameter. The cap is the behaviour worth relying on, not the selection.
   */
  private static String describeKeys(final Map<?, ?> map) {
    if (map.isEmpty())
      return "with no entries";
    final StringJoiner keys = new StringJoiner(", ", "[", "]");
    int described = 0;
    for (final Object key : map.keySet()) {
      if (described++ == MAX_DESCRIBED_KEYS) {
        keys.add("... " + (map.size() - MAX_DESCRIBED_KEYS) + " more");
        break;
      }
      keys.add(abbreviate(String.valueOf(key)));
    }
    return keys.toString();
  }

  /**
   * A heuristic, not a type check: any map with a non-null {@code crs} and numeric {@code x}/{@code y} is treated as
   * a Point, even one a query wrote as a plain literal rather than through {@code point()} - ArcadeDB has no
   * dedicated Geometry runtime type to check identity against instead (#4870). It is the most robust option
   * available today: identity-based exemption breaks as soon as a Point is copied from storage (see the class
   * javadoc above), and {@link #validatePointEntries} closes the map-smuggling loophole a looser key-presence check
   * would leave open.
   */
  private static boolean isPointShaped(final Map<?, ?> map) {
    return map.get("crs") != null && map.get("x") instanceof Number && map.get("y") instanceof Number;
  }

  /** A point-shaped map is exempt as a whole, but its own values are not: this refuses one smuggling a map/list of
   *  maps in under a key {@link #isPointShaped} doesn't look at. */
  private static void validatePointEntries(final Map<?, ?> map, final String propertyName, final Object valueOrigin,
      final boolean insideList) {
    for (final Object entry : map.values())
      validatePropertyValue(entry, propertyName, valueOrigin, insideList);
  }
}
