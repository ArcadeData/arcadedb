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
package com.arcadedb.graph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

/**
 * One hop of APOC's relationship pattern syntax ({@code relTypes}, {@code relTypesAndDirections}, the
 * {@code relationshipFilter} of {@code path.*}): alternatives separated by {@code |}, each a type name with an optional
 * direction marker, {@code TYPE>} outgoing, {@code <TYPE} or {@code TYPE<} incoming (APOC reads both, issue #12674),
 * {@code TYPE} both; {@code >} or {@code <} alone is every type in that direction. A blank entry is skipped, so a blank
 * pattern has no entry.
 * <p>
 * The {@code path.*} procedures read it since #10025; the {@code node.*} functions and the {@code algo.*} path finders
 * read the whole string as one type name, so {@code apoc.node.degree(n, 'KNOWS>')} answered 0 (issue #12103). Every
 * reader parses it here.
 * <p>
 * A type name that itself contains {@code |}, {@code <} or {@code >} cannot be written in this syntax: it reads as the
 * separator or the marker.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class RelationshipPattern {
  /**
   * One alternative of the hop.
   *
   * @param type      the edge type, {@code null} for every type
   * @param direction {@code OUT} for {@code TYPE>}, {@code IN} for {@code <TYPE} or {@code TYPE<}, {@code BOTH} without a marker
   */
  public record Entry(String type, Vertex.DIRECTION direction) {
  }

  /** Every type: what {@link #typesFor} answers for a direction an alternative without a type follows. Shared, never written. */
  private static final String[] EVERY_TYPE = new String[0];

  /** The pattern that names nothing: what a call without one, or a blank one, reads. Immutable, so shared. */
  private static final RelationshipPattern EMPTY = new RelationshipPattern(List.of());

  private final List<Entry> entries;

  private RelationshipPattern(final List<Entry> entries) {
    this.entries = entries;
  }

  /**
   * Parses {@code pattern}: a string, or a collection of strings, each an alternative (the list form ArcadeDB accepted,
   * {@code ['ROAD>', '<RAIL']} is {@code 'ROAD>|<RAIL'}). {@code null} is the empty pattern.
   *
   * @param functionName the function or procedure reading it, as the error messages name it
   */
  public static RelationshipPattern parse(final String functionName, final Object pattern) {
    final String text = text(functionName, pattern);
    if (text == null)
      return EMPTY;

    // node.degree and node.relationship.exists parse the argument once per row, and mostly it is one alternative: no split, no list
    if (text.indexOf('|') < 0) {
      final Entry entry = parseEntry(functionName, text.trim());
      return entry == null ? EMPTY : new RelationshipPattern(List.of(entry));
    }

    final List<Entry> entries = new ArrayList<>();
    int start = 0;
    while (start <= text.length()) {
      int end = text.indexOf('|', start);
      if (end < 0)
        end = text.length();
      final Entry entry = parseEntry(functionName, text.substring(start, end).trim());
      if (entry != null)
        entries.add(entry);
      start = end + 1;
    }
    return entries.isEmpty() ? EMPTY : new RelationshipPattern(entries);
  }

  /** One alternative, {@code null} when it is blank. */
  private static Entry parseEntry(final String functionName, final String entry) {
    if (entry.isEmpty())
      return null;

    final int last = entry.length() - 1;
    final boolean leadingIncoming = entry.charAt(0) == '<';
    final boolean trailingIncoming = last > 0 && entry.charAt(last) == '<';
    final boolean outgoing = entry.charAt(last) == '>';
    if (outgoing && (leadingIncoming || last > 0 && entry.charAt(last - 1) == '<')) {
      final String inner = entry.substring(leadingIncoming ? 1 : 0, last).trim();
      final String name = inner.endsWith("<") ? inner.substring(0, inner.length() - 1).trim() : inner;
      throw new IllegalArgumentException(functionName + "(): relationship filter '" + entry + "' carries both direction markers: "
          + (name.isEmpty() ? "leave the filter empty" : "write '" + name + "'") + " to follow it in both directions");
    }

    final boolean incoming = leadingIncoming || trailingIncoming;
    final String name = entry.substring(leadingIncoming ? 1 : 0, trailingIncoming || outgoing ? last : entry.length()).trim();
    final Vertex.DIRECTION direction = incoming ? Vertex.DIRECTION.IN : outgoing ? Vertex.DIRECTION.OUT : Vertex.DIRECTION.BOTH;
    return new Entry(name.isEmpty() ? null : name, direction);
  }

  private static String text(final String functionName, final Object pattern) {
    return switch (pattern) {
      case null -> null;
      case String s -> s;
      case Collection<?> collection -> {
        final StringJoiner joined = new StringJoiner("|");
        for (final Object item : collection)
          if (item != null)
            joined.add(item.toString());
        yield joined.toString();
      }
      case Map<?, ?> ignored ->
          throw new IllegalArgumentException(functionName + "(): the relationship types must be a string or a list of strings, got a map");
      default -> pattern.toString();
    };
  }

  /** The alternatives in the order written, a repeated one included. */
  public List<Entry> entries() {
    return entries;
  }

  /** Whether the pattern names nothing, which every reader takes as every relationship in both directions. */
  public boolean isEmpty() {
    return entries.isEmpty();
  }

  /** The types named, without the direction markers and without repetition, in the order written. */
  public Set<String> typeNames() {
    final Set<String> names = new LinkedHashSet<>();
    for (final Entry entry : entries)
      if (entry.type() != null)
        names.add(entry.type());
    return names;
  }

  /**
   * The types the hop follows in {@code direction}: {@code null} when it follows none in that direction, an empty array
   * when it follows every type. An empty pattern follows every type both ways.
   */
  public String[] typesFor(final Vertex.DIRECTION direction) {
    if (entries.isEmpty())
      return EVERY_TYPE;
    final Set<String> names = new LinkedHashSet<>();
    for (final Entry entry : entries) {
      if (entry.direction() != Vertex.DIRECTION.BOTH && entry.direction() != direction)
        continue;
      if (entry.type() == null)
        return EVERY_TYPE;
      names.add(entry.type());
    }
    return names.isEmpty() ? null : names.toArray(new String[0]);
  }

  /**
   * The one direction every alternative shares, so that a reader walking a single direction can follow the whole
   * pattern; {@code null} when the alternatives mix directions ({@code 'ROAD>|<RAIL'}).
   */
  public Vertex.DIRECTION commonDirection() {
    Vertex.DIRECTION common = null;
    for (final Entry entry : entries) {
      if (common == null)
        common = entry.direction();
      else if (common != entry.direction())
        return null;
    }
    return common == null ? Vertex.DIRECTION.BOTH : common;
  }

  /**
   * This pattern with the alternatives that carry no direction marker followed in {@code direction} only: the direction
   * argument ArcadeDB accepts beside the pattern. An empty pattern becomes every relationship in {@code direction}
   * ({@code '>'} or {@code '<'}); {@code this} for {@code BOTH} or when every alternative carries a marker.
   */
  public RelationshipPattern withUnmarked(final Vertex.DIRECTION direction) {
    if (direction == Vertex.DIRECTION.BOTH)
      return this;
    if (entries.isEmpty())
      return new RelationshipPattern(List.of(new Entry(null, direction)));
    List<Entry> restricted = null;
    for (int i = 0; i < entries.size(); i++) {
      final Entry entry = entries.get(i);
      if (entry.direction() != Vertex.DIRECTION.BOTH)
        continue;
      if (restricted == null)
        restricted = new ArrayList<>(entries);
      restricted.set(i, new Entry(entry.type(), direction));
    }
    return restricted == null ? this : new RelationshipPattern(restricted);
  }

  /** Whether {@code edge}, met on {@code direction} from the node at hand, is one the pattern follows. */
  public boolean follows(final Edge edge, final Vertex.DIRECTION direction) {
    // what typesFor(direction) says, asked of one edge: it allocates nothing, as the path rebuild asks once per edge
    if (entries.isEmpty())
      return true;
    for (final Entry entry : entries)
      if ((entry.direction() == Vertex.DIRECTION.BOTH || entry.direction() == direction) && (entry.type() == null
          || edge.getType().instanceOf(entry.type())))
        return true;
    return false;
  }
}
