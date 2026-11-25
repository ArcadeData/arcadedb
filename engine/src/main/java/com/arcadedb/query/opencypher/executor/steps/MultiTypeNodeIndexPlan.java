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
package com.arcadedb.query.opencypher.executor.steps;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Identifiable;
import com.arcadedb.engine.Bucket;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.opencypher.Labels;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.security.SecurityHelper;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * Where the candidates of a node pattern that names no single type, pinned to values, are read from (issue #12522): a pattern with
 * no label, or with several.
 * <p>
 * Such a pattern names no type, so the index of the property it is pinned on cannot be chosen from the pattern, and the statement
 * used to walk every vertex of every type the labels select and evaluate the predicate on each one. The union over the types is
 * what that walk computes, so it is computed from the indexes instead: an index that covers a bucket answers for the vertices of
 * that bucket with a seek, and only the buckets no index covers are walked.
 * <p>
 * <b>The unit of the plan is the bucket, not the type.</b> A vertex lives in exactly one bucket, and a bucket belongs to exactly one
 * type, so a plan that gives every vertex bucket to exactly one source answers every vertex once and none twice, whatever the
 * hierarchy above it looks like. That is what a plan by type cannot say: an index declared on a type holds the vertices of its sub
 * types as well, and a type under two types (a diamond, which is what openCypher creates for a node with two labels:
 * {@code CREATE (n:A:B)} makes the type {@code A~B} under both) is held by the index of each branch. Read per bucket, the same
 * facts are one sentence: the bucket of the diamond is covered by two indexes and goes to the first of them, and the seek of the
 * second one drops the vertices of the buckets that are not its own to answer.
 * <p>
 * An index is used only when the value can be keyed by it the way the walk compares it ({@link #isKeyableBy}): the seek only
 * narrows the candidates, the step still evaluates the pattern on each of them, so what it must never do is miss a vertex the walk
 * would have answered.
 * <p>
 * A pattern pinned to a list of values ({@code WHERE n.uuid IN $keys}, issue #12546) is planned the same way, by {@link #ofInList}:
 * the buckets, the choice of the index of each one and the seek that drops the buckets another index answers for are the ones of the
 * equality. What differs is the key: the index must stand on that one property, and every key of the list must be keyable by it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class MultiTypeNodeIndexPlan {
  /** Two to the 53rd: the largest magnitude from which a double stops standing for one long. */
  private static final long EXACT_DOUBLE_LIMIT = 1L << 53;

  /**
   * What {@link #of} answers when an index could answer the pattern but cannot be keyed by the value of this row (a double against
   * an integer key, a text against a number): the walk answers this row, and another row may well be answered by the index.
   * Told apart from null, no index at all, which no row can change.
   */
  static final MultiTypeNodeIndexPlan UNKEYABLE = new MultiTypeNodeIndexPlan(List.of(), List.of());

  private final List<Part>         parts;
  private final List<DocumentType> seekedTypes;

  private MultiTypeNodeIndexPlan(final List<Part> parts, final List<DocumentType> seekedTypes) {
    this.parts = parts;
    this.seekedTypes = seekedTypes;
  }

  /**
   * A source of candidates: the seek of an index over some of its buckets, or the walk of a type or of a bucket.
   *
   * @param index         the index sought, null for a walk
   * @param propertyNames the properties of the key of the index, in its order (a seek)
   * @param buckets       the buckets the seek answers for (a seek): fewer than the ones the index covers when another index
   *                      answers for the rest
   * @param wholeIndex    true when the seek answers for every bucket the index covers, so its cursor needs no filter (a seek)
   * @param typeName      the type walked, when the walk is the one of every bucket of a type, null otherwise (a walk)
   * @param bucketNames   the buckets walked (a walk)
   */
  record Part(TypeIndex index, String[] propertyNames, BitSet buckets, boolean wholeIndex, String typeName, String[] bucketNames) {
    boolean isSeek() {
      return index != null;
    }

    /** The walk of one bucket. */
    static Part walkOf(final String bucketName) {
      return new Part(null, null, null, false, null, new String[] { bucketName });
    }
  }

  /** An index that can answer the pattern, and what it covers. */
  private record Cover(TypeIndex index, String[] propertyNames, int[] bucketIds) {
  }

  List<Part> parts() {
    return parts;
  }

  /**
   * The plan of the pattern, or null when no index answers it: the walk of the types is then the answer. {@link #UNKEYABLE} when an
   * index does, but not with these values.
   *
   * @param schema      the schema of the database
   * @param labels      the labels written on the pattern: none for an unlabelled node, which every vertex type can satisfy
   * @param disjunction whether the labels were written as alternatives ({@code A|B}) rather than as a conjunction
   * @param pinned      the properties the pattern pins, with the values they are pinned to (none of them null)
   */
  static MultiTypeNodeIndexPlan of(final Schema schema, final List<String> labels, final boolean disjunction,
      final Map<String, Object> pinned) {
    return plan(schema, labels, disjunction, (index, unkeyable) -> coverOf(index, pinned, unkeyable));
  }

  /**
   * The plan of a pattern whose property is pinned to a list of values ({@code n.uuid IN $keys}, issue #12546): the same plan as
   * {@link #of}, with the same buckets and the same index choice, but an index answers only when it stands on that one property
   * alone, and only when EVERY key of the list can be keyed by it. One key that cannot is enough to leave the buckets to the walk,
   * since the seek of the other keys would miss what the walk finds for that one.
   *
   * @param property the property the list pins
   * @param keys     the distinct keys of the list, none of them null, each one a one-element array as the seek of the list reads
   *                 them: an empty list is a plan too, whose seeks are never opened
   * @return null when no index answers the property, {@link #UNKEYABLE} when one does but not with these keys
   */
  static MultiTypeNodeIndexPlan ofInList(final Schema schema, final List<String> labels, final boolean disjunction, final String property,
      final List<Object[]> keys) {
    // whether every key can be keyed depends on the type of the key of the index only, and many indexes share one
    final Boolean[] keyableByKeyType = new Boolean[Type.values().length];
    return plan(schema, labels, disjunction, (index, unkeyable) -> coverOfInList(index, property, keys, keyableByKeyType, unkeyable));
  }

  /** What tells whether an index can answer the pattern, and what it covers when it can. */
  @FunctionalInterface
  private interface CoverFinder {
    /** The cover of the index, null if it cannot answer; {@code unkeyable[0]} is set when it could with other values. */
    Cover coverOf(TypeIndex index, boolean[] unkeyable);
  }

  private static MultiTypeNodeIndexPlan plan(final Schema schema, final List<String> labels, final boolean disjunction,
      final CoverFinder finder) {
    final List<DocumentType> types = Labels.matchingVertexTypes(schema, labels, disjunction);
    if (types.isEmpty())
      return null;

    // When every vertex type is selected, the index each type declares is every index there is. When the labels select some of them,
    // the indexes that answer for their buckets can be declared on a type above them (the composite type of (n:A:B) is under A and
    // under B, and holds none of its own), so those are collected too, once each
    final boolean everyType = labels == null || labels.isEmpty();
    final Set<TypeIndex> seen = everyType ? null : Collections.newSetFromMap(new IdentityHashMap<>());
    final boolean[] unkeyable = new boolean[1];
    final List<Cover> covers = new ArrayList<>();
    final BitSet vertexBuckets = new BitSet();
    for (final DocumentType type : types) {
      for (final Bucket bucket : type.getBuckets(false))
        vertexBuckets.set(bucket.getFileId());
      for (final TypeIndex index : type.getAllIndexes(!everyType)) {
        if (seen != null && !seen.add(index))
          continue;
        final Cover cover = finder.coverOf(index, unkeyable);
        if (cover != null)
          covers.add(cover);
      }
    }
    if (covers.isEmpty())
      return unkeyable[0] ? UNKEYABLE : null;

    // The index with the longest key first, since it narrows most; among equals the one that covers more buckets, so that fewer
    // seeks are made; the name settles the rest, so that the same schema is always planned the same way
    covers.sort(Comparator.comparingInt((Cover c) -> -c.propertyNames().length)
        .thenComparingInt(c -> -c.bucketIds().length)
        .thenComparing(c -> c.index().getName()));

    final BitSet answered = new BitSet();
    final List<Part> parts = new ArrayList<>();
    for (final Cover cover : covers) {
      final BitSet mine = new BitSet();
      for (final int bucketId : cover.bucketIds())
        if (vertexBuckets.get(bucketId) && !answered.get(bucketId))
          mine.set(bucketId);
      if (mine.isEmpty())
        continue; // every bucket this index covers is answered by an index chosen before it
      answered.or(mine);
      parts.add(new Part(cover.index(), cover.propertyNames(), mine, mine.cardinality() == cover.bucketIds().length, null, null));
    }

    if (parts.isEmpty())
      return null; // no index covers a bucket of the types after all: the walk of the types answers

    // What no index covers is walked, bucket by bucket unless the whole type is left
    final List<DocumentType> seekedTypes = new ArrayList<>();
    for (final DocumentType type : types) {
      final List<Bucket> buckets = type.getBuckets(false);
      final List<String> uncovered = new ArrayList<>(buckets.size());
      for (final Bucket bucket : buckets)
        if (!answered.get(bucket.getFileId()))
          uncovered.add(bucket.getName());
      if (uncovered.size() < buckets.size())
        seekedTypes.add(type);
      if (uncovered.isEmpty())
        continue;
      if (uncovered.size() == buckets.size())
        parts.add(new Part(null, null, null, false, type.getName(), uncovered.toArray(new String[0])));
      else
        for (final String bucketName : uncovered)
          parts.add(Part.walkOf(bucketName));
    }

    return new MultiTypeNodeIndexPlan(parts, seekedTypes);
  }

  /**
   * Refuses the statement to a user who may not read a type an index answers for, the way the walk of its buckets does.
   * <p>
   * A walk opens the buckets of every type, and a bucket refuses the user who may not read it when it is opened, whatever the value
   * is. A seek loads a record only when the index holds the value, so left to itself it would refuse only for a value the denied
   * type holds, and the statement would tell a user who may not read the type whether it holds a value (the per-type ACL is
   * enforced in the bucket, which an index read does not reach until a record is loaded, GHSA-2c8m). The type is checked once, not
   * every bucket of it: the buckets of a type carry the same access.
   *
   * @throws SecurityException when the current user may not read one of the types
   */
  void checkReadable(final DatabaseInternal database) {
    for (final DocumentType type : seekedTypes)
      SecurityHelper.checkAccessOnType(database, type, SecurityDatabaseUser.ACCESS.READ_RECORD);
  }

  /**
   * True when a walk of the plan reads a record: the statement that has one cannot be answered by seeks alone, and a value the
   * input row decides would repeat that walk for every row.
   */
  boolean walksARecord(final Database database) {
    for (final Part part : parts)
      if (!part.isSeek())
        for (final String bucketName : part.bucketNames())
          if (database.countBucket(bucketName) > 0)
            return true;
    return false;
  }

  /**
   * What the index covers if it can answer the pinned values, null otherwise: it is ready for queries, it is a key index, every
   * property of its key is pinned, and each value can be keyed by it. {@code unkeyable[0]} is set when the index is one that
   * would answer if a value could be keyed by it.
   */
  private static Cover coverOf(final TypeIndex index, final Map<String, Object> pinned, final boolean[] unkeyable) {
    try {
      if (!index.isReadyForQueries())
        return null; // an index a concurrent DDL is rebuilding answers nothing yet
      final List<String> properties = index.getPropertyNamesIfExactKeyLookup();
      if (properties == null || properties.isEmpty())
        return null; // not a key index (a FULL_TEXT index answers by token, issue #8439) or dropped meanwhile
      final Type[] keyTypes = index.getKeyTypes();
      if (keyTypes == null || keyTypes.length < properties.size())
        return null; // an index that does not say what its key holds cannot be asked with a value
      final String[] names = new String[properties.size()];
      for (int i = 0; i < names.length; i++) {
        names[i] = properties.get(i);
        // a prefix of the key does not answer an equality on the whole key
        if (!pinned.containsKey(names[i]))
          return null;
      }
      for (int i = 0; i < names.length; i++)
        if (!isKeyableBy(keyTypes[i], pinned.get(names[i]))) {
          unkeyable[0] = true;
          return null;
        }
      return coverOfBuckets(index, names);
    } catch (final IndexException e) {
      return null; // dropped or rebuilt since it was listed: not a candidate, the walk answers for its buckets
    }
  }

  /**
   * What the index covers if it can answer a list of keys of one property, null otherwise: it is ready for queries, it is a key
   * index on that property alone, and every key can be keyed by it. {@code unkeyable[0]} is set when the index is one that would
   * answer if every key could be keyed by it.
   */
  private static Cover coverOfInList(final TypeIndex index, final String property, final List<Object[]> keys,
      final Boolean[] keyableByKeyType, final boolean[] unkeyable) {
    try {
      if (!index.isReadyForQueries())
        return null; // an index a concurrent DDL is rebuilding answers nothing yet
      final List<String> properties = index.getPropertyNamesIfExactKeyLookup();
      // not a key index (a FULL_TEXT index answers by token, issue #8439), dropped meanwhile, or a key that is more than the
      // property: a list is no equality on the whole key of an index on more columns
      if (properties == null || properties.size() != 1 || !properties.getFirst().equals(property))
        return null;
      final Type[] keyTypes = index.getKeyTypes();
      if (keyTypes == null || keyTypes.length < 1)
        return null; // an index that does not say what its key holds cannot be asked with a value
      if (!allKeyableBy(keyTypes[0], keys, keyableByKeyType)) {
        unkeyable[0] = true;
        return null;
      }
      return coverOfBuckets(index, new String[] { property });
    } catch (final IndexException e) {
      return null; // dropped or rebuilt since it was listed: not a candidate, the walk answers for its buckets
    }
  }

  private static Cover coverOfBuckets(final TypeIndex index, final String[] propertyNames) {
    final IndexInternal[] subIndexes = index.getIndexesOnBuckets();
    final int[] bucketIds = new int[subIndexes.length];
    for (int i = 0; i < bucketIds.length; i++)
      bucketIds[i] = subIndexes[i].getAssociatedBucketId();
    return new Cover(index, propertyNames, bucketIds);
  }

  /** True when every key can be keyed by an index of this key type, which is asked once per key type and not once per index. */
  private static boolean allKeyableBy(final Type keyType, final List<Object[]> keys, final Boolean[] keyableByKeyType) {
    if (keyType == null)
      return keys.isEmpty();
    Boolean all = keyableByKeyType[keyType.ordinal()];
    if (all == null) {
      boolean every = true;
      for (int i = 0; every && i < keys.size(); i++)
        every = isKeyableBy(keyType, keys.get(i)[0]);
      all = every;
      keyableByKeyType[keyType.ordinal()] = all;
    }
    return all;
  }

  /**
   * True when the value can be keyed by an index of this key type exactly the way the walk compares it, so that the seek misses
   * nothing the walk would have found.
   * <p>
   * A string for a string key, a boolean for a boolean key, an integer for an integer key. A floating value against an integer key
   * is left to the walk, which compares the stored long to the double as doubles: the key of the seek is the long of the double, and
   * the two stop agreeing past 2^53. The text of a RID against a number key is left to the walk as well (issue #11933), and so is
   * every key type the pattern's value does not stand for exactly (a decimal, a temporal): such a value reaches the index as a
   * Cypher value whose key conversion does not decide what Cypher's equality decides.
   * <p>
   * The type of the key is the one of the index, not the one of the property: an index can stand on a property the type does not
   * declare (issue #11777), and the key it holds is what the lookup is made with.
   */
  static boolean isKeyableBy(final Type keyType, final Object value) {
    if (keyType == null || value == null)
      return false;
    switch (keyType) {
    case STRING:
      return value instanceof String;
    case BOOLEAN:
      return value instanceof Boolean;
    case BYTE:
    case SHORT:
    case INTEGER:
    case LONG:
      return isIntegral(value);
    case FLOAT:
    case DOUBLE:
      return value instanceof Float || value instanceof Double || (isIntegral(value) && isExactAsDouble(((Number) value).longValue()));
    default:
      return false;
    }
  }

  private static boolean isExactAsDouble(final long value) {
    return value >= -EXACT_DOUBLE_LIMIT && value <= EXACT_DOUBLE_LIMIT;
  }

  private static boolean isIntegral(final Object value) {
    return value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte;
  }

  /**
   * The records of {@code source} that live in one of the buckets: what a seek answers when another index answers for the rest of
   * the buckets its index covers.
   */
  static Iterator<Identifiable> onlyBuckets(final Iterator<Identifiable> source, final BitSet buckets) {
    return new Iterator<>() {
      private Identifiable next = advance();

      private Identifiable advance() {
        while (source.hasNext()) {
          final Identifiable candidate = source.next();
          if (buckets.get(candidate.getIdentity().getBucketId()))
            return candidate;
        }
        return null;
      }

      @Override
      public boolean hasNext() {
        return next != null;
      }

      @Override
      public Identifiable next() {
        if (next == null)
          throw new NoSuchElementException();
        final Identifiable current = next;
        next = advance();
        return current;
      }
    };
  }
}
