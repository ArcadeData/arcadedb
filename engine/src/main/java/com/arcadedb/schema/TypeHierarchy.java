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
package com.arcadedb.schema;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Walks over the type hierarchy that meet each type once, whatever the number of paths that lead to it. A type can have several
 * super types, so a hierarchy is a lattice and not a tree: a recursion over {@link DocumentType#getSuperTypes()} or
 * {@link DocumentType#getSubTypes()} meets a type once per path that leads to it, where these walks meet it once.
 * <p>
 * Types are told apart by name, which is unique in a schema, so the walks also work over the types of a remote schema. No walk
 * includes the type it starts from, and all of them end over a hierarchy that holds a cycle. The order is depth first: the direct
 * types in their declared order, each followed by what is beyond it. The walks that look for something ({@link #findInSuperTypes},
 * {@link #anySuperType} and their sub type twins) stop at the first type that answers, and do not read what is beyond it.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
public final class TypeHierarchy {
  private TypeHierarchy() {
  }

  /**
   * Every type below {@code type}, at any depth, each once, in the order a depth-first walk meets them first: the direct sub types in
   * their declared order, each followed by what is below it.
   */
  public static List<DocumentType> allSubTypes(final DocumentType type) {
    return collect(type, false);
  }

  /**
   * Every type above {@code type}, at any depth, each once, in the order a depth-first walk meets them first: the direct super types in
   * their declared order, each followed by what is above it.
   */
  public static List<DocumentType> allSuperTypes(final DocumentType type) {
    return collect(type, true);
  }

  /** The first non-null answer of {@code lookup} over the types above {@code type}, in the order of {@link #allSuperTypes}, or null. */
  public static <T> T findInSuperTypes(final DocumentType type, final Function<DocumentType, T> lookup) {
    return find(type, true, type.getSuperTypes(), lookup);
  }

  /** The first non-null answer of {@code lookup} over the types below {@code type}, in the order of {@link #allSubTypes}, or null. */
  public static <T> T findInSubTypes(final DocumentType type, final Function<DocumentType, T> lookup) {
    return find(type, false, type.getSubTypes(), lookup);
  }

  /** Whether {@code test} holds for any type above {@code type}, asked in the order of {@link #allSuperTypes}. */
  public static boolean anySuperType(final DocumentType type, final Predicate<DocumentType> test) {
    return find(type, true, type.getSuperTypes(), t -> test.test(t) ? Boolean.TRUE : null) != null;
  }

  /** Whether {@code test} holds for any type below {@code type}, asked in the order of {@link #allSubTypes}. */
  public static boolean anySubType(final DocumentType type, final Predicate<DocumentType> test) {
    return find(type, false, type.getSubTypes(), t -> test.test(t) ? Boolean.TRUE : null) != null;
  }

  private static List<DocumentType> collect(final DocumentType start, final boolean up) {
    final List<DocumentType> direct = up ? start.getSuperTypes() : start.getSubTypes();
    if (direct.isEmpty())
      // the common case: nothing to walk, nothing allocated
      return List.of();

    final List<DocumentType> result = new ArrayList<>();
    find(start, up, direct, t -> {
      result.add(t);
      return null;
    });
    return result;
  }

  private static <T> T find(final DocumentType start, final boolean up, final List<DocumentType> direct,
      final Function<DocumentType, T> lookup) {
    if (direct.isEmpty())
      // the common case: nothing to walk, nothing allocated
      return null;

    final Set<String> visited = new HashSet<>();
    visited.add(start.getName());
    final ArrayDeque<Iterator<DocumentType>> pending = new ArrayDeque<>();
    pending.push(direct.iterator());
    while (!pending.isEmpty()) {
      final Iterator<DocumentType> level = pending.peek();
      if (!level.hasNext()) {
        pending.pop();
        continue;
      }
      final DocumentType next = level.next();
      if (visited.add(next.getName())) {
        final T found = lookup.apply(next);
        if (found != null)
          // what is beyond this type is never read
          return found;
        pending.push((up ? next.getSuperTypes() : next.getSubTypes()).iterator());
      }
    }
    return null;
  }
}
