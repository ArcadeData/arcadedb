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
package com.arcadedb.graphql.schema;

import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.graphql.parser.Definition;
import com.arcadedb.graphql.parser.FragmentDefinition;
import com.arcadedb.graphql.parser.FragmentSpread;
import com.arcadedb.graphql.parser.InlineFragment;
import com.arcadedb.graphql.parser.Selection;
import com.arcadedb.graphql.parser.SelectionSet;

import java.util.*;
import java.util.function.*;

/**
 * The fragment definitions of one GraphQL document, and the expansion of the fragment spreads ({@code ...F}) and inline
 * fragments ({@code ... on T { }}) written in its selection sets into the fields they select.
 * <p>
 * Every consumer of a selection set resolves a selection through its field name, which an ellipsis selection does not
 * have: it must read the selections through {@link #expand} rather than through {@link SelectionSet#getSelections()}
 * directly, or the fields reached through a fragment are dropped and the fragment itself turns into a {@code null}
 * response key (issue #7770).
 * <p>
 * Directives written on a fragment spread or an inline fragment are not evaluated, the same as the {@code @skip} and
 * {@code @include} directives on a field, which this module does not implement either.
 */
public final class GraphQLFragments {
  public static final GraphQLFragments NONE = new GraphQLFragments(Collections.emptyMap());

  private final Map<String, FragmentDefinition> definitions;

  private GraphQLFragments(final Map<String, FragmentDefinition> definitions) {
    this.definitions = definitions;
  }

  /**
   * Collects the fragment definitions of a document, wherever they are declared in it: a fragment may be written after
   * the operation that spreads it.
   */
  public static GraphQLFragments of(final List<Definition> documentDefinitions) {
    Map<String, FragmentDefinition> definitions = null;
    for (final Definition definition : documentDefinitions)
      if (definition instanceof FragmentDefinition fragment) {
        if (definitions == null)
          definitions = new HashMap<>();
        final String name = fragment.getName();
        if (definitions.putIfAbsent(name, fragment) != null)
          throw new CommandParsingException("GraphQL fragment '" + name + "' is defined more than once");
      }
    return definitions != null ? new GraphQLFragments(definitions) : NONE;
  }

  /**
   * Rejects a selection set that spreads a fragment the document does not define, or whose fragments spread each other
   * in a cycle. Both are validation errors in the GraphQL specification, and checking them before any record is read
   * keeps them parsing errors rather than failures raised lazily while the result set is iterated.
   * <p>
   * Only the fragments reachable from {@code selectionSet} are walked: a fragment the operation never spreads is not
   * checked, although the specification rejects an unused fragment too. It can never be expanded, so it cannot fail.
   */
  public void validate(final SelectionSet selectionSet) {
    validate(selectionSet, new ArrayList<>(), new HashSet<>());
  }

  private void validate(final SelectionSet selectionSet, final List<String> path, final Set<String> validated) {
    if (selectionSet == null)
      return;

    for (final Selection selection : selectionSet.getSelections()) {
      final FragmentSpread spread = selection.getFragmentSpread();
      final InlineFragment inline = selection.getInlineFragment();
      if (spread != null) {
        final String name = spread.getName();
        final FragmentDefinition fragment = getDefinition(name);
        if (path.contains(name))
          throw new CommandParsingException(
              "GraphQL fragment '" + name + "' spreads itself through a cycle: " + String.join(" -> ", path) + " -> " + name);
        if (!validated.add(name))
          // ALREADY WALKED FROM ANOTHER SPREAD: WALKING IT AGAIN CANNOT FIND ANYTHING NEW AND ONLY MULTIPLIES THE WORK
          continue;
        path.add(name);
        validate(fragment.getSelectionSet(), path, validated);
        path.removeLast();
      } else if (inline != null)
        validate(inline.getSelectionSet(), path, validated);
      else
        validate(selection.getSelectionSet(), path, validated);
    }
  }

  /**
   * Returns the selections with every fragment spread and inline fragment replaced, recursively, by the selections it
   * contributes, in document order. A fragment whose type condition does not apply is skipped entirely.
   *
   * @param selections     the selections as written in the document, may be null
   * @param typeConditions tells whether a fragment written {@code on T} applies to the object being resolved, given
   *                       the name {@code T}. It is only called for a fragment that carries a type condition
   */
  public List<Selection> expand(final List<Selection> selections, final Predicate<String> typeConditions) {
    if (selections == null)
      return null;

    boolean hasFragments = false;
    for (int i = 0; i < selections.size(); i++)
      if (isFragment(selections.get(i))) {
        hasFragments = true;
        break;
      }
    if (!hasFragments)
      // THE COMMON CASE: NO ALLOCATION
      return selections;

    final List<Selection> expanded = new ArrayList<>(selections.size() + 4);
    expand(selections, typeConditions, expanded, new HashSet<>());
    return expanded;
  }

  private void expand(final List<Selection> selections, final Predicate<String> typeConditions,
      final List<Selection> expanded, final Set<String> spread) {
    for (final Selection selection : selections) {
      final FragmentSpread fragmentSpread = selection.getFragmentSpread();
      final InlineFragment inline = selection.getInlineFragment();
      if (fragmentSpread != null) {
        final String name = fragmentSpread.getName();
        if (!spread.add(name))
          // THE SAME FRAGMENT SPREAD TWICE AT THE SAME LEVEL CONTRIBUTES THE SAME FIELDS TWICE, WHICH MERGE INTO ONE.
          // SKIPPING IT ALSO BOUNDS THE WORK OF A DOCUMENT WHOSE FRAGMENTS SPREAD EACH OTHER A NUMBER OF TIMES
          // EXPONENTIAL IN THEIR NESTING, AND OF A CYCLE THAT WAS NOT VALIDATED AWAY
          continue;
        final FragmentDefinition fragment = getDefinition(name);
        if (applies(fragment.getTypeConditionName(), typeConditions) && fragment.getSelectionSet() != null)
          expand(fragment.getSelectionSet().getSelections(), typeConditions, expanded, spread);
      } else if (inline != null) {
        if (applies(inline.getTypeConditionName(), typeConditions) && inline.getSelectionSet() != null)
          expand(inline.getSelectionSet().getSelections(), typeConditions, expanded, spread);
      } else
        expanded.add(selection);
    }
  }

  private FragmentDefinition getDefinition(final String name) {
    final FragmentDefinition fragment = definitions.get(name);
    if (fragment == null)
      throw new CommandParsingException("GraphQL fragment '" + name + "' is not defined in the document");
    return fragment;
  }

  private static boolean applies(final String typeCondition, final Predicate<String> typeConditions) {
    return typeCondition == null || typeConditions.test(typeCondition);
  }

  private static boolean isFragment(final Selection selection) {
    return selection.getFragmentSpread() != null || selection.getInlineFragment() != null;
  }
}
