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
import com.arcadedb.graphql.parser.AbstractField;
import com.arcadedb.graphql.parser.Argument;
import com.arcadedb.graphql.parser.Arguments;
import com.arcadedb.graphql.parser.Definition;
import com.arcadedb.graphql.parser.Directive;
import com.arcadedb.graphql.parser.Directives;
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
 * The expansion also evaluates the built-in {@code @skip(if:)} and {@code @include(if:)} directives, wherever the
 * specification allows them: on a field, on a fragment spread and on an inline fragment. A selection they exclude is
 * dropped from the expanded list like a fragment whose type condition does not apply, so every consumer honors them
 * without knowing they exist (issue #8615). Their {@code if} argument can reference the variables of the operation,
 * which is why the instance a query is executed with is bound to them through {@link #withVariables}.
 */
public final class GraphQLFragments {
  public static final GraphQLFragments NONE = new GraphQLFragments(Collections.emptyMap(), Collections.emptyMap());

  private static final String SKIP    = "skip";
  private static final String INCLUDE = "include";
  private static final String IF      = "if";

  /**
   * The longest chain of fragments spreading each other that a document may contain. Validation and expansion recurse
   * once per link, so an unbounded chain - cheap to write, since each fragment is a single line - would end in a
   * {@link StackOverflowError} rather than in a parsing error.
   */
  public static final int MAX_FRAGMENT_DEPTH = 100;

  private final Map<String, FragmentDefinition> definitions;

  /**
   * The variable values of the operation being executed, which the {@code if} argument of {@code @skip} and
   * {@code @include} can reference. Empty until {@link #withVariables} binds them.
   */
  private final Map<String, Object> variables;

  private GraphQLFragments(final Map<String, FragmentDefinition> definitions, final Map<String, Object> variables) {
    this.definitions = definitions;
    this.variables = variables;
  }

  /**
   * These fragments, bound to the variable values of the operation being executed. The directives the expansion
   * evaluates read their {@code if} argument from them.
   */
  public GraphQLFragments withVariables(final Map<String, Object> variables) {
    return new GraphQLFragments(definitions, variables != null ? variables : Collections.emptyMap());
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
        rejectOnDefinition(fragment);
      }
    return definitions != null ? new GraphQLFragments(definitions, Collections.emptyMap()) : NONE;
  }

  /**
   * {@code @skip} and {@code @include} are not allowed on a fragment definition, only on the spreads of it. Ignoring one
   * written there would return the fields the document asked to leave out.
   */
  private static void rejectOnDefinition(final FragmentDefinition fragment) {
    final Directives directives = fragment.getDirectives();
    if (directives != null)
      for (final Directive directive : directives.getDirectives())
        if (isInclusionDirective(directive))
          throw new CommandParsingException(
              "Directive @" + directive.getName() + " cannot be used on the definition of fragment '" + fragment.getName()
                  + "': write it on the spread '..." + fragment.getName() + "' instead");
  }

  /**
   * Rejects a selection set that spreads a fragment the document does not define, or whose fragments spread each other
   * in a cycle, or that writes a {@code @skip} or an {@code @include} whose {@code if} argument is missing, repeated or
   * not a Boolean. All are validation errors in the GraphQL specification, and checking them before any record is read
   * keeps them parsing errors rather than failures raised lazily while the result set is iterated.
   * <p>
   * Only the fragments reachable from {@code selectionSet} are walked: a fragment the operation never spreads is not
   * checked, although the specification rejects an unused fragment too. It can never be expanded, so it cannot fail.
   */
  public void validate(final SelectionSet selectionSet) {
    validate(selectionSet, new ArrayList<>(), new HashMap<>());
  }

  /**
   * @param path    the fragments being walked, outermost first
   * @param heights for every fragment already walked, the length of the longest chain of fragments it starts. Memoized
   *                so a fragment spread many times is walked once, while a chain split into segments walked separately
   *                is still measured whole
   *
   * @return the length of the longest chain of fragments below {@code selectionSet}
   */
  private int validate(final SelectionSet selectionSet, final List<String> path, final Map<String, Integer> heights) {
    if (selectionSet == null)
      return 0;

    int height = 0;
    for (final Selection selection : selectionSet.getSelections()) {
      final FragmentSpread spread = selection.getFragmentSpread();
      final InlineFragment inline = selection.getInlineFragment();
      // WHATEVER THE DIRECTIVES DECIDE, THE SELECTION IS STILL WALKED: VALIDATION DOES NOT DEPEND ON THE VARIABLES
      validateDirectives(directivesOf(selection));
      if (spread != null) {
        final String name = spread.getName();
        final FragmentDefinition fragment = getDefinition(name);
        if (path.contains(name))
          throw new CommandParsingException(
              "GraphQL fragment '" + name + "' spreads itself through a cycle: " + String.join(" -> ", path) + " -> " + name);

        Integer fragmentHeight = heights.get(name);
        if (fragmentHeight == null) {
          if (path.size() >= MAX_FRAGMENT_DEPTH)
            throw tooDeep(name);
          path.add(name);
          fragmentHeight = 1 + validate(fragment.getSelectionSet(), path, heights);
          path.removeLast();
          heights.put(name, fragmentHeight);
        }
        if (path.size() + fragmentHeight > MAX_FRAGMENT_DEPTH)
          throw tooDeep(name);
        height = Math.max(height, fragmentHeight);
      } else if (inline != null)
        height = Math.max(height, validate(inline.getSelectionSet(), path, heights));
      else
        height = Math.max(height, validate(selection.getSelectionSet(), path, heights));
    }
    return height;
  }

  private static CommandParsingException tooDeep(final String name) {
    return new CommandParsingException(
        "GraphQL fragments are nested more than " + MAX_FRAGMENT_DEPTH + " levels deep, through fragment '" + name + "'");
  }

  /**
   * Returns the selections with every fragment spread and inline fragment replaced, recursively, by the selections it
   * contributes, in document order. A fragment whose type condition does not apply is skipped entirely, and so is any
   * selection - field, spread or inline fragment - that {@code @skip} or {@code @include} excludes.
   *
   * @param selections     the selections as written in the document, may be null
   * @param typeConditions tells whether a fragment written {@code on T} applies to the object being resolved, given
   *                       the name {@code T}. It is only called for a fragment that carries a type condition
   */
  public List<Selection> expand(final List<Selection> selections, final Predicate<String> typeConditions) {
    return expand(selections, typeConditions, true);
  }

  /**
   * @param evaluateDirectives false to leave {@code @skip} and {@code @include} out of the expansion, which tells a
   *                           selection that selects nothing because of its directives from one whose fragments cannot
   *                           apply
   */
  List<Selection> expand(final List<Selection> selections, final Predicate<String> typeConditions,
      final boolean evaluateDirectives) {
    if (selections == null)
      return null;

    boolean rewritten = false;
    for (int i = 0; i < selections.size(); i++)
      if (isRewritten(selections.get(i), evaluateDirectives)) {
        rewritten = true;
        break;
      }
    if (!rewritten)
      // THE COMMON CASE: NO ALLOCATION
      return selections;

    final List<Selection> expanded = new ArrayList<>(selections.size() + 4);
    expand(selections, typeConditions, evaluateDirectives, expanded, new HashSet<>());
    return expanded;
  }

  private void expand(final List<Selection> selections, final Predicate<String> typeConditions,
      final boolean evaluateDirectives, final List<Selection> expanded, final Set<String> spread) {
    for (final Selection selection : selections) {
      if (evaluateDirectives && !isIncluded(directivesOf(selection)))
        // EVALUATED BEFORE A SPREAD IS RECORDED AS VISITED, AS THE SPECIFICATION'S CollectFields DOES: A SPREAD THE
        // DIRECTIVES EXCLUDE DOES NOT STOP ANOTHER SPREAD OF THE SAME FRAGMENT AT THIS LEVEL FROM CONTRIBUTING
        continue;

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
          expand(fragment.getSelectionSet().getSelections(), typeConditions, evaluateDirectives, expanded, spread);
      } else if (inline != null) {
        if (applies(inline.getTypeConditionName(), typeConditions) && inline.getSelectionSet() != null)
          expand(inline.getSelectionSet().getSelections(), typeConditions, evaluateDirectives, expanded, spread);
      } else
        expanded.add(selection);
    }
  }

  /**
   * Whether the {@code @skip} and {@code @include} among {@code directives} let the selection they are written on be
   * resolved: it is left out when a {@code @skip} condition is true or an {@code @include} condition is false.
   */
  private boolean isIncluded(final Directives directives) {
    if (directives == null)
      return true;

    for (final Directive directive : directives.getDirectives()) {
      final String name = directive.getName();
      if (SKIP.equals(name)) {
        if (condition(directive))
          return false;
      } else if (INCLUDE.equals(name) && !condition(directive))
        return false;
    }
    return true;
  }

  /**
   * Checks every {@code @skip} and {@code @include} among {@code directives}, without short-circuiting on the first one
   * that decides, so an invalid one is reported whatever the others say.
   */
  private void validateDirectives(final Directives directives) {
    if (directives == null)
      return;

    boolean skip = false;
    boolean include = false;
    for (final Directive directive : directives.getDirectives()) {
      final String name = directive.getName();
      if (SKIP.equals(name)) {
        if (skip)
          throw repeated(directive);
        skip = true;
      } else if (INCLUDE.equals(name)) {
        if (include)
          throw repeated(directive);
        include = true;
      } else
        continue;
      condition(directive);
    }
  }

  /**
   * The value of the {@code if: Boolean!} argument of a {@code @skip} or an {@code @include}, a literal or a variable of
   * the operation.
   */
  private boolean condition(final Directive directive) {
    Argument condition = null;
    final Arguments arguments = directive.getArguments();
    if (arguments != null)
      for (final Argument argument : arguments.getList()) {
        if (!IF.equals(argument.getName()))
          throw new CommandParsingException(
              "Directive @" + directive.getName() + " has no argument '" + argument.getName() + "', only 'if'");
        if (condition != null)
          throw new CommandParsingException("Directive @" + directive.getName() + " has the argument 'if' more than once");
        condition = argument;
      }
    if (condition == null)
      throw new CommandParsingException("Directive @" + directive.getName() + " requires the argument 'if'");

    final Object value = GraphQLSchema.resolveValue(condition.getValueWithVariable(), variables);
    if (value instanceof Boolean b)
      return b;
    throw new CommandParsingException(
        "The argument 'if' of directive @" + directive.getName() + " must be a Boolean, but it is " + (value == null ?
            "null" :
            "a " + value.getClass().getSimpleName() + " value"));
  }

  private static CommandParsingException repeated(final Directive directive) {
    return new CommandParsingException("Directive @" + directive.getName() + " is written more than once on the same selection");
  }

  /**
   * The directives written on a selection, whichever way it was written: on the field, on the spread or on the inline
   * fragment.
   */
  private static Directives directivesOf(final Selection selection) {
    final FragmentSpread spread = selection.getFragmentSpread();
    if (spread != null)
      return spread.getDirectives();
    final InlineFragment inline = selection.getInlineFragment();
    if (inline != null)
      return inline.getDirectives();
    final AbstractField field = selection.getAnyField();
    return field != null ? field.getDirectives() : null;
  }

  private static boolean isInclusionDirective(final Directive directive) {
    return SKIP.equals(directive.getName()) || INCLUDE.equals(directive.getName());
  }

  /**
   * Whether the expansion has anything to do for this selection: a fragment to replace, or a directive to evaluate.
   */
  private static boolean isRewritten(final Selection selection, final boolean evaluateDirectives) {
    if (isFragment(selection))
      return true;
    if (!evaluateDirectives)
      return false;
    final AbstractField field = selection.getAnyField();
    final Directives directives = field != null ? field.getDirectives() : null;
    if (directives != null)
      for (final Directive directive : directives.getDirectives())
        if (isInclusionDirective(directive))
          return true;
    return false;
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
