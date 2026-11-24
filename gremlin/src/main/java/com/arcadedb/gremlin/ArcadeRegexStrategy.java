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
package com.arcadedb.gremlin;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import org.apache.tinkerpop.gremlin.process.traversal.NotP;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.PBiPredicate;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Text;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.ProfileStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.process.traversal.util.OrP;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Makes the regular expressions of a Gremlin traversal ({@code TextP.regex()} and {@code TextP.notRegex()}) honour
 * {@code arcadedb.command.regexTimeout}, as the ones of the other query languages do: each predicate TinkerPop builds is replaced by
 * an {@link ArcadeRegexPredicate}, which searches through {@link com.arcadedb.utility.TimeBoundRegex} instead of running
 * {@code Pattern.matcher(value).find()} on the plain string.
 * <p>
 * The time is one budget for the whole statement, root traversal and children together, and counts only what is spent inside the
 * expressions ({@link ArcadeRegexPredicate.Budget}).
 * <p>
 * A strategy is the one place every entry point goes through: a statement of the {@code gremlin} query engine, the fluent API, the
 * string, script and bytecode requests of the Gremlin Server. It is pinned in {@link ArcadeTraversalStrategies}, so no request can
 * drop it with {@code withoutStrategies()}. A finalization strategy: it runs once the optimizations have settled the steps, so a
 * predicate they moved, merged or inlined is found where it ended up, and before {@code ProfileStrategy}, so the steps a profiled
 * traversal times are the final ones.
 * <p>
 * Where the predicates are: every step field of type {@link P} (a {@code is()}, a {@code where(P)}, an {@code any()}, {@code all()}
 * or {@code none()}, and whatever step a later TinkerPop adds with one) and the containers of every {@link HasContainerHolder} (a
 * {@code has()}), read from the declared fields of the step class since TinkerPop gives no accessor for some of them. The predicates a
 * caller passes in are never modified: a replacement is a new object, set in the field of the step of this traversal, so a predicate
 * a caller keeps and uses again starts each statement with a full budget. Through {@link ConnectiveP} ({@code and()}/{@code or()})
 * and {@link NotP} the replacement is rebuilt around the same structure.
 * <p>
 * Whatever steps the traversal has beyond those, {@code GremlinPredicateHoldersTest} fails when a TinkerPop upgrade brings a step
 * that holds a predicate in a shape this class does not read.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
public final class ArcadeRegexStrategy extends AbstractTraversalStrategy<TraversalStrategy.FinalizationStrategy>
    implements TraversalStrategy.FinalizationStrategy {

  private static final ArcadeRegexStrategy                                          INSTANCE = new ArcadeRegexStrategy();
  private static final Set<Class<? extends TraversalStrategy.FinalizationStrategy>> POSTS    = Set.of(ProfileStrategy.class);

  /** The fields of a class (and its super classes) that hold a predicate, made accessible once. */
  private static final ClassValue<Field[]> PREDICATE_FIELDS = new ClassValue<>() {
    @Override
    protected Field[] computeValue(final Class<?> type) {
      final List<Field> fields = new ArrayList<>();
      for (Class<?> c = type; c != null && c != Object.class; c = c.getSuperclass())
        for (final Field field : c.getDeclaredFields())
          if (!Modifier.isStatic(field.getModifiers()) && P.class.isAssignableFrom(field.getType())) {
            field.setAccessible(true);
            fields.add(field);
          }
      return fields.toArray(new Field[0]);
    }
  };

  private ArcadeRegexStrategy() {
  }

  public static ArcadeRegexStrategy instance() {
    return INSTANCE;
  }

  @Override
  public void apply(final Traversal.Admin<?, ?> traversal) {
    // THE ROOT DOES THE WHOLE TREE, SO THAT ONE BUDGET SERVES THE STATEMENT: THE CHILDREN ARE OFFERED TO THIS STRATEGY TOO, AFTER IT
    if (!traversal.isRoot())
      return;

    // A DATABASE THAT IS NOT A LOCAL ONE (A RemoteDatabase) HAS NO SETTINGS OF ITS OWN: THE DEFAULT APPLIES
    final Database database = traversal.getGraph().orElse(null) instanceof ArcadeGraph graph && graph.getDatabase() instanceof Database local ?
        local :
        null;
    final Rewriter rewriter = new Rewriter(database);
    TraversalHelper.applyTraversalRecursively(rewriter::rewrite, traversal);
  }

  @Override
  public Set<Class<? extends TraversalStrategy.FinalizationStrategy>> applyPost() {
    return POSTS;
  }

  /** The replacement of the regular expression predicates of one statement. */
  private static final class Rewriter {
    private final Database                      database;
    private       ArcadeRegexPredicate.Budget budget;

    private Rewriter(final Database database) {
      this.database = database;
    }

    private void rewrite(final Traversal.Admin<?, ?> traversal) {
      for (final Step<?, ?> step : traversal.getSteps()) {
        replaceIn(step);
        if (step instanceof HasContainerHolder<?, ?> holder)
          for (final HasContainer container : holder.getHasContainers())
            replaceIn(container);
      }
    }

    private void replaceIn(final Object holder) {
      for (final Field field : PREDICATE_FIELDS.get(holder.getClass()))
        try {
          final P<?> predicate = (P<?>) field.get(holder);
          final P<?> replacement = predicate != null ? replace(predicate) : null;
          if (replacement != null)
            field.set(holder, replacement);
        } catch (final IllegalAccessException e) {
          throw new IllegalStateException("Cannot read the predicate of " + holder.getClass().getSimpleName(), e);
        }
    }

    /** The predicate with its regular expressions replaced, or null when it holds none that is not already bounded. */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private P<?> replace(final P<?> predicate) {
      if (predicate instanceof ConnectiveP connective) {
        final List<P> original = connective.getPredicates();
        List<P> replaced = null;
        for (int i = 0; i < original.size(); i++) {
          final P<?> replacement = replace(original.get(i));
          if (replacement != null) {
            if (replaced == null)
              replaced = new ArrayList<>(original);
            replaced.set(i, replacement);
          }
        }
        if (replaced == null)
          return null;
        return connective instanceof AndP ? new AndP(replaced) : new OrP(replaced);
      }

      if (predicate instanceof NotP not) {
        // negate() OF A NotP IS THE PREDICATE IT NEGATES
        final P<?> inner = replace(not.negate());
        return inner == null ? null : new NotP(inner);
      }

      final PBiPredicate<?, ?> replacement = replace(predicate.getBiPredicate());
      if (replacement == null)
        return null;
      return predicate instanceof TextP ?
          new TextP((PBiPredicate<String, String>) replacement, (String) predicate.getValue()) :
          new P((PBiPredicate) replacement, predicate.getValue());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private PBiPredicate<?, ?> replace(final PBiPredicate<?, ?> predicate) {
      if (predicate instanceof ArcadeRegexPredicate)
        return null;
      if (predicate instanceof Text.RegexPredicate regex)
        return new ArcadeRegexPredicate(regex.getPattern(), regex.isNegate(), budget());
      if (predicate instanceof NotP.NotPBiPredicate not) {
        final PBiPredicate<?, ?> inner = replace((PBiPredicate<?, ?>) not.getOriginal());
        return inner == null ? null : new NotP.NotPBiPredicate(inner);
      }
      return null;
    }

    private ArcadeRegexPredicate.Budget budget() {
      if (budget == null)
        budget = ArcadeRegexPredicate.Budget.ofMillis(GlobalConfiguration.COMMAND_REGEX_TIMEOUT.getValueAsLong(database));
      return budget;
    }
  }
}
