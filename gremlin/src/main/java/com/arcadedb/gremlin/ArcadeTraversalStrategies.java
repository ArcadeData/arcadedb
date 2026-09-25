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

import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;

import java.util.ArrayList;
import java.util.List;

/**
 * The strategy set every {@link ArcadeGraph} traversal source starts from. It differs from TinkerPop's default only in
 * refusing to drop {@link ArcadeIoRegistrationStrategy}, the strategy that enforces the permission to use the
 * {@code io()} step.
 * <p>
 * {@code withoutStrategies(...)} is a source instruction the CALLER controls: it is part of a bytecode request, of a
 * gremlin-lang string and of the fluent API alike, and every one of those routes ends in
 * {@link #removeStrategies(Class[])} on a clone of this set ({@code TraversalSource#withoutStrategies} clones the
 * source's strategies and removes from the clone; {@code clone()} keeps this class). Refusing the removal here is
 * therefore the one place that holds for every entry point, instead of a check per front end.
 * <p>
 * Naming the pinned strategy is not an error, it is simply not honoured - the same outcome as naming a strategy that
 * is not registered at all.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class ArcadeTraversalStrategies extends DefaultTraversalStrategies {

  public ArcadeTraversalStrategies(final TraversalStrategies base) {
    for (final TraversalStrategy<?> strategy : base)
      addStrategies(strategy);
  }

  @Override
  @SafeVarargs
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public final TraversalStrategies removeStrategies(final Class<? extends TraversalStrategy>... strategyClasses) {
    final List<Class<? extends TraversalStrategy>> removable = new ArrayList<>(strategyClasses.length);
    for (final Class<? extends TraversalStrategy> strategyClass : strategyClasses)
      if (!isPinned(strategyClass))
        removable.add(strategyClass);
    return super.removeStrategies(removable.toArray(new Class[0]));
  }

  /**
   * Whether the strategy cannot be removed from a traversal source. Matched by assignability, not equality: the base
   * implementation removes by exact class, so a subclass of a pinned strategy could never remove the pinned one, but
   * refusing it too keeps the rule obvious.
   */
  public static boolean isPinned(final Class<? extends TraversalStrategy> strategyClass) {
    return strategyClass != null && ArcadeIoRegistrationStrategy.class.isAssignableFrom(strategyClass);
  }
}
