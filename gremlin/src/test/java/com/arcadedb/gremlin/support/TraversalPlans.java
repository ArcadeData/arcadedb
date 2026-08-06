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
package com.arcadedb.gremlin.support;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Inspects the step list of a traversal AFTER strategy application, so a test can assert which steps
 * the optimizer actually installed.
 * <p>
 * A traversal's strategies are applied exactly once and the traversal is locked afterwards, so an
 * instance passed here must not also be executed. Build a separate traversal for execution.
 */
public class TraversalPlans {

  private TraversalPlans() {
  }

  public static List<Step> stepsOf(final Traversal<?, ?> traversal) {
    final Traversal.Admin<?, ?> admin = traversal.asAdmin();
    if (!admin.isLocked())
      admin.applyStrategies();
    return (List<Step>) (List<?>) admin.getSteps();
  }

  public static boolean hasStepOfType(final Traversal<?, ?> traversal, final Class<? extends Step> type) {
    for (final Step step : stepsOf(traversal))
      if (type.isInstance(step))
        return true;
    return false;
  }

  public static String describe(final Traversal<?, ?> traversal) {
    return stepsOf(traversal).stream()
        .map(s -> s.getClass().getSimpleName())
        .collect(Collectors.joining(" -> "));
  }
}
