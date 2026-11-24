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

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.PBiPredicate;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.PredicateTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.PredicateTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.AllStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.AnyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.IsStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.LambdaFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NoneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WherePredicateStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Enumeration;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A census of the places a TinkerPop class keeps a predicate, which {@link ArcadeRegexStrategy} has to read to bound the regular
 * expressions of a traversal. It reads them from the authority: the compiled classes of the step and lambda packages of the TinkerPop
 * library the module runs with, so a class an upgrade adds fails here and not silently, with its regular expressions unbounded.
 * <p>
 * The strategy reads these shapes: a non-final field of type {@link P} (found by its type, whichever class declares it), the
 * containers of a {@link HasContainerHolder}, and the pick of a {@code choose().option()}. A class that keeps predicates in any other
 * shape (a collection or an array of them, a bare container, a field of a functional type a predicate can be put in) is the next
 * offender, unless it is listed in {@link #PREDICATE_TYPED} with why it needs nothing.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class GremlinPredicateHoldersTest {
  private static final String STEPS  = "org/apache/tinkerpop/gremlin/process/traversal/step/";
  private static final String LAMBDA = "org/apache/tinkerpop/gremlin/process/traversal/lambda/";

  /** The classes with a field of a functional type a predicate can be put in, and why each is read, or needs no reading. */
  private static final Map<Class<?>, String> PREDICATE_TYPED = new TreeMap<>((a, b) -> a.getName().compareTo(b.getName()));

  static {
    PREDICATE_TYPED.put(PredicateTraversal.class, "the pick of an option(): replaced through BranchStep.getTraversalOptions()");
    PREDICATE_TYPED.put(PredicateTraverser.class, "wraps a lambda: choose(P, ...) is a plain is() step, whose P is read");
    PREDICATE_TYPED.put(LambdaFilterStep.class, "a lambda, which receives the traverser and not its value: no pattern is ever searched");
  }

  @Test
  void everyClassKeepsItsPredicatesInAShapeTheStrategyReads() throws Exception {
    final File library = new File(Step.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    assertThat(library).as("the TinkerPop library is a jar").isFile();

    int scanned = 0;
    final Set<Class<?>> direct = new TreeSetOfClasses();
    final Set<Class<?>> finalFields = new TreeSetOfClasses();
    final Set<Class<?>> typed = new TreeSetOfClasses();
    final Set<Class<?>> unread = new TreeSetOfClasses();
    try (JarFile jar = new JarFile(library)) {
      for (final Enumeration<JarEntry> entries = jar.entries(); entries.hasMoreElements(); ) {
        final String name = entries.nextElement().getName();
        if (!(name.startsWith(STEPS) || name.startsWith(LAMBDA)) || !name.endsWith(".class"))
          continue;

        final Class<?> type = Class.forName(name.substring(0, name.length() - ".class".length()).replace('/', '.'), false,
            Step.class.getClassLoader());
        ++scanned;
        for (final Field field : type.getDeclaredFields()) {
          if (Modifier.isStatic(field.getModifiers()))
            continue;
          if (P.class.isAssignableFrom(field.getType())) {
            direct.add(type);
            if (Modifier.isFinal(field.getModifiers()))
              finalFields.add(type);
          } else if (isPredicateType(field.getType()))
            typed.add(type);
          else if (holdsPredicates(field.getGenericType()) && !HasContainerHolder.class.isAssignableFrom(type) && type != HasContainer.class)
            unread.add(type);
        }
      }
    }

    // THE SCAN STILL REACHES THE CLASSES THE STRATEGY IS WRITTEN FOR: IF ONE IS MISSING, THE CENSUS IS LOOKING AT THE WRONG LIBRARY
    assertThat(scanned).as("classes read from the step and lambda packages of the TinkerPop library %s", library).isGreaterThan(250);
    assertThat(direct).as("classes with a field of type P, read by the strategy").contains(IsStep.class, WherePredicateStep.class,
        AnyStep.class, AllStep.class, NoneStep.class, HasContainer.class);
    assertThat(HasContainerHolder.class).isAssignableFrom(HasStep.class);
    assertThat(typed).as("classes with a predicate-typed field, listed in PREDICATE_TYPED").contains(PredicateTraversal.class);

    assertThat(finalFields).as("a final field of type P cannot be replaced by ArcadeRegexStrategy").isEmpty();
    assertThat(unread).as("classes that keep predicates in a shape ArcadeRegexStrategy does not read").isEmpty();
    assertThat(typed).as("classes with a predicate-typed field that ArcadeRegexStrategy neither reads nor excuses")
        .containsExactlyInAnyOrderElementsOf(PREDICATE_TYPED.keySet());
  }

  /** The functional types a P can be put in: the field of one can hold a regular expression. */
  private static boolean isPredicateType(final Class<?> type) {
    return type == Predicate.class || type == BiPredicate.class || type == PBiPredicate.class;
  }

  /** Whether the type is, or is a collection or an array of, a predicate or a container of one. */
  private static boolean holdsPredicates(final Type type) {
    if (type instanceof Class<?> c)
      return P.class.isAssignableFrom(c) || HasContainer.class.isAssignableFrom(c) || c.isArray() && holdsPredicates(c.getComponentType());
    if (type instanceof ParameterizedType parameterized) {
      if (holdsPredicates(parameterized.getRawType()))
        return true;
      for (final Type argument : parameterized.getActualTypeArguments())
        if (holdsPredicates(argument))
          return true;
      return false;
    }
    return type instanceof GenericArrayType array && holdsPredicates(array.getGenericComponentType());
  }

  /** A set of classes ordered by name, so that a failure lists them the same way on every run. */
  private static final class TreeSetOfClasses extends TreeSet<Class<?>> {
    private TreeSetOfClasses() {
      super((a, b) -> a.getName().compareTo(b.getName()));
    }
  }
}
