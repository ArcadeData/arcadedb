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
package com.arcadedb.census;

import org.junit.jupiter.api.Test;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9495: the {@code copy()} of {@code SelectStatement}, {@code CreateVertexStatement}, {@code MatchPathItem},
 * {@code FieldMatchPathItem} and {@code MathExpression} created the copy with {@code getClass().getConstructor().newInstance()}. A
 * GraalVM native image registers no constructor for reflection unless told to, so every UPDATE/DELETE (whose plan-cache key copies
 * a synthetic SELECT) failed there with {@code NoSuchMethodException: SelectStatement.<init>()}. The copies now come from an
 * overridable {@code newInstance()} factory, pinned by {@code Issue9495ReflectionFreeCopyTest}. A reflective self-instantiation
 * works on the JVM and fails only in the native image, so no JVM test can catch it by running it.
 * <p>
 * The census ranges over the authority (#11288): every {@code getConstructor}/{@code getDeclaredConstructor} of
 * {@code java.lang.Class} whose receiver is what {@code getClass()} answered, in the compiled classes of every module
 * ({@link CompiledClasses}, {@link CallOperands}): a class asking for the constructor of its own runtime type. The source-text
 * census it replaces matched the spelling {@code getClass().getConstructor(} in the SQL AST sources, so the class held in a local
 * first, or a class in another package, passed it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ReflectionFreeCopyCensusTest {
  private static final String      CLASS        = "java/lang/Class";
  private static final Set<String> CONSTRUCTORS = Set.of("getConstructor", "getDeclaredConstructor");

  /** Reflective self-instantiations the native image is told about, each with where. */
  private static final Map<String, String> REGISTERED = new TreeMap<>(Map.of());

  @Test
  void noClassInstantiatesItsOwnRuntimeTypeReflectively() throws Exception {
    final Map<String, Set<String>> offenders = offenders(CompiledClasses.ofEveryModule());

    assertThat(CompiledClasses.unreviewed(offenders, REGISTERED)).as("a getClass().getConstructor() works on the JVM and fails in "
        + "the native image, which registers no constructor for reflection: create the copy with a factory the subclass "
        + "overrides (issue #9495)").isEmpty();
    assertThat(CompiledClasses.stale(offenders, REGISTERED)).as("entries that no longer instantiate reflectively: remove them")
        .isEmpty();
  }

  /** The control: the runtime type asked for its constructor through a local, and not a class named by a literal. */
  @Test
  void theCensusSeesTheRuntimeTypeWhereverItIsHeld() throws Exception {
    assertThat(offenders(CompiledClasses.ofTheProbes()).keySet()).containsExactly("census/ShapeProbes$ReflectiveCopy#throughALocal");
  }

  private static Map<String, Set<String>> offenders(final Map<String, byte[]> classes) {
    final Map<String, Set<String>> offenders = new TreeMap<>();
    for (final CallOperands.Site site : CallOperands.of(classes,
        (owner, name, desc) -> CLASS.equals(owner) && CONSTRUCTORS.contains(name)))
      for (final AbstractInsnNode origin : site.operand(0))
        if (origin instanceof MethodInsnNode call && "getClass".equals(call.name) && "()Ljava/lang/Class;".equals(call.desc))
          offenders.computeIfAbsent(site.key(), k -> new TreeSet<>()).add(site.call().name);
    return offenders;
  }
}
