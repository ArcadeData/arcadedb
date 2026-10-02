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
package com.arcadedb.utility;

import org.mockito.Answers;
import org.mockito.MockMakers;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

/**
 * Mocks built by Mockito's SUBCLASS mock maker instead of the default inline one (issues #8021, #8851).
 * <p>
 * The inline mock maker does not make a new class: it retransforms the mocked class in place, so a mock is an
 * instance of the real {@code LocalDatabase} whose methods now carry a dispatch to the mock handler. Code the JIT
 * compiled BEFORE that retransformation, with the original method inlined, is supposed to be thrown away - and on the
 * Graal JIT (a GraalVM JDK's default compiler) it is not. A test class that runs after a real server warmed up
 * {@code RaftReplicatedDatabase.getName()} in the same forked JVM therefore reads the mock's {@code name} FIELD,
 * which is null, instead of the stubbed answer, while the same call made from the test itself answers correctly.
 * HotSpot's C2 deoptimizes as it should, which is why the CI lanes (Temurin) never see it.
 * <p>
 * A subclass mock does not depend on that deoptimization: it is an instance of a newly loaded subclass that overrides
 * every method, and loading a subclass invalidates any compiled code that assumed the parent's method was the only
 * implementation, on every JIT. Use these for the classes a test hands to production code it then drives, where
 * that production code is warm in a shared fork - the inline maker remains the right choice for final classes and
 * static mocking, which a subclass cannot express.
 * <p>
 * A test class can route every bare {@code mock(...)} call through this class by statically importing
 * {@code SubclassMocks.mock} in place of {@code Mockito.mock}. {@code InlineMocksOfJitWarmTypesTest} (server module)
 * holds the rule for the engine types the HA and HTTP layers delegate to.
 */
public final class SubclassMocks {
  private SubclassMocks() {
  }

  public static <T> T mock(final Class<T> type) {
    return Mockito.mock(type, Mockito.withSettings().mockMaker(MockMakers.SUBCLASS));
  }

  public static <T> T mock(final Class<T> type, final Answer<?> defaultAnswer) {
    return Mockito.mock(type, Mockito.withSettings().mockMaker(MockMakers.SUBCLASS).defaultAnswer(defaultAnswer));
  }

  @SuppressWarnings("unchecked")
  public static <T> T spy(final T instance) {
    return Mockito.mock((Class<T>) instance.getClass(), Mockito.withSettings().mockMaker(MockMakers.SUBCLASS)
        .spiedInstance(instance).defaultAnswer(Answers.CALLS_REAL_METHODS));
  }
}
