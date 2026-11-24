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

import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.exception.TypeMismatchException;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12526: the verdict of {@link GremlinTraversalErrors} on a {@link ClassCastException} or a {@link NullPointerException} is
 * drawn from the first non-JDK frame of its stack trace (issue #11886), but HotSpot's {@code OmitStackTraceInFastThrow} (on by
 * default) replaces the trace of an implicit exception thrown a few thousand times from the same site with a preallocated instance
 * that has an EMPTY trace and no message. A traversal type error answered as a client error (HTTP 400) turned into a server fault
 * (HTTP 500) under sustained traffic, per throw site and until the JVM restarts.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12526GremlinFastThrowClassificationTest {
  private static final StackTraceElement[] NO_FRAMES = new StackTraceElement[0];

  private static volatile Object sink;

  @Test
  void anImplicitClassCastExceptionWithNoFramesIsATypeMismatch() {
    final ClassCastException error = new ClassCastException();
    error.setStackTrace(NO_FRAMES);

    final RuntimeException clientError = GremlinTraversalErrors.asClientError(error);

    assertThat(clientError).isInstanceOf(TypeMismatchException.class);
    assertThat(ErrorCategory.of(clientError)).isIn(ErrorCategory.PARSING, ErrorCategory.VALIDATION);
    assertThat(clientError.getMessage()).startsWith(GremlinTraversalErrors.PREFIX).contains("type it is not defined for")
        .doesNotContain("ClassCastException");
    assertThat(clientError.getCause()).isSameAs(error);
  }

  @Test
  void anImplicitNullPointerExceptionWithNoFramesIsAParsingError() {
    final NullPointerException error = new NullPointerException();
    error.setStackTrace(NO_FRAMES);

    final RuntimeException clientError = GremlinTraversalErrors.asClientError(error);

    assertThat(clientError).isInstanceOf(CommandParsingException.class);
    assertThat(ErrorCategory.of(clientError)).isIn(ErrorCategory.PARSING, ErrorCategory.VALIDATION);
    assertThat(clientError.getMessage()).startsWith(GremlinTraversalErrors.PREFIX).contains("a step met a null it does not accept")
        .doesNotContain("NullPointerException").doesNotContain("()");
    assertThat(clientError.getCause()).isSameAs(error);
  }

  @Test
  void aMessageOfAnExceptionWithNoFramesIsKept() {
    final ClassCastException cast = new ClassCastException("class java.lang.Integer cannot be cast to class java.lang.String");
    cast.setStackTrace(NO_FRAMES);
    assertThat(GremlinTraversalErrors.asClientError(cast)).isInstanceOf(TypeMismatchException.class)
        .hasMessageContaining("java.lang.Integer cannot be cast to class java.lang.String");

    final NullPointerException nullPointer = new NullPointerException("Cannot invoke \"String.length()\" because \"s\" is null");
    nullPointer.setStackTrace(NO_FRAMES);
    assertThat(GremlinTraversalErrors.asClientError(nullPointer)).isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("Cannot invoke \"String.length()\" because \"s\" is null");
  }

  @Test
  void theRootOfTheCauseChainDecidesWhenItHasNoFrames() {
    final ClassCastException root = new ClassCastException();
    root.setStackTrace(NO_FRAMES);
    final RuntimeException wrapper = new RuntimeException("wrapper", new IllegalStateException("middle", root));

    final RuntimeException clientError = GremlinTraversalErrors.asClientError(wrapper);

    assertThat(clientError).isInstanceOf(TypeMismatchException.class);
    assertThat(clientError.getCause()).isSameAs(wrapper);
  }

  @Test
  void translateAnswersAClientErrorForAnExceptionWithNoFrames() {
    final NullPointerException error = new NullPointerException();
    error.setStackTrace(NO_FRAMES);

    assertThat(GremlinTraversalErrors.translate(error)).isInstanceOf(CommandParsingException.class);
  }

  @Test
  void aClassCastExceptionRaisedByTinkerPopKeepsBeingATypeMismatch() {
    final ClassCastException error = new ClassCastException("class A cannot be cast to class B");
    error.setStackTrace(frames("java.lang.Integer", "org.apache.tinkerpop.gremlin.process.traversal.step.map.SumGlobalStep"));

    assertThat(GremlinTraversalErrors.asClientError(error)).isInstanceOf(TypeMismatchException.class);
  }

  @ParameterizedTest
  @ValueSource(strings = { "com.arcadedb.engine.PageManager", "com.arcadedb.gremlin.ArcadeGraph" })
  void aClassCastExceptionRaisedByArcadeDbCodeIsStillAServerFault(final String className) {
    final ClassCastException error = new ClassCastException("class A cannot be cast to class B");
    error.setStackTrace(frames("java.util.HashMap", className, "org.apache.tinkerpop.gremlin.process.traversal.step.map.SumGlobalStep"));

    assertThat(GremlinTraversalErrors.asClientError(error)).isNull();
  }

  @ParameterizedTest
  @ValueSource(strings = { "com.arcadedb.engine.PageManager", "com.arcadedb.gremlin.ArcadeGraph" })
  void aNullPointerExceptionRaisedByArcadeDbCodeIsStillAServerFault(final String className) {
    final NullPointerException error = new NullPointerException("Cannot invoke \"x\" because \"y\" is null");
    error.setStackTrace(frames("java.util.Objects", className, "org.apache.tinkerpop.gremlin.process.traversal.step.map.SumGlobalStep"));

    assertThat(GremlinTraversalErrors.asClientError(error)).isNull();
  }

  /**
   * Only the two implicit exceptions HotSpot strips of their trace are read this way: the others are raised with {@code new}, which
   * always fills the trace in, so an empty one means the code that raised them is not known.
   */
  @Test
  void anExplicitExceptionWithNoFramesIsUnchanged() {
    final IllegalStateException illegalState = new IllegalStateException("state");
    illegalState.setStackTrace(NO_FRAMES);
    final IllegalArgumentException illegalArgument = new IllegalArgumentException("argument");
    illegalArgument.setStackTrace(NO_FRAMES);
    final UnsupportedOperationException unsupported = new UnsupportedOperationException("unsupported");
    unsupported.setStackTrace(NO_FRAMES);

    assertThat(GremlinTraversalErrors.asClientError(illegalState)).isNull();
    assertThat(GremlinTraversalErrors.asClientError(illegalArgument)).isNull();
    assertThat(GremlinTraversalErrors.asClientError(unsupported)).isNull();
  }

  @Test
  void anUnrelatedExceptionWithNoFramesIsUnchanged() {
    final ArrayIndexOutOfBoundsException error = new ArrayIndexOutOfBoundsException();
    error.setStackTrace(NO_FRAMES);

    assertThat(GremlinTraversalErrors.asClientError(error)).isNull();
    assertThat(GremlinTraversalErrors.translate(error)).isSameAs(error);
  }

  /**
   * The premise, against the real JVM: the same implicit {@link ClassCastException} thrown from one site over and over loses its
   * trace, and what the classifier answers for it must not depend on that. Skipped when the JVM does not do it (a JVM started with
   * {@code -XX:-OmitStackTraceInFastThrow}, or one that did not compile the site within the budget), never passed vacuously.
   */
  @Test
  void aRealImplicitClassCastExceptionIsTheSameClientErrorBeforeAndAfterTheJvmOmitsItsTrace() {
    final Object[] values = { Integer.valueOf(1), "text" };
    final ClassCastException first = castToString(values[0]);
    assertThat(first).isNotNull();
    assertThat(first.getStackTrace()).isNotEmpty();
    // raised by this test, which is no TinkerPop code: a server fault as long as it carries its trace
    assertThat(GremlinTraversalErrors.asClientError(first)).isNull();

    ClassCastException omitted = null;
    for (int i = 0; i < 400_000 && omitted == null; i++) {
      final ClassCastException error = castToString(values[0]);
      if (error != null && error.getStackTrace().length == 0)
        omitted = error;
    }
    Assumptions.assumeTrue(omitted != null, "this JVM did not omit the stack trace of a repeated ClassCastException");

    final RuntimeException clientError = GremlinTraversalErrors.asClientError(omitted);
    assertThat(clientError).isInstanceOf(TypeMismatchException.class);
    assertThat(clientError.getMessage()).startsWith(GremlinTraversalErrors.PREFIX).doesNotContain("ClassCastException");
  }

  private static ClassCastException castToString(final Object value) {
    try {
      sink = (String) value;
      return null;
    } catch (final ClassCastException e) {
      return e;
    }
  }

  private static StackTraceElement[] frames(final String... classNames) {
    final StackTraceElement[] frames = new StackTraceElement[classNames.length];
    for (int i = 0; i < classNames.length; i++)
      frames[i] = new StackTraceElement(classNames[i], "method", classNames[i].substring(classNames[i].lastIndexOf('.') + 1) + ".java", i + 1);
    return frames;
  }
}
