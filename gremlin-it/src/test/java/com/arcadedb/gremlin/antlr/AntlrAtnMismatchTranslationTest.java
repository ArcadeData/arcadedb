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
package com.arcadedb.gremlin.antlr;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.gremlin.ArcadeGremlin;
import org.junit.jupiter.api.Test;

import java.io.InvalidClassException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #5937: the plain (unshaded) {@code arcadedb-gremlin} Maven coordinate puts TinkerPop's
 * precompiled, ANTLR-4.9.1-generated grammar classes ({@code GremlinLexer}/{@code GremlinQueryParser}, "v3 ATN") on
 * the same classpath as the engine's own ANTLR 4.13.2 runtime ("v4 ATN"). The first textual Gremlin query then
 * fails with a raw {@code ExceptionInInitializerError} wrapping {@code UnsupportedOperationException} wrapping
 * {@code java.io.InvalidClassException: ... ATN; Could not deserialize ATN with version 3 (expected 4)} - a
 * confusing crash deep in ANTLR internals that gives no hint that the fix is to depend on the
 * {@code arcadedb-gremlin:shaded} classifier instead.
 * <p>
 * {@code ArcadeGremlin.translateAntlrAtnMismatch(LinkageError)} (private, reached via reflection here) recognizes
 * that specific cause-chain shape and turns it into an actionable {@link CommandExecutionException}. This test
 * exercises the translation directly with a synthetic exception chain of that exact shape, rather than by
 * reproducing the real classpath conflict: {@code AntlrCoexistenceIT} in this same package already proves the
 * shaded classpath this module tests against does NOT hit the conflict, and the module-wide {@code gremlin/pom.xml}
 * {@code skipTests} (see its Surefire config comment) means the plain/unshaded classpath that WOULD hit it never
 * runs any test here either. The translation logic itself needs no real ANTLR grammar class to be exercised, so a
 * synthetic chain is both sufficient and the only practical way to pin it in this module layout.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AntlrAtnMismatchTranslationTest {

  @Test
  void atnVersionMismatchIsTranslatedToAnActionableMessage() throws Exception {
    final LinkageError error = atnMismatchError();

    final CommandExecutionException translated = (CommandExecutionException) invokeTranslate(error);

    assertThat(translated.getCause()).isSameAs(error);
    assertThat(translated.getMessage())
        .contains("arcadedb-gremlin:shaded")
        .contains("ATN")
        .contains("#5937");
  }

  @Test
  void anUnrelatedLinkageErrorIsRethrownUnchanged() {
    final LinkageError unrelated = new NoClassDefFoundError("some/other/Class");

    assertThatThrownBy(() -> invokeTranslate(unrelated))
        .isInstanceOf(InvocationTargetException.class)
        .extracting(Throwable::getCause)
        .isSameAs(unrelated);
  }

  /** {@code ExceptionInInitializerError -> UnsupportedOperationException -> InvalidClassException}, matching #5937. */
  private static LinkageError atnMismatchError() {
    final InvalidClassException invalidClass =
        new InvalidClassException("org.antlr.v4.runtime.atn.ATN", "Could not deserialize ATN with version 3 (expected 4)");
    final UnsupportedOperationException unsupported = new UnsupportedOperationException(invalidClass);
    final ExceptionInInitializerError error = new ExceptionInInitializerError(unsupported);
    return error;
  }

  private static Object invokeTranslate(final LinkageError error) throws Exception {
    final Method method = ArcadeGremlin.class.getDeclaredMethod("translateAntlrAtnMismatch", LinkageError.class);
    method.setAccessible(true);
    return method.invoke(null, error);
  }
}
