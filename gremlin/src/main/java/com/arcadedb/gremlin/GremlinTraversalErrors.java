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
import com.arcadedb.exception.TypeMismatchException;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.VerificationException;

import java.util.regex.Pattern;

/**
 * Tells a traversal TinkerPop refuses from a fault of the server, at the boundary where a Gremlin statement is built and read
 * (issue #11886). A traversal can be wrong in ways no parser sees: a step that cannot follow the previous one ({@code to()} after
 * {@code V()}), a {@code repeat()} with no traversal, a step that meets a value of a type it is not defined for ({@code sum()} of
 * vertices, {@code inV()} of a vertex), a step that needs what the path does not hold ({@code otherV()} with no edge before it).
 * TinkerPop answers each with the exception of the step, raised while the traversal is built, verified or iterated, and every
 * protocol answered those as a server fault (HTTP 500), where the syntax errors of the same request are client errors (#5191,
 * #5201).
 * <p>
 * The verdict is drawn from the ORIGIN of the failure, the root of its cause chain:
 * <ul>
 *   <li>a {@link VerificationException} is TinkerPop's own refusal of the traversal, whatever raised it;</li>
 *   <li>a {@link ClassCastException}, {@link IllegalStateException}, {@link IllegalArgumentException},
 *   {@link UnsupportedOperationException} or {@link NullPointerException} is the traversal's fault only when TinkerPop's code
 *   raised it, read from the first frame of its stack that is not the JDK's (a {@code checkcast} in a step, a
 *   {@code String.compareTo} the step called). The same exception raised by ArcadeDB's code under a step is a fault of the server
 *   and keeps its classification, which is also why the root decides and not some exception along the chain.</li>
 * </ul>
 * A wrong value type becomes a {@link TypeMismatchException}, as the other query languages report one, and a traversal of the wrong
 * shape a {@link CommandParsingException}, the failed semantic validation that category stands for. Both keep TinkerPop's message,
 * which names the step and the value, and both are client errors on every protocol ({@code ErrorCategory}, the HTTP ladder).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class GremlinTraversalErrors {
  static final String PREFIX = "Invalid gremlin traversal: ";

  private static final String  TINKERPOP_PACKAGE = "org.apache.tinkerpop.gremlin.";
  // THE MODULE AND CLASS LOADER A ClassCastException APPENDS: "(x is in unnamed module of loader 'app'; y is in module java.base ...)"
  private static final Pattern CAST_LOADER_DETAIL = Pattern.compile("\\s*\\([^()]*\\b(?:module|loader)\\b[^()]*\\)\\s*$");

  private GremlinTraversalErrors() {
  }

  /**
   * The client error {@code error} stands for when its origin is a traversal TinkerPop refuses, otherwise null.
   */
  public static RuntimeException asClientError(final Throwable error) {
    final Throwable root = rootCause(error);
    if (root instanceof VerificationException)
      return new CommandParsingException(PREFIX + root.getMessage(), error);

    if (!(root instanceof ClassCastException || root instanceof IllegalStateException || root instanceof IllegalArgumentException
        || root instanceof UnsupportedOperationException || root instanceof NullPointerException) || !raisedByTinkerPop(root))
      return null;

    final String message = root.getMessage() != null ? root.getMessage() : root.getClass().getSimpleName();
    if (root instanceof ClassCastException)
      return new TypeMismatchException(PREFIX + CAST_LOADER_DETAIL.matcher(message).replaceFirst(""), error);
    if (root instanceof NullPointerException)
      // THE HELPFUL NPE TEXT NAMES A JDK FIELD ("anotherString"), NOT WHAT THE CALLER WROTE: SAY WHAT IT MEANS FIRST
      return new CommandParsingException(PREFIX + "a step met a null it does not accept (" + message + ")", error);
    return new CommandParsingException(PREFIX + message, error);
  }

  /** {@code error} as the client error it stands for, or unchanged when it is none. */
  public static RuntimeException translate(final RuntimeException error) {
    final RuntimeException clientError = asClientError(error);
    return clientError != null ? clientError : error;
  }

  /** Whether the first frame of the stack that is not the JDK's belongs to TinkerPop. */
  static boolean raisedByTinkerPop(final Throwable error) {
    for (final StackTraceElement frame : error.getStackTrace()) {
      final String className = frame.getClassName();
      if (className.startsWith("java.") || className.startsWith("jdk.") || className.startsWith("sun."))
        continue;
      return className.startsWith(TINKERPOP_PACKAGE);
    }
    return false;
  }

  private static Throwable rootCause(final Throwable error) {
    Throwable root = error;
    // BOUNDED: A CAUSE CHAIN CAN LOOP BACK ON ITSELF
    for (int depth = 0; depth < 32 && root.getCause() != null && root.getCause() != root; depth++)
      root = root.getCause();
    return root;
  }
}
