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

import java.util.Objects;
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
 *   and keeps its classification, which is also why the root decides and not some exception along the chain. A
 *   {@link ClassCastException} or {@link NullPointerException} with no frames at all is the one the JVM stripped of its trace after
 *   a few thousand throws from the same site ({@code OmitStackTraceInFastThrow}, issue #12526): it is read as the traversal's, so
 *   that the verdict on a request does not change with the traffic that came before it.</li>
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
  // THE PREALLOCATED EXCEPTION OF HOTSPOT'S OmitStackTraceInFastThrow HAS NO MESSAGE: SAY WHAT A TRAVERSAL CAN MAKE OF IT (#12526)
  private static final String  OMITTED_CAST_TEXT = "a step met a value of a type it is not defined for";
  private static final String  NULL_TEXT         = "a step met a null it does not accept";

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
        || root instanceof UnsupportedOperationException || root instanceof NullPointerException) || !isTraversalFault(root))
      return null;

    final String message = root.getMessage();
    if (root instanceof ClassCastException)
      return new TypeMismatchException(PREFIX + (message != null ? CAST_LOADER_DETAIL.matcher(message).replaceFirst("") : OMITTED_CAST_TEXT),
          error);
    if (root instanceof NullPointerException)
      // THE HELPFUL NPE TEXT NAMES A JDK FIELD ("anotherString"), NOT WHAT THE CALLER WROTE: SAY WHAT IT MEANS FIRST
      return new CommandParsingException(PREFIX + NULL_TEXT + (message != null ? " (" + message + ")" : ""), error);
    return new CommandParsingException(PREFIX + (message != null ? message : root.getClass().getSimpleName()), error);
  }

  /** {@code error} as the client error it stands for, or unchanged when it is none. */
  public static RuntimeException translate(final RuntimeException error) {
    final RuntimeException clientError = asClientError(error);
    return clientError != null ? clientError : error;
  }

  /**
   * Whether the traversal, and not the server, is at fault for {@code error}, the root of a cause chain that is one of the
   * exceptions a step raises.
   * <p>
   * An empty stack trace is the one case that cannot be read from the frames (issue #12526): HotSpot's
   * {@code OmitStackTraceInFastThrow}, on by default, replaces an implicit {@link ClassCastException} or
   * {@link NullPointerException} thrown a few thousand times from the same site with a preallocated instance that has no frames and
   * no message, for as long as the JVM runs. Left to the frames, a traversal type error answered as a client error would turn into
   * a server fault under sustained traffic. Such an exception cannot be attributed to ArcadeDB's code either, and what a repeated
   * implicit cast or null in a traversal means is nearly always the type or the null of a value the request brought, so it is read as
   * the traversal's. Only these two are read so: the others are raised with {@code new}, which fills the trace in, so an empty one
   * on them leaves the origin unknown and the server answerable for it.
   */
  private static boolean isTraversalFault(final Throwable error) {
    final StackTraceElement[] frames = error.getStackTrace();
    if (frames.length == 0)
      return error instanceof ClassCastException || error instanceof NullPointerException;
    return raisedByTinkerPop(frames);
  }

  /** Whether the first frame of the stack that is not the JDK's belongs to TinkerPop. */
  private static boolean raisedByTinkerPop(final StackTraceElement[] frames) {
    int first = 0;
    while (first < frames.length && isJdkClass(frames[first].getClassName()))
      first++;
    return first < frames.length && frames[first].getClassName().startsWith(TINKERPOP_PACKAGE);
  }

  private static boolean isJdkClass(final String className) {
    return className.startsWith("java.") || className.startsWith("jdk.") || className.startsWith("sun.");
  }

  private static Throwable rootCause(final Throwable error) {
    Throwable root = error;
    // BOUNDED: A CAUSE CHAIN CAN LOOP BACK ON ITSELF. Objects.equals(), as the cycle guard of ArcadeGremlin: a throwable can be its
    // own cause, and Throwable does not override equals(), so this is the same reference test
    for (int depth = 0; depth < 32 && root.getCause() != null && !Objects.equals(root.getCause(), root); depth++)
      root = root.getCause();
    return root;
  }
}
