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
package com.arcadedb.exception;

/**
 * Searches a throwable's cause chain.
 * <p>
 * The wire layers all need this and each had grown its own copy: a failure reaches them wrapped differently depending
 * on how the request arrived - directly, inside the auto-commit {@code TransactionException} wrapper, or carrying the
 * JDK exception it came from as its own cause - so deciding how to report it means looking past the outermost
 * throwable. Inspecting only {@code getCause()} is what made a doubly-wrapped client error report as a server fault.
 * <p>
 * The walk is depth-capped because a cause chain is not guaranteed acyclic: {@code initCause} can be used to build a
 * self-referential one, and an unbounded loop over it never returns. The cap lives here rather than being repeated at
 * each call site.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class CauseChain {

  /**
   * How far down a cause chain to look. Far deeper than any real wrapping (three levels is the most the engine
   * produces), and small enough that a cyclic chain costs nothing.
   */
  private static final int MAX_DEPTH = 32;

  private CauseChain() {
    // utility class
  }

  /**
   * The first throwable in {@code error}'s cause chain - {@code error} itself included - assignable to {@code type},
   * or {@code null} when the chain holds none.
   */
  public static <T extends Throwable> T find(final Throwable error, final Class<T> type) {
    Throwable current = error;
    for (int depth = 0; current != null && depth < MAX_DEPTH; current = current.getCause(), depth++) {
      if (type.isInstance(current))
        return type.cast(current);
    }
    return null;
  }

  /**
   * Whether {@link #find} would return something.
   */
  public static boolean contains(final Throwable error, final Class<? extends Throwable> type) {
    return find(error, type) != null;
  }
}
