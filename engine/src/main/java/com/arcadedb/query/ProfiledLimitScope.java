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
package com.arcadedb.query;

/**
 * Whether the current thread is running, or reading a row of, a statement whose rows the {@code resultSetLimit} of the user's
 * groups already caps (issue #12888). A statement run meanwhile - a subquery, a procedure, a user-defined function in any
 * language - is a part of the capped one and not an answer to the user, so no query engine applies the quota to it again: the
 * quota caps the rows the outer statement answers, never what it computes them from.
 * <p>
 * A counter, not a flag, so a capped statement that reads the rows of another one it holds does not clear it for the outer one.
 * Every {@link #enter()} is paired with an {@link #exit()} in a {@code finally}.
 */
public final class ProfiledLimitScope {
  private static final ThreadLocal<int[]> RUNNING = ThreadLocal.withInitial(() -> new int[1]);

  private ProfiledLimitScope() {
  }

  /** Whether this thread is inside a capped statement: a statement run now is a part of it and is not capped on its own. */
  public static boolean isActive() {
    return RUNNING.get()[0] > 0;
  }

  public static void enter() {
    ++RUNNING.get()[0];
  }

  public static void exit() {
    --RUNNING.get()[0];
  }
}
