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
package com.arcadedb.function.coll;

import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.utility.LongRangeList;

import java.util.List;

/**
 * Abstract base class for collection functions.
 * All collection functions share the "coll." namespace prefix.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public abstract class AbstractCollFunction implements StatelessFunction {
  protected static final String NAMESPACE = "coll";

  /**
   * Returns the simple name without namespace prefix.
   */
  protected abstract String getSimpleName();

  @Override
  public String getName() {
    return NAMESPACE + "." + getSimpleName();
  }

  /**
   * Resolves a {@code coll.*} argument declared as a LIST to a List, exactly as the Cypher list builtins
   * ({@code head()}, {@code tail()}, {@code reverse()}, {@code size()}) resolve theirs.
   * <p>
   * This delegates rather than deciding, because the two families answer the same question and had drifted apart on
   * every part of the answer (issue #6403). {@link CypherFunctionHelper#requireListArgument} accepts a Java array -
   * a numeric-array parameter is a Cypher LIST, which is what issue #4284 established and what this family never
   * got - and an {@code Iterable}/{@code Iterator}, and it raises a {@link CommandSemanticException} on a type
   * mismatch, which the HTTP layer reports as 400 Bad Request. The {@code CommandExecutionException} this used to
   * raise was reported as 500, so the same client mistake read as a server fault through {@code coll.sort(42)} and
   * as the caller's through {@code tail(42)} (issues #5476/#5477/#5222).
   *
   * @return the argument as a List, or {@code null} when the argument itself is {@code null}
   *
   * @throws CommandSemanticException when the argument is neither {@code null} nor a list
   */
  protected List<Object> asList(final Object arg) {
    return CypherFunctionHelper.requireListArgument(arg, getName());
  }

  /**
   * The argument as a lazily evaluated range, or {@code null} when it is anything else.
   * <p>
   * A range is an arithmetic progression that occupies constant heap however long it is, so the operations that can
   * answer from its shape alone must not walk it: materialising one reinstates the heap exhaustion the lazy range
   * removed (issue #6353, advisory GHSA-xmjm-8q85-g778). Asked here rather than at each call site so that the answer
   * to "is this a range?" is given once - {@link #asList} hands back the very same instance for a range, since a
   * {@code LongRangeList} is already a {@code List}.
   */
  protected static LongRangeList asRange(final Object arg) {
    return arg instanceof LongRangeList range ? range : null;
  }
}
