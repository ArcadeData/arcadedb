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
package com.arcadedb.function.path;

import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.function.StatelessFunction;
import com.arcadedb.function.cypher.CypherFunctionHelper;
import com.arcadedb.query.opencypher.traversal.TraversalPath;

/**
 * Abstract base class for path functions.
 * <p>
 * Every Cypher path is a {@link TraversalPath} (issue #9913): the one a pattern binds, the one a path procedure yields, and
 * the one each of these functions returns, so {@code nodes()}, {@code relationships()}, {@code length()} and the drivers
 * read it as a path (issue #10017). {@link TraversalPath#of(Object)} also reads the {@code {_type: "path"}} map a client may
 * send back as a parameter.
 *
 * @author Luca Garulli (l.garulli--(at)--arcadedata.com)
 */
public abstract class AbstractPathFunction implements StatelessFunction {
  @Override
  public String getName() {
    return "path." + getSimpleName();
  }

  protected abstract String getSimpleName();

  /**
   * Returns the path argument in any shape {@link TraversalPath#of(Object)} reads. Anything else is a type error rather
   * than a null or empty answer, which would read as a correct one (issue #9913).
   *
   * @throws CommandSemanticException when the argument is not a path
   */
  protected TraversalPath requirePath(final Object value) {
    final TraversalPath path = TraversalPath.of(value);
    if (path == null)
      throw CypherFunctionHelper.typeMismatch(getName(), "a PATH", value);
    return path;
  }
}
