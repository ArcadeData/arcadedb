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
package com.arcadedb.query.opencypher.functions.coll;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.function.StatelessFunction;

import java.util.Collection;
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
   * Safely converts an argument to a List.
   */
  @SuppressWarnings("unchecked")
  protected List<Object> asList(final Object arg) {
    if (arg == null)
      return null;
    if (arg instanceof List)
      return (List<Object>) arg;
    if (arg instanceof Collection)
      return new java.util.ArrayList<>((Collection<Object>) arg);
    throw new CommandExecutionException(getName() + "() requires a list argument, got: " + arg.getClass().getSimpleName());
  }
}
