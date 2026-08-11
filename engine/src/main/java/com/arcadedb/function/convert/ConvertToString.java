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
package com.arcadedb.function.convert;

import com.arcadedb.query.sql.executor.CommandContext;

/**
 * convert.toString(value) - APOC-namespaced entry point for the standard toString() function,
 * so it is also reachable as apoc.convert.toString(). See {@link ToStringFunction} for the conversion semantics.
 */
public class ConvertToString extends AbstractConvertFunction {
  private final ToStringFunction delegate = new ToStringFunction();

  @Override
  protected String getSimpleName() {
    return "toString";
  }

  @Override
  public int getMinArgs() {
    return 1;
  }

  @Override
  public int getMaxArgs() {
    return 1;
  }

  @Override
  public String getDescription() {
    return "Converts a value to a string";
  }

  @Override
  public Object execute(final Object[] args, final CommandContext context) {
    // Checked here, not left to the delegate: ToStringFunction.checkArity would use its own getName() ("toString"),
    // naming the wrong function in the error message for a call made as convert.toString()/apoc.convert.toString().
    checkArity(args);
    return delegate.execute(args, context);
  }
}
