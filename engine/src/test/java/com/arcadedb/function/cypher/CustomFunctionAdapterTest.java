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
package com.arcadedb.function.cypher;

import com.arcadedb.TestHelper;
import com.arcadedb.function.FunctionDefinition;
import com.arcadedb.function.FunctionLibraryDefinition;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #7005: {@link CustomFunctionAdapter#execute(Object[], CommandContext)} wrapped the
 * exact-match function <b>call</b> inside the {@code try} meant to guard only the lookup, so a function body
 * throwing {@link IllegalArgumentException} was misread as "function not found" and the case-insensitive fallback
 * ran (and executed) the same function a second time.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CustomFunctionAdapterTest extends TestHelper {

  private static final String LIBRARY_NAME  = "adapterTest";
  private static final String FUNCTION_NAME = "throwing";

  /**
   * A function whose body always throws {@link IllegalArgumentException}, counting how many times it was invoked.
   */
  private static class ThrowingFunction implements FunctionDefinition {
    final AtomicInteger invocations = new AtomicInteger();

    @Override
    public String getName() {
      return FUNCTION_NAME;
    }

    @Override
    public Object execute(final Object... parameters) {
      invocations.incrementAndGet();
      throw new IllegalArgumentException("invalid argument from function body");
    }
  }

  private static class SingleFunctionLibrary implements FunctionLibraryDefinition<FunctionDefinition> {
    final FunctionDefinition function;

    SingleFunctionLibrary(final FunctionDefinition function) {
      this.function = function;
    }

    @Override
    public String getName() {
      return LIBRARY_NAME;
    }

    @Override
    public Iterable<FunctionDefinition> getFunctions() {
      return Collections.singletonList(function);
    }

    @Override
    public FunctionDefinition getFunction(final String functionName) throws IllegalArgumentException {
      if (!function.getName().equals(functionName))
        throw new IllegalArgumentException("Function '" + functionName + "' not defined");
      return function;
    }

    @Override
    public boolean hasFunction(final String functionName) {
      return function.getName().equals(functionName);
    }

    @Override
    public FunctionLibraryDefinition<FunctionDefinition> registerFunction(final FunctionDefinition registerFunction) {
      throw new UnsupportedOperationException();
    }

    @Override
    public FunctionLibraryDefinition<FunctionDefinition> unregisterFunction(final String functionName) {
      throw new UnsupportedOperationException();
    }
  }

  @Test
  void bodyThrowingIllegalArgumentExceptionIsNotSwallowedNorRetried() {
    final ThrowingFunction function = new ThrowingFunction();
    database.getSchema().registerFunctionLibrary(new SingleFunctionLibrary(function));
    try {
      final CustomFunctionAdapter adapter = new CustomFunctionAdapter(LIBRARY_NAME, FUNCTION_NAME);
      final BasicCommandContext context = new BasicCommandContext();
      context.setDatabase(database);

      assertThatThrownBy(() -> adapter.execute(new Object[0], context))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("invalid argument from function body");

      // The exact-match lookup must not fall through to the case-insensitive search and execute the function again.
      assertThat(function.invocations.get()).isEqualTo(1);
    } finally {
      database.getSchema().unregisterFunctionLibrary(LIBRARY_NAME);
    }
  }
}
