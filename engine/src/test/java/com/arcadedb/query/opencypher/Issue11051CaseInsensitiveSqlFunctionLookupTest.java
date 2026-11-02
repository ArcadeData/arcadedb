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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Identifiable;
import com.arcadedb.function.sql.DefaultSQLFunctionFactory;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class Issue11051CaseInsensitiveSqlFunctionLookupTest extends TestHelper {
  private static final String CUSTOM_FUNCTION = "Acme11051.Twice";

  @Test
  void canonicalCamelCaseNameWorksInCall() {
    try (ResultSet result = database.query("opencypher",
        "CALL vector.l2Distance([1, 0], [0, 1]) YIELD value RETURN value")) {
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<Number>getProperty("value").doubleValue()).isCloseTo(Math.sqrt(2), within(0.000001));
    }
  }

  @Test
  void registeredNameIsFoundWithItsOriginalCase() {
    final DefaultSQLFunctionFactory factory = DefaultSQLFunctionFactory.getInstance();
    factory.register(CUSTOM_FUNCTION, Twice.class);
    try {
      assertThat(factory.hasFunction(CUSTOM_FUNCTION)).isTrue();
    } finally {
      factory.unregister(CUSTOM_FUNCTION.toLowerCase(Locale.ENGLISH));
    }
  }

  @Test
  void registeredNameCanBeRemovedWithItsOriginalCase() {
    final DefaultSQLFunctionFactory factory = DefaultSQLFunctionFactory.getInstance();
    factory.register(CUSTOM_FUNCTION, Twice.class);
    try {
      factory.unregister(CUSTOM_FUNCTION);
      assertThat(factory.getFunctionInstance(CUSTOM_FUNCTION)).isNull();
    } finally {
      factory.unregister(CUSTOM_FUNCTION.toLowerCase(Locale.ENGLISH));
    }
  }

  public static class Twice extends SQLFunctionAbstract {
    public Twice() {
      super(CUSTOM_FUNCTION);
    }

    @Override
    public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
        final CommandContext context) {
      return ((Number) params[0]).intValue() * 2;
    }

    @Override
    public String getSyntax() {
      return CUSTOM_FUNCTION + "(<n>)";
    }
  }
}
