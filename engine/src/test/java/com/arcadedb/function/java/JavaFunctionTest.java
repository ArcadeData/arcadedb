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
package com.arcadedb.function.java;

import com.arcadedb.TestHelper;
import com.arcadedb.function.FunctionExecutionException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class JavaFunctionTest extends TestHelper {

  public static class Sum {
    public int sum(final int a, final int b) {
      return a + b;
    }

    public static int SUM(final int a, final int b) {
      return a + b;
    }
  }

  @Test
  public void testRegistration()
          throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    // TEST REGISTRATION HERE
    registerClass();

    try {
      registerClass();
      fail("");
    } catch (final IllegalArgumentException e) {
      // EXPECTED
    }

    database.getSchema().unregisterFunctionLibrary("math");
    registerClass();
  }

  @Test
  public void testRegistrationByClassInstance()
          throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    // TEST REGISTRATION HERE
    database.getSchema().registerFunctionLibrary(new JavaClassFunctionLibraryDefinition("math", JavaFunctionTest.Sum.class));

    try {
      database.getSchema().registerFunctionLibrary(new JavaClassFunctionLibraryDefinition("math", JavaFunctionTest.Sum.class));
      fail("");
    } catch (final IllegalArgumentException e) {
      // EXPECTED
    }

    database.getSchema().unregisterFunctionLibrary("math");
    database.getSchema().registerFunctionLibrary(new JavaClassFunctionLibraryDefinition("math", JavaFunctionTest.Sum.class));
  }

  @Test
  public void testRegistrationSingleMethods()
          throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    // TEST REGISTRATION HERE
    database.getSchema()
            .registerFunctionLibrary(new JavaMethodFunctionLibraryDefinition("math", JavaFunctionTest.Sum.class.getMethod("sum", Integer.TYPE, Integer.TYPE)));

    try {
      database.getSchema()
              .registerFunctionLibrary(new JavaMethodFunctionLibraryDefinition("math", JavaFunctionTest.Sum.class.getMethod("sum", Integer.TYPE, Integer.TYPE)));
      fail("");
    } catch (final IllegalArgumentException e) {
      // EXPECTED
    }

    database.getSchema().unregisterFunctionLibrary("math");
    database.getSchema()
            .registerFunctionLibrary(new JavaMethodFunctionLibraryDefinition("math", JavaFunctionTest.Sum.class.getMethod("sum", Integer.TYPE, Integer.TYPE)));
  }

  @Test
  public void testFunctionNotFound() {
    try {
      database.getSchema().getFunction("math", "sum");
      fail("");
    } catch (final IllegalArgumentException e) {
      // EXPECTED
    }
  }

  @Test
  public void testMethodParameterByPosition()
          throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    // TEST REGISTRATION HERE
    registerClass();

    final Integer result = (Integer) database.getSchema().getFunction("math", "sum").execute(3, 5);
    assertThat(result).isEqualTo(8);
  }

  @Test
  public void testStaticMethodParameterByPosition()
          throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    registerClass();

    final Integer result = (Integer) database.getSchema().getFunction("math", "SUM").execute(3, 5);
    assertThat(result).isEqualTo(8);
  }

  @Test
  public void testExecuteFromSQL()
          throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    registerClass();

    database.transaction(() -> {
      final ResultSet rs = database.command("SQL", "SELECT `math.sum`(20,7) as sum");
      assertThat(rs.hasNext()).isTrue();
      final Result record = rs.next();
      assertThat(record).isNotNull();
      assertThat(record.getIdentity()).isNotPresent();
      assertThat(((Number) record.getProperty("sum")).intValue()).isEqualTo(27);
    });
  }

  @Test
  public void testNotFound() throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    registerClass();
    try {
      database.getSchema().getFunction("math", "NOT_found").execute(3, 5);
      fail("");
    } catch (IllegalArgumentException e) {
      // EXPECTED
    }
  }

  @Test
  public void testExecutionError()
          throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    registerClass();
    try {
      database.getSchema().getFunction("math", "SUM").execute("invalid", 5);
      fail("");
    } catch (FunctionExecutionException e) {
      // EXPECTED
    }
  }

  private void registerClass() throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
    database.getSchema().registerFunctionLibrary(new JavaClassFunctionLibraryDefinition("math", "com.arcadedb.function.java.JavaFunctionTest$Sum"));
  }
}
