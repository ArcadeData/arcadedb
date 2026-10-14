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
package com.arcadedb.function.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.function.sql.misc.SQLStaticReflectiveFunction;
import com.arcadedb.log.WarningCapture;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #9986: {@code SELECT math_max(null)} raised a raw {@link NullPointerException} out of the very message that
 * was meant to report that no overload matches, because the diagnostic called {@code getClass()} on each argument.
 * <p>
 * The neighbouring case went wrong too, more quietly: a null where every overload wants a primitive
 * ({@code math_abs(null)}) was matched anyway, failed inside {@code Method.invoke}, and came back as {@code null} after
 * logging a SEVERE line - once per row. A null now matches only a reference parameter, and a null that leaves no
 * overload of the right arity is SQL's NULL in, NULL out, with nothing logged.
 * <p>
 * The guarantee is a sweep over every registered reflective function, at every arity from one to three, with all-null
 * arguments: the answer is a value or a {@link CommandParsingException}, never anything else.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9986ReflectiveFunctionNullArgumentTest extends TestHelper {

  @Test
  void aNullArgumentToAnArityNoOverloadHasIsReportedNotAnNpe() {
    assertThatThrownBy(() -> single("select math_max(null) as v"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("Unable to find a function for math_max(null [ null ])");
  }

  @Test
  void aNullForAPrimitiveParameterIsNullInNullOutWithNothingLogged() {
    final List<String> severe = WarningCapture.captureSevere(() -> {
      assertThat(single("select math_abs(null) as v")).isNull();
      assertThat(single("select math_max(null, 1) as v")).isNull();
      assertThat(single("select math_max(1, null) as v")).isNull();
    });
    assertThat(severe).as("a NULL argument is not an engine error").isEmpty();

    // AND A NON-NULL CALL STILL PICKS ITS OVERLOAD
    assertThat(single("select math_max(3, 7) as v")).isEqualTo(7);
    assertThat(single("select math_abs(-2.5) as v")).isEqualTo(2.5);
  }

  @Test
  void aWrongTypedArgumentBesideANullIsStillReported() {
    // NO math_max OVERLOAD TAKES A STRING: THE NULL BESIDE IT DOES NOT TURN THAT INTO A SILENT NULL
    assertThatThrownBy(() -> single("select math_max('text', null) as v"))
        .isInstanceOf(CommandParsingException.class)
        .hasMessageContaining("Unable to find a function for math_max(text [ java.lang.String ], null [ null ])");
  }

  @Test
  void everyReflectiveFunctionAnswersAllNullArgumentsWithAValueOrAParsingError() {
    final List<String> failures = new ArrayList<>();
    for (final Map.Entry<String, Object> entry : DefaultSQLFunctionFactory.getInstance().getFunctions().entrySet()) {
      if (!(entry.getValue() instanceof SQLStaticReflectiveFunction))
        continue;

      for (int arity = 1; arity <= 3; arity++) {
        final String sql = "select " + entry.getKey() + "(" + String.join(", ", Collections.nCopies(arity, "null"))
            + ") as v";
        try {
          single(sql);
        } catch (final CommandParsingException expected) {
          // NO OVERLOAD OF THIS ARITY: REPORTED, WHICH IS FINE
        } catch (final RuntimeException e) {
          failures.add(sql + " -> " + e);
        }
      }
    }

    assertThat(failures).isEmpty();
  }

  private Object single(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next().getProperty("v");
    }
  }
}
