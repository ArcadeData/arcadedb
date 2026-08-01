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
package com.arcadedb.function.procedure;

import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProcedureInterfaceTest {

  @Test
  void validateArgsExactCountValid() {
    final Procedure proc = createProcedure("exactProc", 2, 2);

    // Should not throw
    proc.validateArgs(new Object[]{"a", "b"});
  }

  @Test
  void validateArgsExactCountTooFew() {
    final Procedure proc = createProcedure("exactProc", 2, 2);

    assertThatThrownBy(() -> proc.validateArgs(new Object[]{"a"}))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessage("Procedure 'exactProc' expects 2 arguments but got 1");
  }

  @Test
  void validateArgsExactCountTooMany() {
    final Procedure proc = createProcedure("exactProc", 2, 2);

    assertThatThrownBy(() -> proc.validateArgs(new Object[]{"a", "b", "c"}))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessage("Procedure 'exactProc' expects 2 arguments but got 3");
  }

  @Test
  void validateArgsRangeValid() {
    final Procedure proc = createProcedure("rangeProc", 1, 3);

    // Should not throw for any count in range
    proc.validateArgs(new Object[]{"a"});
    proc.validateArgs(new Object[]{"a", "b"});
    proc.validateArgs(new Object[]{"a", "b", "c"});
  }

  @Test
  void validateArgsRangeTooFew() {
    final Procedure proc = createProcedure("rangeProc", 2, 4);

    assertThatThrownBy(() -> proc.validateArgs(new Object[]{"a"}))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessage("Procedure 'rangeProc' expects 2-4 arguments but got 1");
  }

  @Test
  void validateArgsRangeTooMany() {
    final Procedure proc = createProcedure("rangeProc", 1, 3);

    assertThatThrownBy(() -> proc.validateArgs(new Object[]{"a", "b", "c", "d"}))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessage("Procedure 'rangeProc' expects 1-3 arguments but got 4");
  }

  @Test
  void anUnboundedMaximumIsNotReadAsAnUpperLimit() {
    // The two spellings of "no limit": the registry writes -1, Function.getMaxArgs defaults to Integer.MAX_VALUE.
    // A raw `count > getMaxArgs()` against -1 rejects every call, since any count exceeds -1 (issue #5627).
    for (final int unbounded : new int[] { -1, Integer.MAX_VALUE }) {
      final Procedure proc = createProcedure("variadicProc", 1, unbounded);

      proc.validateArgs(new Object[]{"a"});
      proc.validateArgs(new Object[]{"a", "b", "c", "d"});

      assertThatThrownBy(() -> proc.validateArgs(new Object[0]))
          .as("maxArgs=%d", unbounded)
          .isInstanceOf(CommandSemanticException.class)
          .hasMessage("Procedure 'variadicProc' expects at least 1 argument but got 0");
    }
  }

  @Test
  void aNullArgumentArrayCountsAsNoArguments() {
    // args.length on a null array raised NullPointerException, which the CALL path wraps as an internal failure.
    assertThatThrownBy(() -> createProcedure("exactProc", 2, 2).validateArgs(null))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessage("Procedure 'exactProc' expects 2 arguments but got 0");

    // A procedure accepting no arguments is handed the null array unchanged, as functions are.
    createProcedure("zeroArgProc", 0, 0).validateArgs(null);
  }

  @Test
  void isWriteProcedureDefaultIsFalse() {
    final Procedure proc = createProcedure("readProc", 0, 0);
    assertThat(proc.isWriteProcedure()).isFalse();
  }

  @Test
  void isWriteProcedureCanBeOverridden() {
    final Procedure proc = new Procedure() {
      @Override
      public String getName() {
        return "writeProc";
      }

      @Override
      public int getMinArgs() {
        return 0;
      }

      @Override
      public int getMaxArgs() {
        return 0;
      }

      @Override
      public String getDescription() {
        return "";
      }

      @Override
      public List<String> getYieldFields() {
        return Collections.emptyList();
      }

      @Override
      public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
        return Stream.empty();
      }

      @Override
      public boolean isWriteProcedure() {
        return true;
      }
    };

    assertThat(proc.isWriteProcedure()).isTrue();
  }

  private Procedure createProcedure(final String name, final int minArgs, final int maxArgs) {
    return new Procedure() {
      @Override
      public String getName() {
        return name;
      }

      @Override
      public int getMinArgs() {
        return minArgs;
      }

      @Override
      public int getMaxArgs() {
        return maxArgs;
      }

      @Override
      public String getDescription() {
        return "Test procedure";
      }

      @Override
      public List<String> getYieldFields() {
        return Collections.singletonList("result");
      }

      @Override
      public Stream<Result> execute(final Object[] args, final Result inputRow, final CommandContext context) {
        return Stream.empty();
      }
    };
  }
}
