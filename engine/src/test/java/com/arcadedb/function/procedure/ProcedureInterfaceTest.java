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
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("exactProc")
        .hasMessageContaining("exactly 2")
        .hasMessageContaining("got 1");
  }

  @Test
  void validateArgsExactCountTooMany() {
    final Procedure proc = createProcedure("exactProc", 2, 2);

    assertThatThrownBy(() -> proc.validateArgs(new Object[]{"a", "b", "c"}))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("exactProc")
        .hasMessageContaining("exactly 2")
        .hasMessageContaining("got 3");
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
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("rangeProc")
        .hasMessageContaining("2 to 4")
        .hasMessageContaining("got 1");
  }

  @Test
  void validateArgsRangeTooMany() {
    final Procedure proc = createProcedure("rangeProc", 1, 3);

    assertThatThrownBy(() -> proc.validateArgs(new Object[]{"a", "b", "c", "d"}))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("rangeProc")
        .hasMessageContaining("1 to 3")
        .hasMessageContaining("got 4");
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
