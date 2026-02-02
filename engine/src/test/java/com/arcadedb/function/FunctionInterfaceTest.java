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
package com.arcadedb.function;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FunctionInterfaceTest {

  @Test
  void getSyntaxDefault() {
    final Function fn = createFunction("myFunction", 1, 3);
    assertThat(fn.getSyntax()).isEqualTo("myFunction(...)");
  }

  @Test
  void getSyntaxCustom() {
    final Function fn = new Function() {
      @Override
      public String getName() {
        return "customFunc";
      }

      @Override
      public int getMinArgs() {
        return 2;
      }

      @Override
      public int getMaxArgs() {
        return 2;
      }

      @Override
      public String getDescription() {
        return "Custom function";
      }

      @Override
      public String getSyntax() {
        return "customFunc(arg1, arg2)";
      }
    };

    assertThat(fn.getSyntax()).isEqualTo("customFunc(arg1, arg2)");
  }

  @Test
  void getAliasDefault() {
    final Function fn = createFunction("myFunction", 1, 3);
    assertThat(fn.getAlias()).isNull();
  }

  @Test
  void getAliasCustom() {
    final Function fn = new Function() {
      @Override
      public String getName() {
        return "newName";
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
        return "";
      }

      @Override
      public String getAlias() {
        return "oldName";
      }
    };

    assertThat(fn.getAlias()).isEqualTo("oldName");
  }

  @Test
  void validateArgsExactCountValid() {
    final Function fn = createFunction("exactFunc", 2, 2);

    // Should not throw
    fn.validateArgs(new Object[]{"a", "b"});
  }

  @Test
  void validateArgsExactCountTooFew() {
    final Function fn = createFunction("exactFunc", 2, 2);

    assertThatThrownBy(() -> fn.validateArgs(new Object[]{"a"}))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("exactFunc")
        .hasMessageContaining("exactly 2")
        .hasMessageContaining("got 1");
  }

  @Test
  void validateArgsExactCountTooMany() {
    final Function fn = createFunction("exactFunc", 2, 2);

    assertThatThrownBy(() -> fn.validateArgs(new Object[]{"a", "b", "c"}))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("exactFunc")
        .hasMessageContaining("exactly 2")
        .hasMessageContaining("got 3");
  }

  @Test
  void validateArgsRangeValid() {
    final Function fn = createFunction("rangeFunc", 1, 3);

    // Should not throw for any count in range
    fn.validateArgs(new Object[]{"a"});
    fn.validateArgs(new Object[]{"a", "b"});
    fn.validateArgs(new Object[]{"a", "b", "c"});
  }

  @Test
  void validateArgsRangeTooFew() {
    final Function fn = createFunction("rangeFunc", 2, 4);

    assertThatThrownBy(() -> fn.validateArgs(new Object[]{"a"}))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("rangeFunc")
        .hasMessageContaining("2 to 4")
        .hasMessageContaining("got 1");
  }

  @Test
  void validateArgsRangeTooMany() {
    final Function fn = createFunction("rangeFunc", 1, 3);

    assertThatThrownBy(() -> fn.validateArgs(new Object[]{"a", "b", "c", "d"}))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("rangeFunc")
        .hasMessageContaining("1 to 3")
        .hasMessageContaining("got 4");
  }

  @Test
  void validateArgsEmptyAllowed() {
    final Function fn = createFunction("optionalArgs", 0, 3);

    // Should not throw
    fn.validateArgs(new Object[]{});
  }

  @Test
  void validateArgsUnlimitedMax() {
    final Function fn = createFunction("varArgs", 1, Integer.MAX_VALUE);

    // Should not throw even with many args
    fn.validateArgs(new Object[]{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"});
  }

  private Function createFunction(final String name, final int minArgs, final int maxArgs) {
    return new Function() {
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
        return "Test function";
      }
    };
  }
}
