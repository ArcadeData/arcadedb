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

import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.function.date.DateAdd;
import com.arcadedb.function.date.DateConvert;
import com.arcadedb.function.text.TextCharAt;
import com.arcadedb.function.text.TextIndexOf;
import com.arcadedb.function.text.TextLpad;
import com.arcadedb.function.text.TextRandom;
import com.arcadedb.function.text.TextRpad;
import com.arcadedb.function.util.UtilSleep;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10954: the TEXT branch of the integer arguments of date.add / date.convert / util.sleep / text.* went through
 * Long.parseLong / Integer.parseInt, so a text that is not a whole number answered a raw NumberFormatException (an internal
 * error) instead of the client error naming the function and the argument that the number branch gives.
 */
class Issue10954TextWholeNumberArgumentsTest {

  @Test
  void dateConvertRefusesANonIntegerText() {
    final DateConvert convert = new DateConvert();
    assertThat(convert.execute(new Object[] { "2", "s", "ms" }, null)).isEqualTo(2000L);
    assertThatThrownBy(() -> convert.execute(new Object[] { "1.5", "s", "ms" }, null))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessage("date.convert() requires a whole number <value>, but received '1.5'");
    assertThatThrownBy(() -> convert.execute(new Object[] { "abc", "s", "ms" }, null))
        .isInstanceOf(CommandSemanticException.class).hasMessageContaining("<value>");
  }

  @Test
  void dateAddRefusesANonIntegerText() {
    final DateAdd add = new DateAdd();
    assertThat(add.execute(new Object[] { 1000L, "2", "ms" }, null)).isEqualTo(1002L);
    assertThatThrownBy(() -> add.execute(new Object[] { 1000L, "1.5", "h" }, null))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessage("date.add() requires a whole number <value>, but received '1.5'");
  }

  @Test
  void utilSleepRefusesANonIntegerText() {
    assertThat(new UtilSleep().execute(new Object[] { "0" }, null)).isNull();
    assertThatThrownBy(() -> new UtilSleep().execute(new Object[] { "1.5" }, null))
        .isInstanceOf(CommandSemanticException.class).hasMessageContaining("<milliseconds>");
  }

  @Test
  void textFunctionsRefuseANonIntegerText() {
    assertThat(new TextLpad().execute(new Object[] { "x", "3", "-" }, null)).isEqualTo("--x");
    assertThatThrownBy(() -> new TextLpad().execute(new Object[] { "x", "2.5", "-" }, null))
        .isInstanceOf(CommandSemanticException.class).hasMessageContaining("text.lpad() requires a whole number <length>");
    assertThatThrownBy(() -> new TextRpad().execute(new Object[] { "x", "abc", "-" }, null))
        .isInstanceOf(CommandSemanticException.class).hasMessageContaining("<length>");
    assertThatThrownBy(() -> new TextCharAt().execute(new Object[] { "abc", "1.5" }, null))
        .isInstanceOf(CommandSemanticException.class).hasMessageContaining("<index>");
    assertThatThrownBy(() -> new TextIndexOf().execute(new Object[] { "abc", "a", "1.5" }, null))
        .isInstanceOf(CommandSemanticException.class).hasMessageContaining("<start>");
    assertThatThrownBy(() -> new TextRandom().execute(new Object[] { "1.5" }, null))
        .isInstanceOf(CommandSemanticException.class).hasMessageContaining("<length>");
    assertThatThrownBy(() -> new TextCharAt().execute(new Object[] { "abc", "4294967297" }, null))
        .isInstanceOf(CommandSemanticException.class).hasMessageContaining("in the int range");
  }
}
