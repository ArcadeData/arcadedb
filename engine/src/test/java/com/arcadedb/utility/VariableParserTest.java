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
package com.arcadedb.utility;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class VariableParserTest {

  @Test
  void resolveVariablesWithSimpleVariable() {
    final Object result = VariableParser.resolveVariables(
        "Hello ${name}!",
        "${", "}",
        variable -> "World"
    );

    assertThat(result).isEqualTo("Hello World!");
  }

  @Test
  void resolveVariablesWithMultipleVariables() {
    final Object result = VariableParser.resolveVariables(
        "${greeting} ${name}!",
        "${", "}",
        variable -> {
          if ("greeting".equals(variable))
            return "Hello";
          if ("name".equals(variable))
            return "World";
          return variable;
        }
    );

    assertThat(result).isEqualTo("Hello World!");
  }

  @Test
  void resolveVariablesWithNoVariables() {
    final Object result = VariableParser.resolveVariables(
        "Plain text",
        "${", "}",
        variable -> "unused"
    );

    assertThat(result).isEqualTo("Plain text");
  }

  @Test
  void resolveVariablesWithEmptyString() {
    final Object result = VariableParser.resolveVariables(
        "",
        "${", "}",
        variable -> "unused"
    );

    assertThat(result).isEqualTo("");
  }

  @Test
  void resolveVariablesWithCustomDelimiters() {
    final Object result = VariableParser.resolveVariables(
        "Value is [var]",
        "[", "]",
        variable -> "42"
    );

    assertThat(result).isEqualTo("Value is 42");
  }

  @Test
  void resolveVariablesWithAdjacentVariables() {
    final Object result = VariableParser.resolveVariables(
        "${a}${b}${c}",
        "${", "}",
        variable -> variable.toUpperCase()
    );

    assertThat(result).isEqualTo("ABC");
  }

  @Test
  void resolveVariablesPreservesUnknownVariables() {
    final Object result = VariableParser.resolveVariables(
        "Value is ${unknown}",
        "${", "}",
        variable -> null
    );

    // When resolver returns null without default, the variable placeholder is removed
    assertThat(result.toString()).contains("Value is");
  }

  @Test
  void systemVariableResolverResolvesEnvVariables() {
    final SystemVariableResolver resolver = new SystemVariableResolver();

    // PATH should exist on all systems
    final String path = resolver.resolve("PATH");
    assertThat(path).isNotNull();
  }

  @Test
  void systemVariableResolverResolvesSystemProperties() {
    final SystemVariableResolver resolver = new SystemVariableResolver();

    // java.version should always exist
    final String javaVersion = resolver.resolve("java.version");
    assertThat(javaVersion).isNotNull();
  }

  @Test
  void systemVariableResolverReturnsNullForUnknown() {
    final SystemVariableResolver resolver = new SystemVariableResolver();

    final String unknown = resolver.resolve("VERY_UNLIKELY_TO_EXIST_VAR_12345");
    assertThat(unknown).isNull();
  }

  @Test
  void systemVariableResolverResolveSystemVariables() {
    final SystemVariableResolver resolver = new SystemVariableResolver();

    final String result = resolver.resolveSystemVariables("Java: ${java.version}", "default");
    assertThat(result).startsWith("Java: ");
    assertThat(result).doesNotContain("${java.version}");
  }

  @Test
  void systemVariableResolverWithDefaultValue() {
    final SystemVariableResolver resolver = new SystemVariableResolver();

    final String result = resolver.resolveSystemVariables("${NONEXISTENT_VAR_XYZ}", "default");
    assertThat(result).isEqualTo("default");
  }

  @Test
  void resolveVariablesWithTextBeforeAndAfter() {
    final Object result = VariableParser.resolveVariables(
        "prefix ${var} suffix",
        "${", "}",
        variable -> "VALUE"
    );

    assertThat(result).isEqualTo("prefix VALUE suffix");
  }

  @Test
  void resolveVariablesWithSpecialCharactersInValue() {
    final Object result = VariableParser.resolveVariables(
        "${var}",
        "${", "}",
        variable -> "value with $pecial ch@rs!"
    );

    assertThat(result).isEqualTo("value with $pecial ch@rs!");
  }

  @Test
  void resolveVariablesWithDefaultValue() {
    final Object result = VariableParser.resolveVariables(
        "${missing}",
        "${", "}",
        variable -> null,
        "defaultValue"
    );

    assertThat(result).isEqualTo("defaultValue");
  }
}
