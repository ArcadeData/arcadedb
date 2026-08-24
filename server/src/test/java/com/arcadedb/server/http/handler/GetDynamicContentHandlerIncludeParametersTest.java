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
package com.arcadedb.server.http.handler;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Regression tests for issue #6423: an {@code ${include:file.html k=v ...}} parameter with no '=' used to throw
 * {@link ArrayIndexOutOfBoundsException} out of the page render, and a value containing a further '=' was
 * silently truncated.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GetDynamicContentHandlerIncludeParametersTest {

  @Test
  void wellFormedParametersArePutIntoVariables() {
    final Map<String, Object> variables = new HashMap<>();
    GetDynamicContentHandler.parseIncludeParameters("a=1 b=2", "test.html", variables);
    assertThat(variables).containsEntry("a", "1").containsEntry("b", "2");
  }

  @Test
  void valueContainingEqualsIsKeptWhole() {
    final Map<String, Object> variables = new HashMap<>();
    GetDynamicContentHandler.parseIncludeParameters("flag=x=y", "test.html", variables);
    assertThat(variables).containsEntry("flag", "x=y");
  }

  @Test
  void parameterWithNoSeparatorIsSkippedInsteadOfThrowing() {
    final Map<String, Object> variables = new HashMap<>();
    assertThatCode(() -> GetDynamicContentHandler.parseIncludeParameters("a=1 noequals b=2", "test.html", variables))
        .doesNotThrowAnyException();
    assertThat(variables).containsEntry("a", "1").containsEntry("b", "2").doesNotContainKey("noequals");
  }
}
