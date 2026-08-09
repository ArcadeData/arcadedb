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
package com.arcadedb.query.sql.method;

import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class AbstractSQLMethodTest {

  private static class DummyMethod extends AbstractSQLMethod {
    DummyMethod(final int minParams, final int maxParams) {
      super("dummy", minParams, maxParams);
    }

    @Override
    public Object execute(final Object self, final Identifiable currentRecord, final CommandContext context, final Object[] params) {
      return null;
    }
  }

  @Test
  void syntaxWithFixedParamCount() {
    assertThat(new DummyMethod(2, 2).getSyntax()).isEqualTo("<field>.dummy(param1, param2)");
  }

  @Test
  void syntaxWithBoundedOptionalParams() {
    assertThat(new DummyMethod(1, 3).getSyntax()).isEqualTo("<field>.dummy(param1[, param2, param3])");
  }

  @Test
  void syntaxWithVariadicParamsAndRequiredMinimum() {
    // REGRESSION FOR #5972: maxParams == -1 USED TO RENDER AN EMPTY "[]" REGARDLESS OF minParams
    assertThat(new DummyMethod(1, -1).getSyntax()).isEqualTo("<field>.dummy(param1[, param2]*)");
  }

  @Test
  void syntaxWithVariadicParamsAndNoRequiredMinimum() {
    assertThat(new DummyMethod(0, -1).getSyntax()).isEqualTo("<field>.dummy([param1[, param2]*])");
  }
}
