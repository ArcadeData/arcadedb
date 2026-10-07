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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9454: FOREACH over a source that evaluates to null (e.g. a variable that is not set) threw a bare NullPointerException.
 * It must iterate zero times instead.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9454ForEachNullSourceTest extends TestHelper {

  @Test
  void foreachOverUnsetVariableIteratesNothing() {
    database.getSchema().createDocumentType("Dst");

    database.command("sqlscript", """
        foreach ($p IN $notSet) {
          insert into Dst set id = 1;
        }
        """);

    assertThat(database.countType("Dst", true)).isZero();
  }
}
