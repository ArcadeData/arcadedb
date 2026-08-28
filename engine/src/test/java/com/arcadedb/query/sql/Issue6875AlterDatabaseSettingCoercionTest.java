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
package com.arcadedb.query.sql;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #6875: {@code ALTER DATABASE ... SETTING} is the third writer into a {@code ContextConfiguration}, after the
 * {@code set_server_setting} MCP tool and the {@code "set server setting"} HTTP command, and it stored the evaluated
 * SQL expression with the same plain map put the other two used. It now goes through
 * {@link GlobalConfiguration#coerce(Object)} as well, so a value that is not the setting's declared type is refused
 * by the statement that names it rather than by whichever component reads the setting next.
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
class Issue6875AlterDatabaseSettingCoercionTest extends TestHelper {

  @Test
  void alterDatabaseRefusesAValueThatIsNotTheSettingType() {
    assertThatThrownBy(() -> database.command("sql", "alter database `arcadedb.asyncWorkerThreads` 'abc'"))
        .hasMessageContaining("arcadedb.asyncWorkerThreads");

    // the refused statement is not the one that breaks a later read
    assertThatCode(() -> database.getConfiguration().getValueAsInteger(GlobalConfiguration.ASYNC_WORKER_THREADS))
        .doesNotThrowAnyException();
  }

  @Test
  void alterDatabaseStoresACoercedTypedValue() {
    try (final ResultSet result = database.command("sql", "alter database `arcadedb.asyncWorkerThreads` '6'")) {
      assertThat(result.hasNext()).isTrue();
      assertThat(result.next().<Object>getProperty("newValue")).isEqualTo(6);
    }

    assertThat((Object) database.getConfiguration().getValue(GlobalConfiguration.ASYNC_WORKER_THREADS))
        .isInstanceOf(Integer.class).isEqualTo(6);
    assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.ASYNC_WORKER_THREADS)).isEqualTo(6);
  }

  /** A String setting is unaffected: it takes any text, quotes stripped by the expression evaluator as before. */
  @Test
  void alterDatabaseStillAcceptsAnyTextForAStringSetting() {
    database.command("sql", "alter database `arcadedb.externalPropertyBucketPath` 'not a number at all'").close();
    assertThat(database.getConfiguration().getValueAsString(GlobalConfiguration.EXTERNAL_PROPERTY_BUCKET_PATH))
        .isEqualTo("not a number at all");
  }
}
