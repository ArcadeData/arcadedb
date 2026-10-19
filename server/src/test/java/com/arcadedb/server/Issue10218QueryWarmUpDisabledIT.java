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
package com.arcadedb.server;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.GlobalConfiguration;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10218: {@code arcadedb.server.queryWarmUp=false} starts no warm-up, and the server answers its queries as it always did.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10218QueryWarmUpDisabledIT extends BaseGraphServerTest {

  @Override
  protected void onServerConfiguration(final ContextConfiguration config) {
    super.onServerConfiguration(config);
    config.setValue(GlobalConfiguration.SERVER_QUERY_WARM_UP, false);
  }

  @Test
  void theSwitchStartsNoWarmUp() {
    assertThat(getServer(0).getQueryWarmUp()).isNull();

    try (final ResultSet resultSet = getServer(0).getDatabase(getDatabaseName()).query("sql", "select 1 as one")) {
      assertThat(resultSet.next().<Integer>getProperty("one")).isEqualTo(1);
    }
  }
}
