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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The SQL {@code COMMIT RETRY} step has its own copy of the backoff. TX_RETRY_DELAY has database scope, so that
 * copy must come from the database the script runs against and not from the global static.
 * <p>
 * The command context's own configuration is not a substitute: the script engine is handed an empty one on the
 * embedded API and the server's one over HTTP, and neither carries a per-database override.
 */
public class Issue5693RetryStepDelayScopeTest extends TestHelper {
  @Test
  void theStepReadsTheDelayConfiguredOnThisDatabase() {
    final int savedGlobal = GlobalConfiguration.TX_RETRY_DELAY.getValueAsInteger();
    GlobalConfiguration.TX_RETRY_DELAY.setValue(11);
    try {
      database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY, 777);

      assertThat(newRetryStep().getRetryDelay()).isEqualTo(777);
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.TX_RETRY_DELAY, savedGlobal);
      GlobalConfiguration.TX_RETRY_DELAY.setValue(savedGlobal);
    }
  }

  @Test
  void theStepFallsBackToTheGlobalDelayWhenTheDatabaseDoesNotOverrideIt() {
    final int savedGlobal = GlobalConfiguration.TX_RETRY_DELAY.getValueAsInteger();
    GlobalConfiguration.TX_RETRY_DELAY.setValue(13);
    try {
      assertThat(newRetryStep().getRetryDelay()).isEqualTo(13);
    } finally {
      GlobalConfiguration.TX_RETRY_DELAY.setValue(savedGlobal);
    }
  }

  /**
   * Builds the step the way the script engine does, with a context that carries the database but an empty
   * configuration of its own.
   */
  private RetryStep newRetryStep() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(((DatabaseInternal) database).getWrappedDatabaseInstance());
    return new RetryStep(List.of(), 3, null, Boolean.FALSE, context, false);
  }
}
