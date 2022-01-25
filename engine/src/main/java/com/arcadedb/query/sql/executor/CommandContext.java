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

import com.arcadedb.database.DatabaseInternal;

import java.util.Map;

/**
 * Basic interface for commands. Manages the context variables during execution.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public interface CommandContext {
  enum TIMEOUT_STRATEGY {
    RETURN, EXCEPTION
  }

  Object getVariable(String iName);

  Object getVariable(String iName, Object iDefaultValue);

  CommandContext setVariable(String iName, Object iValue);

  CommandContext incrementVariable(String getNeighbors);

  Map<String, Object> getVariables();

  CommandContext getParent();

  CommandContext setParent(CommandContext iParentContext);

  CommandContext setChild(CommandContext context);

  /**
   * Updates a counter. Used to record metrics.
   *
   * @param iName  Metric's name
   * @param iValue delta to add or subtract
   *
   * @return
   */
  long updateMetric(String iName, long iValue);

  boolean isRecordingMetrics();

  CommandContext setRecordingMetrics(boolean recordMetrics);

  void beginExecution(long timeoutMs, TIMEOUT_STRATEGY iStrategy);

  /**
   * Check if timeout is elapsed, if defined.
   *
   * @return false if it the timeout is elapsed and strategy is "return"
   * if the strategy is "exception" (default)
   */
  boolean checkTimeout();

  Map<String, Object> getInputParameters();

  void setInputParameters(Map<String, Object> inputParameters);

  /**
   * Creates a copy of execution context.
   */
  CommandContext copy();

  DatabaseInternal getDatabase();

  void declareScriptVariable(String varName);

  boolean isScriptVariableDeclared(String varName);
}
