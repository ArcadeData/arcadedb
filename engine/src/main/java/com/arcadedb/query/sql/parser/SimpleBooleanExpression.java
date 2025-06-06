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
package com.arcadedb.query.sql.parser;

import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;

import java.util.*;

/**
 * Created by luigidellaquila on 21/11/16.
 */
@ExcludeFromJacocoGeneratedReport
public interface SimpleBooleanExpression {

  /**
   * if the condition involved the current pattern (MATCH statement, eg. $matched.something = foo),
   * returns the name of involved pattern aliases ("something" in this case)
   *
   * @return a list of pattern aliases involved in this condition. Null it does not involve the pattern
   */
  List<String> getMatchPatternInvolvedAliases();
}
