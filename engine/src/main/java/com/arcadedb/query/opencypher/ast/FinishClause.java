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
package com.arcadedb.query.opencypher.ast;

/**
 * Represents the GQL FINISH clause (issue #3365 section 1.3).
 * FINISH explicitly terminates a query that produces no result rows; side-effects
 * still execute. Mutually exclusive with RETURN per ISO/IEC 39075:2024.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class FinishClause {
  public static final FinishClause INSTANCE = new FinishClause();

  private FinishClause() {
  }
}
