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

import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * Shared SET/CONTENT application for the RESTORE DOCUMENT/VERTEX/EDGE statements. Mirrors the same evaluation calls
 * {@code UpdateSetStep}/{@code UpdateContentStep} use for a normal INSERT/UPDATE, applied directly here instead of
 * through the streaming execution-plan pipeline: RESTORE always targets exactly one, already-identified RID, so
 * there is no upstream result set to pull from.
 */
final class RestoreStatementSupport {
  private RestoreStatementSupport() {
  }

  /**
   * No-op when {@code body} is {@code null} (the common case: an empty shell restore that recovers structure only,
   * not the original property values - see each RESTORE statement's grammar javadoc).
   */
  static void applyBody(final MutableDocument doc, final InsertBody body, final CommandContext context) {
    if (body == null)
      return;

    if (body.setExpressions != null) {
      for (final InsertSetExpression exp : body.setExpressions)
        doc.set(exp.left.getStringValue(), exp.right.execute((com.arcadedb.database.Identifiable) null, context));
    } else if (body.contentJson != null) {
      doc.fromMap(body.contentJson.toMap((com.arcadedb.database.Identifiable) null, context));
    } else if (body.contentArray != null && !body.contentArray.items.isEmpty()) {
      doc.fromMap(body.contentArray.items.get(0).toMap((com.arcadedb.database.Identifiable) null, context));
    } else if (body.contentInputParam != null) {
      final Object val = body.contentInputParam.getValue(context.getInputParameters());
      if (val instanceof java.util.Map<?, ?> map)
        doc.fromMap((java.util.Map<String, Object>) map);
      else if (val != null)
        throw new com.arcadedb.exception.CommandSQLParsingException("Invalid CONTENT value for RESTORE: " + val);
    }
  }
}
