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
package com.arcadedb.function.sql.misc;

import com.arcadedb.database.Identifiable;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.query.opencypher.Labels;
import com.arcadedb.query.sql.executor.CommandContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;

/**
 * Creates the vertex types of an openCypher label set - every label that is missing and the composite that extends them - in ONE
 * schema frame, and answers whether this call made the composite (issue #11063).
 * <p>
 * It is what a replica running a statement in a client's transaction sends to the leader instead of creating the types itself: the
 * replica's re-base past the new type requires exactly one schema entry, which separate {@code CREATE ... TYPE} statements per label
 * would not give. The answer is a hint, not a lock: the check and the create are not atomic, so two concurrent calls for the same
 * new composite may both answer {@code true}. The permission to change the schema is asked by the type builder, as for any other route to a type.
 */
public class SQLFunctionEnsureLabelTypes extends SQLFunctionAbstract {
  public static final String NAME = "ensureLabelTypes";

  public SQLFunctionEnsureLabelTypes() {
    super(NAME);
  }

  @Override
  public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
      final CommandContext context) {
    final List<String> labels = new ArrayList<>();
    if (params != null && params.length > 0 && params[0] instanceof Collection<?> collection)
      for (final Object label : collection) {
        if (label == null)
          throw new IllegalArgumentException("ensureLabelTypes() does not accept a null label");
        labels.add(label.toString());
      }
    if (labels.isEmpty())
      throw new IllegalArgumentException("ensureLabelTypes() needs a non-empty list of labels");

    final String compositeTypeName = String.join(Labels.LABEL_SEPARATOR, new TreeSet<>(labels));
    final boolean existed = context.getDatabase().getSchema().existsType(compositeTypeName);
    Labels.ensureCompositeType(context.getDatabase().getSchema(), labels);
    return !existed;
  }

  @Override
  public String getSyntax() {
    return "ensureLabelTypes(<labels>)";
  }
}
