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
package com.arcadedb.query.sql.method.misc;

import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.MultiValue;

/**
 * Remove the first occurrence of elements from a collection.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 * @see SQLMethodRemoveAll
 */
public class SQLMethodRemove extends AbstractSQLMethod {

    public static final String NAME = "remove";

    public SQLMethodRemove() {
        super(NAME, 1, -1);
    }

    @Override
    public Object execute(final Object self,
                          final Identifiable currentRecord,
                          final CommandContext context,
                          Object result,
                          final Object[] params) {
        if (params != null &&
                params.length > 0 &&
                params[0] != null) {
            Object[] arguments = MultiValue.array(params, Object.class, argument -> {
                if (argument instanceof String &&
                        ((String) argument).startsWith("$")) {
                    return context.getVariable((String) argument);
                }
                return argument;
            });
            for (Object o : arguments) {
                result = MultiValue.remove(result, o, false);
            }
            return result;
        }

        return result;
    }

    @Override
    public String getSyntax() {
        return "remove(<item>*)";
    }
}
