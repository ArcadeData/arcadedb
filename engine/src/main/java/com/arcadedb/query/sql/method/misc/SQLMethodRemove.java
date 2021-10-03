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
    public Object execute(final Object iThis,
                          final Identifiable iCurrentRecord,
                          final CommandContext iContext,
                          final Object ioResult,
                          final Object[] iParams) {
        if (iParams != null && iParams.length > 0 && iParams[0] != null) {
            Object[] arguments = MultiValue.array(iParams, Object.class, iArgument -> {
                if (iArgument instanceof String &&
                        ((String) iArgument).startsWith("$")) {
                    return iContext.getVariable((String) iArgument);
                }
                return iArgument;
            });
            Object cleaned = null;
            for (Object o : arguments) {
                cleaned = MultiValue.remove(ioResult, o, false);
            }
            return cleaned;
        }

        return ioResult;
    }

    @Override
    public String getSyntax() {
        return "remove(<item>*)";
    }
}
