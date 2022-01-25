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

/**
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLMethodAsBoolean extends AbstractSQLMethod {

    public static final String NAME = "asboolean";

    public SQLMethodAsBoolean() {
        super(NAME);
    }

    @Override
    public Object execute(Object self,
                          Identifiable currentRecord,
                          CommandContext context,
                          Object result,
                          Object[] params) {
        if (result != null) {
            if (result instanceof String) {
                result = Boolean.valueOf(((String) result).trim());
            } else if (result instanceof Number) {
                return ((Number) result).intValue() != 0;
            }
        }
        return result;
    }
}
