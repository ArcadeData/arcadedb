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

import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;

/**
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLMethodField extends AbstractSQLMethod {

    public static final String NAME = "field";

    public SQLMethodField() {
        super(NAME, 1, 1);
    }

    @Override
    public Object execute(final Object iThis,
                          final Identifiable iCurrentRecord,
                          final CommandContext iContext,
                          final Object ioResult,
                          final Object[] iParams) {
        if (iParams[0] == null)
            return null;

        final String field = iParams[0].toString();

        if (ioResult instanceof Identifiable) {
            final Document doc = (Document) ((Identifiable) ioResult).getRecord();
            return doc.get(field);
        }

        return null;
    }

    @Override
    public boolean evaluateParameters() {
        return false;
    }
}
