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
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.PatternConst;

import java.text.Normalizer;

/**
 * @author Johann Sorel (Geomatys)
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLMethodNormalize extends AbstractSQLMethod {

    public static final String NAME = "normalize";

    public SQLMethodNormalize() {
        super(NAME, 0, 2);
    }

    @Override
    public Object execute(final Object iThis,
                          final Identifiable iCurrentRecord,
                          final CommandContext iContext,
                          final Object ioResult,
                          final Object[] iParams) {

        if (ioResult != null) {
            final Normalizer.Form form = iParams != null && iParams.length > 0 ?
                    Normalizer.Form.valueOf(FileUtils.getStringContent(iParams[0].toString())) :
                    Normalizer.Form.NFD;

            String normalized = Normalizer.normalize(ioResult.toString(), form);
            if (iParams != null && iParams.length > 1) {
                return normalized.replaceAll(FileUtils.getStringContent(iParams[1].toString()), "");
            }
            return PatternConst.PATTERN_DIACRITICAL_MARKS.matcher(normalized).replaceAll("");
        }
        return null;
    }
}
