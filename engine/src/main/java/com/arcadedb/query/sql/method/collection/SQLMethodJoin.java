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
package com.arcadedb.query.sql.method.collection;

import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.method.AbstractSQLMethod;

/**
 * Splits a string using a delimiter.
 *
 * @author Luca Garulli (l.garulli--(at)--gmail.com)
 */
public class SQLMethodJoin extends AbstractSQLMethod {

    public static final String NAME = "join";

    public SQLMethodJoin() {
        super(NAME, 1);
    }

    @Override
    public Object execute(final Object value,
                          final Identifiable record,
                          final CommandContext context,
                          final Object[] params) {

        if (value == null) {
            return null;
        }

        if (value instanceof List) {

            final String separator = Optional.ofNullable(params)
                    .filter(p -> p.length > 0)
                    .filter(p -> p[0] != null)
                    .map(p -> p[0].toString())
                    .orElse(",");

            return ((List<?>) value).stream()
                    .map(Object::toString)
                    .collect(Collectors.joining(separator));

        } else
            return value.toString();
    }
}
