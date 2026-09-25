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

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8301: the default name of a SQL {@code EXPORT DATABASE} carries a timestamp that must not depend on the JVM's
 * default locale (calendar and digits), like every other generated file name.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8301ExportDefaultTargetNameTest {

  @Test
  void defaultTargetNameIsLocaleIndependent() {
    final Locale saved = Locale.getDefault();
    try {
      for (final String tag : new String[] { "th-TH", "ar-EG", "ja-JP-u-ca-japanese", "en-US" }) {
        Locale.setDefault(Locale.forLanguageTag(tag));
        final String name = ExportDatabaseStatement.defaultTargetName("db8301", "jsonl");
        assertThat(name).as("locale %s", tag)
            .matches("db8301-export-\\d{8}-\\d{9}-[0-9a-f-]{36}\\.jsonl\\.tgz")
            .startsWith("db8301-export-" + LocalDate.now().getYear());
      }
    } finally {
      Locale.setDefault(saved);
    }
  }
}
