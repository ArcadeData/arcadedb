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
package com.arcadedb.query.sql.executor;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class AlterDatabaseExecutionTest extends TestHelper {
  @Test
  public void testBasicCreateProperty() {
    final int defPageSize = ((int) GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.getDefValue());
    assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE)).isEqualTo(defPageSize);

    database.command("sql", "ALTER DATABASE `arcadedb.bucketDefaultPageSize` 262144");

    assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE)).isEqualTo(262144);

    database.command("sql", "ALTER DATABASE `arcadedb.bucketDefaultPageSize` " + defPageSize);

    assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE)).isEqualTo(defPageSize);

    database.command("sql", "ALTER DATABASE `arcadedb.dateTimeFormat` 'yyyy-MM-dd HH:mm:ss.SSS'");

    assertThat(database.getSchema().getDateTimeFormat()).isEqualTo("yyyy-MM-dd HH:mm:ss.SSS");
  }
}
