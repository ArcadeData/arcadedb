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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.*;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

public class SchemaTest extends TestHelper {

  @Test
  public void tesSchemaSettings() {
    database.transaction(() -> {
      final ZoneId zoneId = database.getSchema().getZoneId();
      assertThat(zoneId).isNotNull();
      database.getSchema().setZoneId(ZoneId.of("America/New_York"));
      assertThat(database.getSchema().getZoneId()).isEqualTo(ZoneId.of("America/New_York"));

      final TimeZone timeZone = database.getSchema().getTimeZone();
      assertThat(timeZone).isNotNull();
      database.getSchema().setTimeZone(TimeZone.getTimeZone("UK"));
      assertThat(database.getSchema().getTimeZone()).isEqualTo(TimeZone.getTimeZone("UK"));

      final String dateFormat = database.getSchema().getDateFormat();
      assertThat(dateFormat).isNotNull();
      database.getSchema().setDateFormat("yyyy-MMM-dd");
      assertThat(database.getSchema().getDateFormat()).isEqualTo("yyyy-MMM-dd");

      final String dateTimeFormat = database.getSchema().getDateTimeFormat();
      assertThat(dateTimeFormat).isNotNull();
      database.getSchema().setDateTimeFormat("yyyy-MMM-dd HH:mm:ss");
      assertThat(database.getSchema().getDateTimeFormat()).isEqualTo("yyyy-MMM-dd HH:mm:ss");

      final String encoding = database.getSchema().getEncoding();
      assertThat(encoding).isNotNull();
      database.getSchema().setEncoding("UTF-8");
      assertThat(database.getSchema().getEncoding()).isEqualTo("UTF-8");
    });
  }
}
