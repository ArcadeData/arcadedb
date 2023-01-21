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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.*;
import java.util.*;

public class SchemaTest extends TestHelper {

  @Test
  public void tesSchemaSettings() {
    database.transaction(() -> {
      final ZoneId zoneId = database.getSchema().getZoneId();
      Assertions.assertNotNull(zoneId);
      database.getSchema().setZoneId(ZoneId.of("America/New_York"));
      Assertions.assertEquals(ZoneId.of("America/New_York"), database.getSchema().getZoneId());

      final TimeZone timeZone = database.getSchema().getTimeZone();
      Assertions.assertNotNull(timeZone);
      database.getSchema().setTimeZone(TimeZone.getTimeZone("UK"));
      Assertions.assertEquals(TimeZone.getTimeZone("UK"), database.getSchema().getTimeZone());

      final String dateFormat = database.getSchema().getDateFormat();
      Assertions.assertNotNull(dateFormat);
      database.getSchema().setDateFormat("yyyy-MMM-dd");
      Assertions.assertEquals("yyyy-MMM-dd", database.getSchema().getDateFormat());

      final String dateTimeFormat = database.getSchema().getDateTimeFormat();
      Assertions.assertNotNull(dateTimeFormat);
      database.getSchema().setDateTimeFormat("yyyy-MMM-dd HH:mm:ss");
      Assertions.assertEquals("yyyy-MMM-dd HH:mm:ss", database.getSchema().getDateTimeFormat());

      final String encoding = database.getSchema().getEncoding();
      Assertions.assertNotNull(encoding);
      database.getSchema().setEncoding("UTF-8");
      Assertions.assertEquals("UTF-8", database.getSchema().getEncoding());
    });
  }
}
