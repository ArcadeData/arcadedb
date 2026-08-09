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
package com.arcadedb.server.http.ws;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.schema.Property;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5863: {@code WebSocketEventBus} calls {@code record.toJSON(true)} once per change
 * event and broadcasts the resulting JSON to every connected subscriber, with no per-subscriber signal to opt
 * in to the {@code @props} type-hint. It must therefore never appear in the broadcast payload.
 */
class ChangeEventTest {
  private DatabaseFactory factory;
  private Database        database;

  @BeforeEach
  void setUp() {
    final String path = "./target/databases/changeevent5863_" + UUID.randomUUID();
    factory = new DatabaseFactory(path);
    database = factory.create();
    database.getSchema().createDocumentType("Doc");
  }

  @AfterEach
  void tearDown() {
    if (database != null && database.isOpen())
      database.drop();
    if (factory != null)
      factory.close();
  }

  @Test
  void changeEventNeverEmitsPropsHint() {
    database.begin();
    // A dynamic (non-schema) LONG property is lossy through JSON, exactly the shape that leaked @props before the fix.
    final MutableDocument doc = database.newDocument("Doc").set("dynamicLong", 99L);
    doc.save();
    database.commit();

    final String json = new ChangeEvent(ChangeEvent.TYPE.CREATE, doc).toJSON();

    assertThat(json).doesNotContain(Property.PROPERTY_TYPES_PROPERTY);
    assertThat(json).contains("\"dynamicLong\":99");
  }
}
