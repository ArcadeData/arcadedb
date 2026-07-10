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
package com.arcadedb.remote.grpc;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.remote.grpc.utils.ProtoUtils;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.PrintStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5050 (H1): {@code ProtoUtils.toProtoRecord} must not write to {@code System.out}. A
 * leftover {@code System.out.print} used to fire for every property of every record on the client write path, both
 * flooding stdout and leaking property values.
 */
class Issue5050ProtoUtilsNoStdoutTest {

  @TempDir
  Path tempDir;

  @Test
  void toProtoRecordDoesNotWriteToStdout() {
    final String dbPath = tempDir.resolve("issue5050-protoutils").toString();

    try (final DatabaseFactory factory = new DatabaseFactory(dbPath)) {
      final Database db = factory.create();
      try {
        db.transaction(() -> {
          final DocumentType type = db.getSchema().createDocumentType("Person");
          type.createProperty("name", com.arcadedb.schema.Type.STRING);
          type.createProperty("age", com.arcadedb.schema.Type.INTEGER);

          final MutableDocument doc = db.newDocument("Person");
          doc.set("name", "secret-value");
          doc.set("age", 42);
          doc.save();

          final PrintStream originalOut = System.out;
          final ByteArrayOutputStream captured = new ByteArrayOutputStream();
          try {
            System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
            ProtoUtils.toProtoRecord(doc);
          } finally {
            System.setOut(originalOut);
          }

          assertThat(captured.toString(StandardCharsets.UTF_8))
              .as("toProtoRecord must not print anything to stdout")
              .isEmpty();
        });
      } finally {
        db.drop();
      }
    }
  }
}
