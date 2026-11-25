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
package com.arcadedb.integration.importer;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.zip.GZIPOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12047: the import report did not say how many edges were dropped for a missing endpoint. The Neo4j importer counted
 * every relationship of the file as {@code skippedEdges} in its vertex pass, created or not, and counted the ones it really
 * dropped as {@code warnings}; the OrientDB importer counted them in a field that never reached the report. Both now report
 * exactly the dropped edges as {@code skippedEdges}, the counter the CSV route reports the same case in.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12047ImportReportSkippedEdgesTest {
  private static final String DB_PATH = "target/databases/issue12047";

  @BeforeEach
  @AfterEach
  void cleanUp() {
    TestHelper.checkActiveDatabases();
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
  }

  @Test
  void neo4jReportsOnlyTheDroppedRelationships() throws Exception {
    final File source = new File("target/issue12047-neo4j.json");
    Files.writeString(source.toPath(), """
        {"type":"node","id":"0","labels":["P"],"properties":{}}
        {"type":"node","id":"1","labels":["P"],"properties":{}}
        {"type":"relationship","id":"0","label":"R","properties":{},"start":{"id":"0","labels":["P"]},"end":{"id":"1","labels":["P"]}}
        {"type":"relationship","id":"1","label":"R","properties":{},"start":{"id":"1","labels":["P"]},"end":{"id":"0","labels":["P"]}}
        {"type":"relationship","id":"2","label":"R","properties":{},"start":{"id":"0","labels":["P"]},"end":{"id":"99","labels":["P"]}}
        {"type":"relationship","id":"3","label":"R","properties":{},"start":{"id":"98","labels":["P"]},"end":{"id":"0","labels":["P"]}}
        {"type":"relationship","id":"4","properties":{},"start":{"id":"0","labels":["P"]},"end":{"id":"1","labels":["P"]}}
        """, StandardCharsets.UTF_8);
    try {
      final Result report = importFile(source);
      assertThat(report.<Long>getProperty("createdEdges")).isEqualTo(2L);
      // MISSING END, MISSING START AND NO LABEL: THE THREE CASES THE EDGE PASS DROPS A RELATIONSHIP IN
      assertThat(report.<Long>getProperty("skippedEdges")).isEqualTo(3L);
      assertThat(report.hasProperty("warnings")).isFalse();
      // EVERY RELATIONSHIP IS EITHER CREATED OR SKIPPED, SO THE REPORT ADDS UP (ISSUE #7488)
      assertThat(report.<Long>getProperty("parsedRecords")).isEqualTo(2L + 2L + 3L);
    } finally {
      source.delete();
    }
  }

  @Test
  void orientDbReportsTheEdgesDroppedForAMissingVertex() throws Exception {
    final File source = new File("target/issue12047-orientdb.json.gz");
    try (final OutputStream out = new GZIPOutputStream(Files.newOutputStream(source.toPath()))) {
      out.write(("""
          {"info":{"name":"t","default-cluster-id":3,"exporter-version":12,"engine-version":"3.1.13","storage-config-version":23,\
          "schema-version":4,"schemaRecordId":"#0:1","indexMgrRecordId":"#0:2"},\
          "clusters":[{"name":"internal","id":0},{"name":"v","id":10},{"name":"e","id":22},{"name":"person","id":34},\
          {"name":"friend","id":37}],\
          "schema":{"version":1,"blob-clusters":[],"classes":[{"name":"V","default-cluster-id":10,"cluster-ids":[10]},\
          {"name":"E","default-cluster-id":22,"cluster-ids":[22]},\
          {"name":"Person","default-cluster-id":34,"cluster-ids":[34],"super-classes":["V"]},\
          {"name":"Friend","default-cluster-id":37,"cluster-ids":[37],"super-classes":["E"]}]},\
          "records":[{"@type":"b","@rid":"#0:0","@version":1,"value":"AAAAAA=="},\
          {"@type":"d","@rid":"#0:1","@version":1,"schemaVersion":4},\
          {"@type":"d","@rid":"#0:2","@version":1,"indexes":[]},\
          {"@type":"d","@rid":"#34:0","@version":1,"@class":"Person","name":"a","out_Friend":["#37:0","#37:1","#37:2"],"@fieldTypes":"out_Friend=g"},\
          {"@type":"d","@rid":"#34:1","@version":1,"@class":"Person","name":"b","in_Friend":["#37:0"],"@fieldTypes":"in_Friend=g"},\
          {"@type":"d","@rid":"#37:0","@version":1,"@class":"Friend","out":"#34:0","in":"#34:1","@fieldTypes":"in=x,out=x"},\
          {"@type":"d","@rid":"#37:1","@version":1,"@class":"Friend","out":"#34:0","in":"#34:99","@fieldTypes":"in=x,out=x"},\
          {"@type":"d","@rid":"#37:2","@version":1,"@class":"Friend","out":"#34:98","in":"#34:0","@fieldTypes":"in=x,out=x"}]}
          """).getBytes(StandardCharsets.UTF_8));
    }
    try {
      final Result report = importFile(source);
      assertThat(report.<Long>getProperty("createdEdges")).isEqualTo(1L);
      assertThat(report.<Long>getProperty("skippedEdges")).isEqualTo(2L);
    } finally {
      source.delete();
    }
  }

  private static Result importFile(final File source) {
    try (final Database db = new DatabaseFactory(DB_PATH).create();
        final ResultSet rs = db.command("sql", "IMPORT DATABASE file://" + source.getAbsolutePath())) {
      return rs.next();
    }
  }
}
