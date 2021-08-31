/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.importer;

import com.arcadedb.utility.FileUtils;

import java.util.HashMap;
import java.util.Map;

public class ImporterSettings {
  String  database = "./databases/imported";
  boolean wal      = false;

  String documents;
  String documentsFileType;
  String documentsDelimiter;
  String documentsHeader;
  Long   documentsSkipEntries      = null;
  String documentTypeName          = "Document";
  String documentPropertiesInclude = "*";

  String vertices;
  String verticesFileType;
  String verticesDelimiter;
  String verticesHeader;
  Long   verticesSkipEntries     = null;
  String vertexTypeName          = "Node";
  String vertexPropertiesInclude = "*";
  long   expectedVertices        = 0l;

  String edges;
  String edgesFileType;
  String edgesDelimiter;
  String edgesHeader;
  Long   edgesSkipEntries      = null;
  String edgeTypeName          = "Relationship";
  String edgePropertiesInclude = "*";
  public long expectedEdges       = 0l;
  public long maxRAMIncomingEdges = 256 * 1024 * 1024; // 256MB
  String  edgeFromField     = null;
  String  edgeToField       = null;
  boolean edgeBidirectional = true;

  String  typeIdProperty         = null;
  boolean typeIdPropertyIsUnique = false;
  String  typeIdType             = "String";
  int     parallel               = 1;
  boolean forceDatabaseCreate;
  boolean trimText               = true;
  long    analysisLimitBytes     = 100000;
  long    analysisLimitEntries   = 10000;
  long    parsingLimitBytes;
  long    parsingLimitEntries;
  public int commitEvery = 5000;

  final Map<String, String> options = new HashMap<>();

  public ImporterSettings() {
    parallel = Runtime.getRuntime().availableProcessors() / 2 - 1;
    if (parallel < 1)
      parallel = 1;
  }

  protected void parseParameters(final String[] args) {
    if (args != null)
      for (int i = 0; i < args.length - 1; i += 2)
        parseParameter(args[i].substring(1), args[i + 1]);
  }

  public void parseParameter(final String name, final String value) {
    if ("database".equals(name))
      database = value;
    else if ("forceDatabaseCreate".equals(name))
      forceDatabaseCreate = Boolean.parseBoolean(value);
    else if ("wal".equals(name))
      wal = Boolean.parseBoolean(value);
    else if ("commitEvery".equals(name))
      commitEvery = Integer.parseInt(value);
    else if ("parallel".equals(name))
      parallel = Integer.parseInt(value);
    else if ("typeIdProperty".equals(name))
      typeIdProperty = value;
    else if ("typeIdUnique".equals(name))
      typeIdPropertyIsUnique = Boolean.parseBoolean(value);
    else if ("typeIdType".equals(name))
      typeIdType = value;
    else if ("trimText".equals(name))
      trimText = Boolean.parseBoolean(value);
    else if ("analysisLimitBytes".equals(name))
      analysisLimitBytes = FileUtils.getSizeAsNumber(value);
    else if ("analysisLimitEntries".equals(name))
      analysisLimitEntries = Long.parseLong(value);
    else if ("parsingLimitBytes".equals(name))
      parsingLimitBytes = FileUtils.getSizeAsNumber(value);
    else if ("parsingLimitEntries".equals(name))
      parsingLimitEntries = Long.parseLong(value);

      // DOCUMENT SETTINGS

    else if ("documents".equals(name))
      documents = value;
    else if ("documentsFileType".equals(name))
      documentsFileType = value;
    else if ("documentsDelimiter".equals(name))
      documentsDelimiter = value;
    else if ("documentsHeader".equals(name))
      documentsHeader = value;
    else if ("documentsSkipEntries".equals(name))
      documentsSkipEntries = Long.parseLong(value);
    else if ("documentPropertiesInclude".equals(name))
      documentPropertiesInclude = value;
    else if ("documentType".equals(name))
      documentTypeName = value;

      // VERTICES SETTINGS

    else if ("vertices".equals(name))
      vertices = value;
    else if ("verticesFileType".equals(name))
      verticesFileType = value;
    else if ("verticesDelimiter".equals(name))
      verticesDelimiter = value;
    else if ("verticesHeader".equals(name))
      verticesHeader = value;
    else if ("verticesSkipEntries".equals(name))
      verticesSkipEntries = Long.parseLong(value);
    else if ("expectedVertices".equals(name))
      expectedVertices = Integer.parseInt(value);
    else if ("vertexType".equals(name))
      vertexTypeName = value;
    else if ("vertexPropertiesInclude".equals(name))
      vertexPropertiesInclude = value;

      // EDGES SETTINGS

    else if ("edges".equals(name))
      edges = value;
    else if ("edgesFileType".equals(name))
      edgesFileType = value;
    else if ("edgesDelimiter".equals(name))
      edgesDelimiter = value;
    else if ("edgesHeader".equals(name))
      edgesHeader = value;
    else if ("edgesSkipEntries".equals(name))
      edgesSkipEntries = Long.parseLong(value);
    else if ("expectedEdges".equals(name))
      expectedEdges = Integer.parseInt(value);
    else if ("maxRAMIncomingEdges".equals(name))
      maxRAMIncomingEdges = Long.parseLong(value);
    else if ("edgeType".equals(name))
      edgeTypeName = value;
    else if ("edgePropertiesInclude".equals(name))
      edgePropertiesInclude = value;
    else if ("edgeFromField".equals(name))
      edgeFromField = value;
    else if ("edgeToField".equals(name))
      edgeToField = value;
    else if ("edgeBidirectional".equals(name))
      edgeBidirectional = Boolean.parseBoolean(value);
    else
      // ADDITIONAL OPTIONS
      options.put(name, value);
  }
}
