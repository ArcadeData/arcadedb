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
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_USERTYPE_VISIBILITY_PUBLIC=true */
package com.arcadedb.query.sql.parser;

import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ResultSet;

import java.util.*;

public class TruncateBucketStatement extends DDLStatement {

  public Identifier bucketName;
  public PInteger   bucketNumber;
  public boolean    unsafe = false;

  public TruncateBucketStatement(final int id) {
    super(id);
  }

  @Override
  public ResultSet executeDDL(final CommandContext context) {
//    ODatabaseDocumentAbstract database = (ODatabaseDocumentAbstract) context.getDatabase();
//    OInternalResultSet rs = new OInternalResultSet();
//
//    Integer bucketId = null;
//    if (clusterNumber != null) {
//      bucketId = clusterNumber.getValue().intValue();
//    } else {
//      bucketId = database.getClusterIdByName(bucketName.getStringValue());
//    }
//
//    if (bucketId < 0) {
//      throw new ODatabaseException("Cluster with name " + bucketName + " does not exist");
//    }
//
//    final OSchema schema = database.getMetadata().getSchema();
//    final OClass typez = schema.getClassByClusterId(bucketId);
//    if (typez == null) {
//      final OStorage storage = database.getStorage();
//      final com.orientechnologies.orient.core.storage.OCluster bucket = storage.getClusterById(bucketId);
//
//      if (bucket == null) {
//        throw new ODatabaseException("Cluster with name " + bucketName + " does not exist");
//      }
//
//      try {
//        database.checkForClusterPermissions(bucket.getName());
//        bucket.truncate();
//      } catch (IOException ioe) {
//        throw OException.wrapException(new ODatabaseException("Error during truncation of bucket with name " + bucketName), ioe);
//      }
//    } else {
//      String name = database.getClusterNameById(bucketId);
//      typez.truncateCluster(name);
//    }
//
//    OResultInternal result = new OResultInternal();
//    result.setProperty("operation", "truncate cluster");
//    if (bucketName != null) {
//      result.setProperty("bucketName", bucketName.getStringValue());
//    }
//    result.setProperty("bucketId", bucketId);
//
//    rs.add(result);
//    return rs;
    throw new UnsupportedOperationException();
  }

  @Override
  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    builder.append("TRUNCATE BUCKET ");
    if (bucketName != null) {
      bucketName.toString(params, builder);
    } else if (bucketNumber != null) {
      bucketNumber.toString(params, builder);
    }
    if (unsafe) {
      builder.append(" UNSAFE");
    }
  }

  @Override
  public TruncateBucketStatement copy() {
    final TruncateBucketStatement result = new TruncateBucketStatement(-1);
    result.bucketName = bucketName == null ? null : bucketName.copy();
    result.bucketNumber = bucketNumber == null ? null : bucketNumber.copy();
    result.unsafe = unsafe;
    return result;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final TruncateBucketStatement that = (TruncateBucketStatement) o;

    if (unsafe != that.unsafe)
      return false;
    if (!Objects.equals(bucketName, that.bucketName))
      return false;
    return Objects.equals(bucketNumber, that.bucketNumber);
  }

  @Override
  public int hashCode() {
    int result = bucketName != null ? bucketName.hashCode() : 0;
    result = 31 * result + (bucketNumber != null ? bucketNumber.hashCode() : 0);
    result = 31 * result + (unsafe ? 1 : 0);
    return result;
  }
}
/* JavaCC - OriginalChecksum=301f993f6ba2893cb30c8f189674b974 (do not edit this line) */
