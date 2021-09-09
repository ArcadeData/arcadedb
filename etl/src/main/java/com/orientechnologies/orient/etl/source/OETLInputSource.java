/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (info(-at-)orientdb.com)
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.orientechnologies.orient.etl.source;

import com.orientechnologies.orient.core.record.impl.ODocument;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Reader;

/** ETL Source that reads from System.in */
public class OETLInputSource extends OETLAbstractSource {
  protected final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

  @Override
  public ODocument getConfiguration() {
    return new ODocument().fromJSON("{}");
  }

  @Override
  public String getUnit() {
    return "bytes";
  }

  @Override
  public String getName() {
    return "input";
  }

  @Override
  public Reader read() {
    return reader;
  }
}
