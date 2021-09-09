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

package com.orientechnologies.orient.etl.extractor;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.OETLAbstractComponent;

/** ETL abstract extractor. */
public abstract class OETLAbstractExtractor extends OETLAbstractComponent implements OETLExtractor {
  protected long current = 0;
  protected long total = -1;

  @Override
  public long getProgress() {
    return current;
  }

  @Override
  public long getTotal() {
    return total;
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException("remove()");
  }

  @Override
  public ODocument getConfiguration() {
    return new ODocument().fromJSON("{parameters:[],output:'String'}");
  }
}
