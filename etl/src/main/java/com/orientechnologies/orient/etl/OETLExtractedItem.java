/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.etl;

/** Immutable Object representing extracted item. */
public class OETLExtractedItem {
  public final long num;
  public final Object payload;
  public final boolean finished;

  public OETLExtractedItem(final long iCurrent, final Object iPayload) {
    num = iCurrent;
    payload = iPayload;
    finished = false;
  }

  public OETLExtractedItem(final boolean iFinished) {
    num = 0;
    payload = null;
    finished = iFinished;
  }
}
