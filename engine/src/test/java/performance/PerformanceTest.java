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
 */
package performance;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.utility.FileUtils;

import java.io.File;
import java.util.logging.Level;

public abstract class PerformanceTest {
  public final static String DATABASE_PATH = "target/databases/performance";

  protected String getDatabasePath() {
    return PerformanceTest.DATABASE_PATH;
  }

  public static void clean() {
    GlobalConfiguration.PROFILE.setValue("high-performance");

    LogManager.instance().setLogger(new Logger() {

      @Override
      public void log(Object iRequester, Level iLevel, String iMessage, Throwable iException, String context, Object... args) {
      }

      @Override
      public void flush() {
      }
    });

    final File dir = new File(PerformanceTest.DATABASE_PATH);
    FileUtils.deleteRecursively(dir);
    dir.mkdirs();
  }
}
