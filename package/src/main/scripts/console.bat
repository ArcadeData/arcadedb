@echo off
rem
rem Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
rem
rem Licensed under the Apache License, Version 2.0 (the "License");
rem you may not use this file except in compliance with the License.
rem You may obtain a copy of the License at
rem
rem     http://www.apache.org/licenses/LICENSE-2.0
rem
rem Unless required by applicable law or agreed to in writing, software
rem distributed under the License is distributed on an "AS IS" BASIS,
rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
rem See the License for the specific language governing permissions and
rem limitations under the License.


echo
echo  █████╗ ██████╗  ██████╗ █████╗ ██████╗ ███████╗██████╗ ██████╗
echo ██╔══██╗██╔══██╗██╔════╝██╔══██╗██╔══██╗██╔════╝██╔══██╗██╔══██╗
echo ███████║██████╔╝██║     ███████║██║  ██║█████╗  ██║  ██║██████╔╝
echo ██╔══██║██╔══██╗██║     ██╔══██║██║  ██║██╔══╝  ██║  ██║██╔══██╗
echo ██║  ██║██║  ██║╚██████╗██║  ██║██████╔╝███████╗██████╔╝██████╔╝
echo ╚═╝  ╚═╝╚═╝  ╚═╝ ╚═════╝╚═╝  ╚═╝╚═════╝ ╚══════╝╚═════╝ ╚═════╝
echo PLAY WITH DATA                                    arcadedb.com

rem Guess ARCADEDB_HOME if not defined
set CURRENT_DIR=%cd%

if exist "%JAVA_HOME%\bin\java.exe" goto setJavaHome
set JAVA="java"
goto okJava

:setJavaHome
set JAVA="%JAVA_HOME%\bin\java"

:okJava
if not "%ARCADEDB_HOME%" == "" goto gotHome
set ARCADEDB_HOME=%CURRENT_DIR%
if exist "%ARCADEDB_HOME%\bin\console.bat" goto okHome
cd ..
set ARCADEDB_HOME=%cd%
cd %CURRENT_DIR%

:gotHome
if exist "%ARCADEDB_HOME%\bin\console.bat" goto okHome
echo The ARCADEDB_HOME environment variable is not defined correctly
echo This environment variable is needed to run this program
goto end

:okHome
rem Get remaining unshifted command line arguments and save them in the
set CMD_LINE_ARGS=

:setArgs
if ""%1""=="""" goto doneSetArgs
set CMD_LINE_ARGS=%CMD_LINE_ARGS% %1
shift
goto setArgs

:doneSetArgs

set JAVA_OPTS_SCRIPT=-XX:+HeapDumpOnOutOfMemoryError -Djava.awt.headless=true -Dfile.encoding=UTF8

rem ARCADEDB memory options, default uses the available RAM. To set it to a specific value, like 2GB of heap, use "-Xms2G -Xmx2G"
set ARCADEDB_OPTS_MEMORY=

set ARCADEDB_SETTINGS=-Xmx1024m -Djna.nosys=true -Djava.util.logging.config.file="%ARCADEDB_HOME%\config\orientdb-client-log.properties" -Djava.awt.headless=true

call %JAVA% -client %JAVA_OPTS% %ARCADEDB_OPTS_MEMORY% %JAVA_OPTS_SCRIPT% %ARCADEDB_JMX% %ARCADEDB_SETTINGS% -cp "%ARCADEDB_HOME%\lib\*" com.arcadedb.console.Console %CMD_LINE_ARGS%

:end
