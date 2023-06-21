@REM
@REM Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
@REM
@REM Licensed under the Apache License, Version 2.0 (the "License");
@REM you may not use this file except in compliance with the License.
@REM You may obtain a copy of the License at
@REM
@REM     http://www.apache.org/licenses/LICENSE-2.0
@REM
@REM Unless required by applicable law or agreed to in writing, software
@REM distributed under the License is distributed on an "AS IS" BASIS,
@REM WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@REM See the License for the specific language governing permissions and
@REM limitations under the License.
@REM

@echo off
chcp 65001 >nul 2<&1 || goto afterBanner
echo  █████╗ ██████╗  ██████╗ █████╗ ██████╗ ███████╗██████╗ ██████╗
echo ██╔══██╗██╔══██╗██╔════╝██╔══██╗██╔══██╗██╔════╝██╔══██╗██╔══██╗
echo ███████║██████╔╝██║     ███████║██║  ██║█████╗  ██║  ██║██████╔╝
echo ██╔══██║██╔══██╗██║     ██╔══██║██║  ██║██╔══╝  ██║  ██║██╔══██╗
echo ██║  ██║██║  ██║╚██████╗██║  ██║██████╔╝███████╗██████╔╝██████╔╝
echo ╚═╝  ╚═╝╚═╝  ╚═╝ ╚═════╝╚═╝  ╚═╝╚═════╝ ╚══════╝╚═════╝ ╚═════╝
:afterBanner
echo PLAY WITH DATA                             https://arcadedb.com
echo:

@setlocal

set ERROR_CODE=0

rem Validations
if not "%JAVA_HOME%"=="" goto OkJHome

rem Look for java executable on PATH
for %%i in (java.exe) do set "JAVACMD=%%~$PATH:i"
goto checkJCmd

:OkJHome
set "JAVACMD=%JAVA_HOME%\bin\java.exe"

:checkJCmd
if exist "%JAVACMD%" goto chkArcHome

echo The JAVA_HOME environment variable is not defined correctly, >&2
echo this environment variable is needed to run this program. >&2
goto error

:chkArcHome
if not "%ARCADEDB_HOME%" == "" goto gotHome

rem Guess ARCADEDB_HOME if not defined
set "ARCADEDB_HOME=%~dp0"
set "ARCADEDB_HOME=%ARCADEDB_HOME:~0,-5%"

:gotHome
if exist "%ARCADEDB_HOME%\bin\server.bat" goto okHome
echo The ARCADEDB_HOME environment variable is not defined correctly
echo This environment variable is needed to run this program
goto end

:okHome
echo ARCADEDB server script path = %~dpnx0
echo ARCADEDB home directory     = %ARCADEDB_HOME%

rem Always change directory to HOME directory
cd /d %ARCADEDB_HOME%

rem Get remaining unshifted command line arguments and save them in the
set CMD_LINE_ARGS=

:setArgs
if ""%1""=="""" goto doneSetArgs
set CMD_LINE_ARGS=%CMD_LINE_ARGS% %1
shift
goto setArgs

:doneSetArgs

set JAVA_OPTS_SCRIPT=-XX:+HeapDumpOnOutOfMemoryError --add-exports java.management/sun.management=ALL-UNNAMED --add-opens java.base/java.io=ALL-UNNAMED -Dpolyglot.engine.WarnInterpreterOnly=false -Djava.awt.headless=true -Dfile.encoding=UTF8 -Djava.util.logging.config.file=config/arcadedb-log.properties

rem ARCADEDB memory options, default uses the available RAM. To set it to a specific value, like 2GB of heap, use "-Xms2G -Xmx2G"
set ARCADEDB_OPTS_MEMORY=

set ARCADEDB_JMX=-Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.port=9999 -Dcom.sun.management.jmxremote.rmi.port=9998

rem TO DEBUG ARCADEDB SERVER RUN IT WITH THESE OPTIONS:
rem -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=1044
rem AND ATTACH TO THE CURRENT HOST, PORT 1044

"%JAVACMD%" ^
  -server %JAVA_OPTS% ^
  %ARCADEDB_OPTS_MEMORY% ^
  %JAVA_OPTS_SCRIPT% ^
  %ARCADEDB_JMX% ^
  %ARCADEDB_SETTINGS% ^
  -cp "%ARCADEDB_HOME%\lib\*" ^
  %CMD_LINE_ARGS% com.arcadedb.server.ArcadeDBServer

if ERRORLEVEL 1 goto error
goto end

:error
set ERROR_CODE=1

:end
@endlocal & set ERROR_CODE=%ERROR_CODE%
cmd /c exit /b %ERROR_CODE%
