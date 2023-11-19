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
@setlocal

set ERROR_CODE=0

rem Validations
if not "%JAVA_HOME%"=="" goto OkJHome

rem Look for java executable
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
if exist "%ARCADEDB_HOME%\bin\console.bat" goto okHome
echo The ARCADEDB_HOME environment variable is not defined correctly
echo This environment variable is needed to run this program
goto end

:okHome
echo ARCADEDB console script path = %~dpnx0
echo ARCADEDB home directory      = %ARCADEDB_HOME%

rem Always change directory to HOME directory
cd /d %ARCADEDB_HOME%

rem Get full command line arguments for the batch file
set CMD_LINE_ARGS=%*

set JAVA_OPTS_SCRIPT=-XX:+HeapDumpOnOutOfMemoryError -Djava.awt.headless=true -Dfile.encoding=UTF8 -Dpolyglot.engine.WarnInterpreterOnly=false

"%JAVACMD%" ^
 -client ^
 %JAVA_OPTS% ^
 %JAVA_OPTS_SCRIPT% ^
 %ARCADEDB_OPTS_MEMORY% ^
 %ARCADEDB_JMX% ^
 %ARCADEDB_SETTINGS% ^
 %CMD_LINE_ARGS% ^
 -cp "%ARCADEDB_HOME%\lib\*" ^
 com.arcadedb.console.Console

:end
