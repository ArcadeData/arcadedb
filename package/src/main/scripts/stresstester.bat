@echo off
rem
rem Copyright (c) Arcade Analytics LTD (https://www.arcadeanalytics.com)
rem
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
if exist "%ARCADEDB_HOME%\bin\stresstester.bat" goto okHome
cd ..
set ARCADEDB_HOME=%cd%
cd %CURRENT_DIR%

:gotHome
if exist "%ARCADEDB_HOME%\bin\stresstester.bat" goto okHome
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

call %JAVA% -client -cp "%ARCADEDB_HOME%\lib\*;%ARCADEDB_HOME%\plugins\*" com.arcadedb.console.stresstest.StressTester %CMD_LINE_ARGS%

:end
