@echo off
setlocal

set "ROOT=%~dp0"
if "%ROOT:~-1%"=="\" set "ROOT=%ROOT:~0,-1%"

set "JAVA_HOME=C:\Java\jdk-21.0.2"
set "MAVEN_HOME=C:\Java\apache-maven-3.9.14"
set "PATH=%JAVA_HOME%\bin;%MAVEN_HOME%\bin;%PATH%"

echo Using JAVA_HOME=%JAVA_HOME%
echo Using MAVEN_HOME=%MAVEN_HOME%

echo [1/5] Checking java...
java -version
if errorlevel 1 exit /b 1

echo [2/5] Checking javac...
javac -version
if errorlevel 1 exit /b 1

echo [3/5] Checking Maven...
call mvn -version
if errorlevel 1 exit /b 1

echo [4/5] Generating JNI headers with Maven...
call mvn -f "%ROOT%\pom.xml" compile
if errorlevel 1 exit /b 1

if not exist "%ROOT%\target\headers\net_quasardb_qdb_jni_qdb.h" (
    echo Expected JNI header was not generated:
    echo   %ROOT%\target\headers\net_quasardb_qdb_jni_qdb.h
    exit /b 1
)

echo [5/5] Configuring CMake...
if not exist "%ROOT%\build" mkdir "%ROOT%\build"
pushd "%ROOT%\build"

cmake -G "Visual Studio 17 2022" .. || exit /b 1
REM cmake --build . || exit /b 1

popd
endlocal
