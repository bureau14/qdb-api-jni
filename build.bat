if exist build (
    rd /S /Q build
)
mkdir build
pushd build
cmake -G "Visual Studio 12" .. && cmake --build .
popd
