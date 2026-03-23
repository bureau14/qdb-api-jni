if exist build (
    rd /S /Q build
)
mkdir build
pushd build
cmake -G "Visual Studio 16 2019" .. && cmake --build .
popd
