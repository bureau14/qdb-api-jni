if exist build (
    rd /S /Q build
)
mkdir build
pushd build
cmake -G "Visual Studio 15" .. && cmake --build .
popd
