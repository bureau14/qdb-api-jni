if exist build (
    rd /S /Q build
)
mkdir build
pushd build
cmake -G "Visual Studio 12 Win64" ..
cmake --build .
cpack -V -C Debug -G ZIP .
popd
