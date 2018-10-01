{ stdenv, cmake, jdk }:

stdenv.mkDerivation rec {

  name = "qdb-api-jni-${version}";
  version = "3.0.0master";

  src = ./.;

  buildInputs = [ cmake jdk ];
  enableParallelBuilding = true;

  meta = with stdenv.lib; {
    homepage = https://quasardb.net/;
    description = "JNI API for QuasarDB";
    license = licenses.bsd3;
    maintainers = [ maintainers.solatis ];
    platforms = platforms.linux;
  };
}
