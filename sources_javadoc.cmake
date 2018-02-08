set(JAVADOC_OUTPUT "${CMAKE_BINARY_DIR}/javadoc/")

# JAR: source.jar
add_custom_command(
  OUTPUT ${JAVADOC_OUTPUT}
  DEPENDS ${SOURCE_JAR_FILE}
  COMMAND javadoc -public -d ${JAVADOC_OUTPUT} -sourcepath src/main/java -subpackages net.quasardb.qdb net.quasardb.qdb.exception net.quasardb.qdb.ts -exclude net.quasardb.qdb.jni
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

add_custom_target(jni-javadoc
    ALL
    DEPENDS ${JAVADOC_OUTPUT}
)
