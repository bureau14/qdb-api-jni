set(SOURCE_JAR_FILE "${CMAKE_BINARY_DIR}/jni-${QDB_API_VERSION}-sources.jar")

# JAR: source.jar
add_custom_command(
    OUTPUT ${SOURCE_JAR_FILE}
    DEPENDS ${QDB_JAVA_SOURCES} ${QDB_JNI_SOURCES}
    COMMAND jar cvf ${SOURCE_JAR_FILE} ${QDB_JNI_SOURCES}
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

add_custom_target(jni-sources
    ALL
    DEPENDS ${SOURCE_JAR_FILE}
)
