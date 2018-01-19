#include <cassert>
#include <stdlib.h>
#include <qdb/query.h>

#include "helpers.h"
#include "net_quasardb_qdb_jni_qdb.h"

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_query_1execute(JNIEnv * env, jclass /*thisClass*/, jlong handle,
                                             jstring query, jobject output) {

  qdb_query_result_t * result;

  printf("*NATIVE* executing query!\n");
  fflush(stdout);

  qdb_error_t err = qdb_exp_query((qdb_handle_t)(handle),
                                  StringUTFChars(env, query),
                                  &result);

  if (QDB_SUCCESS(err)) {
    printf("*NATIVE* success!!!!\n");
    fflush(stdout);
  }

  qdb_release((qdb_handle_t)handle, result);
  return err;
}
