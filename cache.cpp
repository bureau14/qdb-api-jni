#include <assert.h>
#include <stdio.h>

#include "net_quasardb_qdb_jni_qdb.h"
#include "cache.h"

void
cacheClass(JNIEnv * env, char const * alias, jclass * output) {
  jclass localOutput = env->FindClass(alias);
  assert(localOutput != NULL);

  jclass globalOutput = reinterpret_cast<jclass> (env->NewGlobalRef(localOutput));
  assert(globalOutput != NULL);
  *output = globalOutput;
}

cache::cache(JNIEnv * env) {
  printf("*NATIVE* cache::cache(JNIEnv *) called!!\n");
  fflush(stdout);

  cacheClass(env, "net/quasardb/qdb/QdbTimeSeriesRow", &this->qdbTimeSeriesRowClass);
  cacheClass(env, "net/quasardb/qdb/QdbTimeSeriesValue", &this->qdbTimeSeriesValueClass);
  cacheClass(env, "net/quasardb/qdb/QdbTimeSeriesValue$Type", &this->qdbTimeSeriesValueTypeClass);
  cacheClass(env, "net/quasardb/qdb/QdbTimespec", &this->qdbTimeSeriesTimespecClass);

  assert(this->qdbTimeSeriesRowClass != NULL);
  assert(this->qdbTimeSeriesValueClass != NULL);
  assert(this->qdbTimeSeriesValueTypeClass != NULL);
  assert(this->qdbTimeSeriesTimespecClass != NULL);
}
