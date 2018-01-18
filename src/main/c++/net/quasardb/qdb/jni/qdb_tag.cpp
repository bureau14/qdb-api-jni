#include "net_quasardb_qdb_jni_qdb.h"

#include "helpers.h"
#include <qdb/tag.h>
#include <stdlib.h>

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_attach_1tag(JNIEnv *env, jclass /*thisClass*/, jlong handle,
                                          jstring alias, jstring tag) {
  return qdb_attach_tag((qdb_handle_t)handle, StringUTFChars(env, alias), StringUTFChars(env, tag));
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_has_1tag(JNIEnv *env, jclass /*thisClass*/, jlong handle, jstring alias,
                                       jstring tag) {
  return qdb_has_tag((qdb_handle_t)handle, StringUTFChars(env, alias), StringUTFChars(env, tag));
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_detach_1tag(JNIEnv *env, jclass /*thisClass*/, jlong handle,
                                          jstring alias, jstring tag) {
  return qdb_detach_tag((qdb_handle_t)handle, StringUTFChars(env, alias), StringUTFChars(env, tag));
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_get_1tags(JNIEnv *env, jclass /*thisClass*/, jlong handle, jstring alias,
                                        jobject tags) {
  const char **nativeTags = NULL;
  size_t tagCount = 0;
  qdb_error_t err =
      qdb_get_tags((qdb_handle_t)handle, StringUTFChars(env, alias), &nativeTags, &tagCount);
  setStringArray(env, tags, nativeTags, tagCount);
  qdb_release((qdb_handle_t)handle, nativeTags);
  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_tag_1iterator_1begin(JNIEnv *env, jclass /*thisClass*/, jlong handle,
                                                   jstring alias, jobject iterator) {
  qdb_const_tag_iterator_t *nativeIterator = new qdb_const_tag_iterator_t;
  qdb_error_t err =
      qdb_tag_iterator_begin((qdb_handle_t)handle, StringUTFChars(env, alias), nativeIterator);
  if (QDB_SUCCESS(err)) {
    setLong(env, iterator, (jlong)nativeIterator);
  } else {
    delete nativeIterator;
    setLong(env, iterator, 0);
  }
  return err;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_tag_1iterator_1next(JNIEnv * /*env*/, jclass /*thisClass*/, jlong iterator) {
  return qdb_tag_iterator_next((qdb_const_tag_iterator_t *)iterator);
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_tag_1iterator_1close(JNIEnv * /*env*/, jclass /*thisClass*/, jlong iterator) {
  qdb_error_t err = qdb_tag_iterator_close((qdb_const_tag_iterator_t *)iterator);
  delete (qdb_const_tag_iterator_t *)iterator;
  return err;
}

JNIEXPORT jstring JNICALL
Java_net_quasardb_qdb_jni_qdb_tag_1iterator_1alias(JNIEnv *env, jclass /*thisClass*/, jlong iterator) {
  if (iterator)
    return env->NewStringUTF(((qdb_const_tag_iterator_t *)iterator)->alias);
  else
    return NULL;
}

JNIEXPORT jint JNICALL
Java_net_quasardb_qdb_jni_qdb_tag_1iterator_1type(JNIEnv * /*env*/, jclass /*thisClass*/, jlong iterator) {
  if (iterator)
    return ((qdb_const_tag_iterator_t *)iterator)->type;
  else
    return qdb_entry_uninitialized;
}
