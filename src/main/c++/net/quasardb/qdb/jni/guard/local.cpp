#include <iostream>

#include "../env.h"
#include "local.h"


void
qdb::jni::guard::local_deleter::operator() (jobject ref) noexcept {
  if (ref) {
    std::cout << "deleting reference to jobject: " << ref << std::endl;

    qdb::jni::env env;
    env.instance().DeleteLocalRef(ref);
  }
}
