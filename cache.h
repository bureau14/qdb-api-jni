#pragma once

#include <jni.h>

class cache {
 public:
  jclass qdbTimeSeriesValueClass;
  jclass qdbTimeSeriesValueTypeClass;
  jclass qdbTimeSeriesRowClass;
  jclass qdbTimeSeriesTimespecClass;

  cache(JNIEnv *);
};
