/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class edu_useoul_streamix_faster_java_FasterKV */

#ifndef _Included_edu_useoul_streamix_faster_java_FasterKV
#define _Included_edu_useoul_streamix_faster_java_FasterKV
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     edu_useoul_streamix_faster_java_FasterKV
 * Method:    open
 * Signature: (IILjava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_edu_useoul_streamix_faster_1java_FasterKV_open
  (JNIEnv *, jobject, jint, jint, jstring);

/*
 * Class:     edu_useoul_streamix_faster_java_FasterKV
 * Method:    read
 * Signature: (J[B)[B
 */
JNIEXPORT jbyteArray JNICALL Java_edu_useoul_streamix_faster_1java_FasterKV_read
  (JNIEnv *, jobject, jlong, jbyteArray);

/*
 * Class:     edu_useoul_streamix_faster_java_FasterKV
 * Method:    upsert
 * Signature: (J[B[B)[B
 */
JNIEXPORT void JNICALL Java_edu_useoul_streamix_faster_1java_FasterKV_upsert
  (JNIEnv *, jobject, jlong, jbyteArray, jbyteArray);

/*
 * Class:     edu_useoul_streamix_faster_java_FasterKV
 * Method:    delete
 * Signature: (J[B)V
 */
JNIEXPORT void JNICALL Java_edu_useoul_streamix_faster_1java_FasterKV_delete
  (JNIEnv *, jobject, jlong, jbyteArray);

/*
 * Class:     edu_useoul_streamix_faster_java_FasterKV
 * Method:    close
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_edu_useoul_streamix_faster_1java_FasterKV_close
  (JNIEnv *, jobject, jlong);

#ifdef __cplusplus
}
#endif
#endif