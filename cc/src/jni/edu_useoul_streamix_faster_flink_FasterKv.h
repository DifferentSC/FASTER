/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class edu_useoul_streamix_faster_flink_FasterKv */

#ifndef _Included_edu_useoul_streamix_faster_flink_FasterKv
#define _Included_edu_useoul_streamix_faster_flink_FasterKv
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     edu_useoul_streamix_faster_flink_FasterKv
 * Method:    open
 * Signature: (IILjava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_edu_useoul_streamix_faster_1flink_FasterKv_open
  (JNIEnv *, jobject, jint, jint, jstring);

/*
 * Class:     edu_useoul_streamix_faster_flink_FasterKv
 * Method:    close
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_edu_useoul_streamix_faster_1flink_FasterKv_close
  (JNIEnv *, jobject, jlong);

/*
 * Class:     edu_useoul_streamix_faster_flink_FasterKv
 * Method:    read
 * Signature: (J[B)[B
 */
JNIEXPORT jbyteArray JNICALL Java_edu_useoul_streamix_faster_1flink_FasterKv_read
  (JNIEnv *, jobject, jlong, jbyteArray);

/*
 * Class:     edu_useoul_streamix_faster_flink_FasterKv
 * Method:    upsert
 * Signature: (J[B[B)V
 */
JNIEXPORT void JNICALL Java_edu_useoul_streamix_faster_1flink_FasterKv_upsert
  (JNIEnv *, jobject, jlong, jbyteArray, jbyteArray);

/*
 * Class:     edu_useoul_streamix_faster_flink_FasterKv
 * Method:    delete
 * Signature: (J[B)V
 */
JNIEXPORT void JNICALL Java_edu_useoul_streamix_faster_1flink_FasterKv_delete
  (JNIEnv *, jobject, jlong, jbyteArray);

#ifdef __cplusplus
}
#endif
#endif