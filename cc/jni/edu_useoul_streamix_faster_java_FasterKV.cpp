#include <jni.h>
#include <string.h>

#include "edu_useoul_streamix_faster_java_FasterKV.h"
#include "core/faster.h"

using namespace std::chrono_literals;
using namespace FASTER::core;

class ByteArrayKey {
public:
    ByteArrayKey(uint8_t* key, const uint64_t key_length)
      : temp_buffer {key}, key_length_ {key_length} {
    }

    ~ByteArrayKey() {
        if(this->temp_buffer != null) {
            free((void*)temp_buffer);
        }
    }

    inline uint32_t size() const {
        return static_cast<uint32_t>(sizeof(ByteArrayKey) + key_length_);
    }

    inline KeyHash GetHash() const {
        if (this->temp_buffer != NULL) {
            return KeyHash(Utility::GetHashCode(key_, key_length_));
        }
    }

    inline bool operator==(const Key& other) const {
        if (this->key_length_ != other.key_length_) return false;
        if (this->temp_buffer != NULL) {
            return memcmp(temp_buffer, other.buffer(), key_length_) == 0;
        }
        return memcmp(buffer(), other.buffer(), key_length_) == 0;
    }
    inline bool operator!=(const Key& other) const {
        if (this->key_length_ != other.key_length_) return true;
        if (this->temp_buffer != NULL) {
            return memcmp(temp_buffer, other.buffer(), key_length_) != 0;
        }
        return memcmp(buffer(), other.buffer(), key_length_) != 0;
    }

private:
    uint64_t key_length_;
    const uint8_t* temp_buffer;

    inline const uint8_t* buffer() const {
        return reinterpret_cast<const uint8_t*>(this + 1);
    }

    inline uint8_t* buffer() {
        return reinterpret_cast<uint8_t*>(this + 1);
    }
};

class ByteArrayValue {
public:
    ByteArrayValue(uint8_t* value, const uint64_t value_length)
    : temp_buffer {value}, value_length_ {value_length} {
    }

    ~ByteArrayValue() {
        if(this->temp_buffer != null) {
            free((void*)temp_buffer);
        }
    }

    inline uint32_t size() const {
        return static_cast<uint32_t>(sizeof(ByteArrayValue) + value_length_);
    }

    inline uint32_t length() const {
        return static_cast<uint32_t>(value_length_);
    };

    inline uint8_t* getBytes() {
        return temp_buffer;
    }

    inline bool operator==(const Key& other) const {
        if (this->key_length_ != other.key_length_) return false;
        if (this->temp_buffer != NULL) {
            return memcmp(temp_buffer, other.buffer(), key_length_) == 0;
        }
        return memcmp(buffer(), other.buffer(), key_length_) == 0;
    }
    inline bool operator!=(const Key& other) const {
        if (this->key_length_ != other.key_length_) return true;
        if (this->temp_buffer != NULL) {
            return memcmp(temp_buffer, other.buffer(), key_length_) != 0;
        }
        return memcmp(buffer(), other.buffer(), key_length_) != 0;
    }

private:
    uint64_t value_size_;
    const uint8_t *temp_buffer;

    inline const uint16_t* buffer() const {
        return reinterpret_cast<const uint16_t*>(this + 1);
    }
    inline uint16_t* buffer() {
        return reinterpret_cast<uint16_t*>(this + 1);
    }
};

class ReadContext: public IAsyncContext {
public:

    ReadContext(uint8_t* key, uint64_t key_length)
            : key_{key, key_length} {
    }

    ReadContext(const ReadContext& other)
            : key_ { other.key_ } {
    }

    inline const Key& key() const {
        return key_;
    }

    inline void Get(const Value& value) {
        output = value;
    }

    inline void GetAtomic(const Value& value) {
        // Atomic read does not happen.
        ASSERT_TRUE(false);
    }

protected:
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
        return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }
private:
    Key key_;
public:
    Value output;
};

class UpsertContext: public IAsyncContext {

public:

    UpsertContext(uint8_t* key, uint64_t key_length, uint8_t* value, uint64_t value_length)
            : key_ {key, key_length}, value_ {value, value_length} {
    }

    UpsertContext(const UpsertContext& other) {
        key_ = other.key_;
        value_ = other.value_;
    }

    inline const Key& key() const {
        return key_;
    }

    inline uint32_t value_size() {
        return value_.size();
    }

    inline void Put(Value& value) {
        value.value = value_;
    }
    inline bool PutAtomic(Value& value) {
        // Not called
        ASSERT_TRUE(false);
    }

protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
        return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    Key key_;
    Value value_;
};

class DeleteContext: public IAsyncContext {

public:
    DeleteContext(uint8_t* key, uint64_t key_length)
            : key_ {key, key_length} {
    }
    DeleteContext(const DeleteContext& other) {
        key_ = other.key_;
    }
    inline const Key& key() const {
        return key_;
    }

protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext*& context_copy) {
        return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    Key key_;
};

/*
 * Class:     edu_useoul_streamix_faster_java_FasterKV
 * Method:    open
 * Signature: (IILjava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_edu_useoul_streamix_faster_1java_FasterKV_open
        (JNIEnv * jenv, jobject object, jint table_size, jint log_size, jstring filename) {
    FasterKv<Key, Value, FASTER::device::FileSystemDisk>* fasterKv = new FasterKv(table_size, log_size, filename);
    fasterKv->StartSession();
    return reinterpret_cast<jlong>(fasterKv);
}

/*
 * Class:     edu_useoul_streamix_faster_java_FasterKV
 * Method:    read
 * Signature: ([B)[B
 */
JNIEXPORT jbyteArray JNICALL Java_edu_useoul_streamix_faster_1java_FasterKV_read
        (JNIEnv *env, jobject object, jlong handle, jbyteArray key) {
    FasterKv* fasterKv = reinterpret_cast<FasterKv*>(handle);
    // Convert jbyteArray to uint8_t array
    uint64_t len = (*env)->GetArrayLength(env, key);
    uint8_t* key_bytes = static_cast<uint8_t*>((*env)->GetByteArrayElements(env, key, NULL));
    auto callback = [](IAsyncContext* ctxt, Status result) {
        CallBackContext<ReadContext> context {ctxt}
    };
    ReadContext context {key_bytes, len};
    Status result = store->read(context, callback, 1);
    jbyteArray javaBytes = (*env)->NewByteArray(env, context.output.length());
    (*env)->SetByteRegion(env, javaBytes, 0, context.output.length(), (jbyte *)context.output.getBytes());
    return jbyteArray;
}

/*
 * Class:     edu_useoul_streamix_faster_java_FasterKV
 * Method:    upsert
 * Signature: ([B[B)[B
 */
JNIEXPORT void JNICALL Java_edu_useoul_streamix_faster_1java_FasterKV_upsert
        (JNIEnv * jenv, jobject object, jlong handle, jbyteArray key, jbyteArray value) {
    FasterKv* fasterKv = reinterpret_cast<FasterKv*>(handle);
    // Convert jbyteArray to uint8_t array
    uint64_t key_len = (*env)->GetArrayLength(env, key);
    uint8_t* key_bytes = static_cast<uint8_t*>((*env)->GetByteArrayElements(env, key, NULL));
    uint32_t value_len = (*env)->GetArrayLength(env, value);
    uint8_t* value_bytes = static_Cast<uint8_t*>((*env)->GetByteArrayElements(env, value, NULL));
    auto callback = [](IAsyncContext* ctxt, Status result) {
        CallBackContext<UpsertContext> context {ctxt}
    };
    UpsertContext context {key_bytes, key_len, value_bytes, value_len};
    Status result = store->upsert(context, callback, 1);
}

/*
 * Class:     edu_useoul_streamix_faster_java_FasterKV
 * Method:    delete
 * Signature: ([B)V
 */
JNIEXPORT void JNICALL Java_edu_useoul_streamix_faster_1java_FasterKV_delete
(JNIEnv * env, jobject object, jbyteArray key) {
   FasterKv* fasterKv = reinterpret_cast<FasterKv*>(handle);
   uint64_t key_len = (*env)->GetArrayLength(env, key);
   uint8_t* key_bytes = static_cast<uint8_t*>((*env)->GetByteArrayElements(env, key, NULL));
   auto callback = [](IAsyncContext* ctxt, Status result) {
       CallBackContext<DeleteContext> context {ctxt}
   };
   DeleteContext context {key_bytes, key_len};
   Status result = store->delete(context, callback, 1);
}

/*
 * Class:     edu_useoul_streamix_faster_java_FasterKV
 * Method:    close
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_edu_useoul_streamix_faster_1java_FasterKV_close
(JNIEnv * env, jobject object, jlong handle) {
    FasterKv* fasterKv = reinterpret_cast<FasterKv*>(handle);
    fasterKv->StopSession();
    delete fasterKv;
}