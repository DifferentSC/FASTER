#include <jni.h>
#include <cstring>

#include "edu_useoul_streamix_faster_java_FasterKV.h"
#include "core/faster.h"

using namespace std;
using namespace FASTER::core;

class ByteArrayKey {
public:
    ByteArrayKey(const jbyte *key, const uint64_t key_length)
            : temp_buffer{key}, key_length_{key_length} {
    }

    ~ByteArrayKey() {
        if (this->temp_buffer != nullptr) {
            free((void *) temp_buffer);
        }
    }

    inline uint32_t size() const {
        return static_cast<uint32_t>(sizeof(ByteArrayKey) + key_length_);
    }

    inline KeyHash GetHash() const {
        if (this->temp_buffer != nullptr) {
            return KeyHash(Utility::HashBytes(temp_buffer, static_cast<size_t>(key_length_)));
        }
    }

    inline bool operator==(const ByteArrayKey &other) const {
        if (this->key_length_ != other.key_length_) return false;
        if (this->temp_buffer != nullptr) {
            return memcmp(temp_buffer, other.buffer(), key_length_) == 0;
        }
        return memcmp(buffer(), other.buffer(), key_length_) == 0;
    }

    inline bool operator!=(const ByteArrayKey &other) const {
        if (this->key_length_ != other.key_length_) return true;
        if (this->temp_buffer != nullptr) {
            return memcmp(temp_buffer, other.buffer(), key_length_) != 0;
        }
        return memcmp(buffer(), other.buffer(), key_length_) != 0;
    }

private:
    uint64_t key_length_;
    const jbyte *temp_buffer;

    inline const jbyte *buffer() const {
        return reinterpret_cast<const jbyte *>(this + 1);
    }

    inline jbyte *buffer() {
        return reinterpret_cast<jbyte *>(this + 1);
    }
};

class ByteArrayValue {
public:
    ByteArrayValue(const jbyte *value, const uint64_t value_length)
            : temp_buffer{value}, value_length_{value_length} {
    }

    ~ByteArrayValue() {
        if (this->temp_buffer != nullptr) {
            free((void *) temp_buffer);
        }
    }

    inline uint32_t size() const {
        return static_cast<uint32_t>(sizeof(ByteArrayValue) + value_length_);
    }

    inline uint32_t length() const {
        return static_cast<uint32_t>(value_length_);
    };

    inline bool operator==(const ByteArrayValue &other) const {
        if (this->value_length_ != other.value_length_) return false;
        if (this->temp_buffer != nullptr) {
            return memcmp(temp_buffer, other.buffer(), value_length_) == 0;
        }
        return memcmp(buffer(), other.buffer(), value_length_) == 0;
    }

    inline bool operator!=(const ByteArrayValue &other) const {
        if (this->value_length_ != other.value_length_) return true;
        if (this->temp_buffer != nullptr) {
            return memcmp(temp_buffer, other.buffer(), value_length_) != 0;
        }
        return memcmp(buffer(), other.buffer(), value_length_) != 0;
    }

    inline const jbyte* getBuffer() const {
        return temp_buffer;
    }

    friend class ReadContext;
    friend class UpsertContext;

private:
    uint64_t value_length_;
    const jbyte *temp_buffer;

    inline const jbyte *buffer() const {
        return reinterpret_cast<const jbyte *>(this + 1);
    }

    inline jbyte *buffer() {
        return reinterpret_cast<jbyte *>(this + 1);
    }
};

class ReadContext : public IAsyncContext {
public:

    typedef ByteArrayKey key_t;
    typedef ByteArrayValue value_t;

    ReadContext(jbyte *key, uint64_t key_length)
            : key_{key, key_length}, output{nullptr, 0} {
    }

    ReadContext(const ReadContext &other)
            : key_{other.key_}, output{other.output} {
    }

    inline const ByteArrayKey &key() const {
        return key_;
    }

    inline void Get(const ByteArrayValue &value) {
        output.value_length_ = value.value_length_;
        output.temp_buffer = value.buffer();
    }

    inline void GetAtomic(const ByteArrayValue &value) {
        // Atomic read does not happen.
    }

protected:
    Status DeepCopy_Internal(IAsyncContext *&context_copy) {
        return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    ByteArrayKey key_;
public:
    ByteArrayValue output;
};

class UpsertContext : public IAsyncContext {

public:

    typedef ByteArrayKey key_t;
    typedef ByteArrayValue value_t;

    UpsertContext(jbyte *key, uint64_t key_length, jbyte *value, uint64_t value_length)
            : key_{key, key_length}, value_{value, value_length} {
    }

    UpsertContext(const UpsertContext &other)
            : key_{other.key_}, value_{other.value_} {
    }

    inline const ByteArrayKey &key() const {
        return key_;
    }

    inline uint32_t value_size() const {
        return value_.size();
    }

    inline void Put(ByteArrayValue &value) {
        value.value_length_ = value_.value_length_;
        memcpy(value.buffer(), value_.buffer(), value_.value_length_);
    }

    inline bool PutAtomic(ByteArrayValue &value) {
        // Not called.
    }

protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext *&context_copy) {
        return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    ByteArrayKey key_;
    ByteArrayValue value_;
};

class DeleteContext : public IAsyncContext {

public:
    typedef ByteArrayKey key_t;
    typedef ByteArrayValue value_t;

    DeleteContext(jbyte *key, uint64_t key_length, const jbyte *value, uint64_t value_length)
            : key_{key, key_length}, value_{value, value_length} {
    }

    DeleteContext(const DeleteContext &other)
            : key_{other.key_}, value_{other.value_} {
    }

    inline const ByteArrayKey &key() const {
        return key_;
    }

    inline uint32_t value_size() const {
        return value_.size();
    }

protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext *&context_copy) {
        return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    ByteArrayKey key_;
    ByteArrayValue value_;
};

typedef FASTER::environment::QueueIoHandler handler_t;
typedef FASTER::device::FileSystemDisk<handler_t, 33554432L> disk_t;

/*
 * Class:     edu_useoul_streamix_faster_java_FasterKv
 * Method:    open
 * Signature: (IILjava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_edu_useoul_streamix_faster_1java_FasterKv_open
        (JNIEnv *env, jobject object, jint table_size, jint log_size, jstring jfilename) {
    const char *cstr = env->GetStringUTFChars(jfilename, nullptr);
    std::string filename = std::string(cstr);
    FasterKv<ByteArrayKey, ByteArrayValue, disk_t> *fasterKv
            = new FasterKv<ByteArrayKey, ByteArrayValue, disk_t>(
                    static_cast<uint64_t>(table_size),
                    static_cast<uint64_t>(log_size),
                    filename);
    fasterKv->StartSession();
    return reinterpret_cast<jlong>(fasterKv);
}

/*
 * Class:     edu_useoul_streamix_faster_java_FasterKv
 * Method:    read
 * Signature: ([B)[B
 */
JNIEXPORT jbyteArray JNICALL Java_edu_useoul_streamix_faster_1java_FasterKv_read
        (JNIEnv *env, jobject object, jlong handle, jbyteArray key) {
    auto fasterKv = reinterpret_cast<FasterKv<ByteArrayKey, ByteArrayValue, disk_t> *>(handle);
    // Convert jbyteArray to uint8_t array
    uint64_t key_len = env->GetArrayLength(key);
    jbyte *key_bytes = env->GetByteArrayElements(key, nullptr);
    auto callback = [](IAsyncContext *ctxt, Status result) {
        CallbackContext<ReadContext> context{ctxt};
    };
    ReadContext context{key_bytes, key_len};
    Status result = fasterKv->Read(context, callback, 1);
    jbyteArray javaBytes = env->NewByteArray(context.output.length());
    env->SetByteArrayRegion(javaBytes, 0, context.output.length(), context.output.getBuffer());
    return javaBytes;
}

/*
 * Class:     edu_useoul_streamix_faster_java_FasterKV
 * Method:    upsert
 * Signature: ([B[B)[B
 */
JNIEXPORT void JNICALL Java_edu_useoul_streamix_faster_1java_FasterKv_upsert
        (JNIEnv *env, jobject object, jlong handle, jbyteArray key, jbyteArray value) {
    auto fasterKv = reinterpret_cast<FasterKv<ByteArrayKey, ByteArrayValue, disk_t> *>(handle);
    // Convert jbyteArray to uint8_t array
    uint64_t key_len = env->GetArrayLength(key);
    jbyte *key_bytes = env->GetByteArrayElements(key, nullptr);
    uint32_t value_len = env->GetArrayLength(value);
    jbyte *value_bytes = env->GetByteArrayElements(value, nullptr);
    auto callback = [](IAsyncContext *ctxt, Status result) {
        CallbackContext<UpsertContext> context{ctxt};
    };
    UpsertContext context{key_bytes, key_len, value_bytes, value_len};
    Status result = fasterKv->Upsert(context, callback, 1);
}

/*
 * Class:     edu_useoul_streamix_faster_java_FasterKV
 * Method:    delete
 * Signature: ([B)V
 */
JNIEXPORT void JNICALL Java_edu_useoul_streamix_faster_1java_FasterKv_delete
        (JNIEnv *env, jobject object, jlong handle, jbyteArray key) {
    auto fasterKv = reinterpret_cast<FasterKv<ByteArrayKey, ByteArrayValue, disk_t> *>(handle);
    uint64_t key_len = env->GetArrayLength(key);
    jbyte *key_bytes = env->GetByteArrayElements(key, nullptr);

    // Need to read first because it is necessary to get the size of value for DeleteContext.
    auto read_callback = [](IAsyncContext *ctxt, Status result) {
        CallbackContext<ReadContext> context{ctxt};
    };
    ReadContext read_context{key_bytes, key_len};
    fasterKv->Read(read_context, read_callback, 1);

    auto callback = [](IAsyncContext *ctxt, Status result) {
        CallbackContext<DeleteContext> context{ctxt};
    };
    DeleteContext context{key_bytes, key_len, read_context.output.getBuffer(),
                          static_cast<uint64_t>(read_context.output.length())};
    Status result = fasterKv->Delete(context, callback, 1);
}

/*
 * Class:     edu_useoul_streamix_faster_java_FasterKv
 * Method:    close
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_edu_useoul_streamix_faster_1java_FasterKV_close
        (JNIEnv *env, jobject object, jlong handle) {
    auto fasterKv = reinterpret_cast<FasterKv<ByteArrayKey, ByteArrayValue, disk_t> *>(handle);
    fasterKv->StopSession();
    delete fasterKv;
}