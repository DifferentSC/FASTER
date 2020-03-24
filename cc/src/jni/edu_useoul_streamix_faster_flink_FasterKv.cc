#include <jni.h>
#include <cassert>
#include <cstring>
#include <iostream>

#include "edu_useoul_streamix_faster_flink_FasterKv.h"
#include "murmur3.h"
#include "core/faster.h"
#include "device/null_disk.h"

using namespace std;
using namespace FASTER::core;

class GenLock {
public:
    GenLock()
            : control_{ 0 } {
    }
    GenLock(uint64_t control)
            : control_{ control } {
    }
    inline GenLock& operator=(const GenLock& other) {
        control_ = other.control_;
        return *this;
    }

    union {
        struct {
            uint64_t gen_number : 62;
            uint64_t locked : 1;
            uint64_t replaced : 1;
        };
        uint64_t control_;
    };
};
static_assert(sizeof(GenLock) == 8, "sizeof(GenLock) != 8");

class AtomicGenLock {
public:
    AtomicGenLock()
            : control_{ 0 } {
    }
    AtomicGenLock(uint64_t control)
            : control_{ control } {
    }

    inline GenLock load() const {
        return GenLock{ control_.load() };
    }
    inline void store(GenLock desired) {
        control_.store(desired.control_);
    }

    inline bool try_lock(bool& replaced) {
        replaced = false;
        GenLock expected{ control_.load() };
        expected.locked = 0;
        expected.replaced = 0;
        GenLock desired{ expected.control_ };
        desired.locked = 1;

        if(control_.compare_exchange_strong(expected.control_, desired.control_)) {
            return true;
        }
        if(expected.replaced) {
            replaced = true;
        }
        return false;
    }
    inline void unlock(bool replaced) {
        if(!replaced) {
            // Just turn off "locked" bit and increase gen number.
            uint64_t sub_delta = ((uint64_t)1 << 62) - 1;
            control_.fetch_sub(sub_delta);
        } else {
            // Turn off "locked" bit, turn on "replaced" bit, and increase gen number
            uint64_t add_delta = ((uint64_t)1 << 63) - ((uint64_t)1 << 62) + 1;
            control_.fetch_add(add_delta);
        }
    }

private:
    std::atomic<uint64_t> control_;
};

class ByteArrayKey {
public:

    ByteArrayKey()
            : temp_buffer {nullptr}, key_length_{0} {
        //std::cout << "Default ByteArrayKey constructor is called!" << std::endl;
    }

    ByteArrayKey(const jbyte *key, const uint64_t key_length)
            : temp_buffer {key}, key_length_{key_length} {
        //std::cout << "Constructor for ByteArrayKey is called! key length = " << key_length << std::endl;
    }

    ByteArrayKey(const ByteArrayKey& other) {
        //std::cout << "Copy constructor for ByteArrayKey is called! key length = " << other.key_length_ << std::endl;
        key_length_ = other.key_length_;
        temp_buffer = nullptr;
        if (other.temp_buffer == nullptr) {
            memcpy(buffer(), other.buffer(), key_length_);
        } else {
            memcpy(buffer(), other.temp_buffer, key_length_);
        }
    }

    ~ByteArrayKey() {
        // std::cout << "Destructor for ByteArrayKey is called!" << std::endl;
        if (temp_buffer != nullptr) {
            free((void *) temp_buffer);
        }
    }

    inline uint32_t size() const {
        return static_cast<uint32_t>(sizeof(ByteArrayKey) + key_length_);
    }

    inline KeyHash GetHash() const {
        uint32_t hash_value;
        if (this->temp_buffer != nullptr) {
            MurmurHash3_x86_32(static_cast<const void*>(temp_buffer), key_length_, 0, &hash_value);
        } else {
            MurmurHash3_x86_32(static_cast<const void*>(buffer()), key_length_, 0, &hash_value);
        }
        std::cout << "GetHash() is called. Hash = " << hash_value << std::endl;
        return KeyHash(static_cast<uint64_t>(hash_value));
        /*
        if (this->temp_buffer != nullptr) {
            return SimpleHash(temp_buffer);
        } else {
            return SimpleHash(buffer());
        }*/
    }

    inline bool operator==(const ByteArrayKey &other) const {
        // std::cout << "other.key_length_ = " << other.key_length_ << std::endl;
        // std::cout << "is other.temp_buffer == nullptr? " << (other.temp_buffer == nullptr) << std::endl;
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

    inline KeyHash SimpleHash(const jbyte* seq) const {
        uint64_t hash_value =
                seq[0] << 8
                + seq[1] << 7
                + seq[2] << 6
                + seq[3] << 5
                + seq[4] << 4
                + seq[5] << 3
                + seq[6] << 2
                + seq[7];
        return KeyHash(hash_value);
    }

public:
    ByteArrayKey(uint64_t keyLength) : key_length_(keyLength) {}
};

class ByteArrayValue {
public:
    ByteArrayValue()
            : gen_lock_{0}, size_(0), length_(0) {
    }
    /*
    ByteArrayValue(const ByteArrayValue& other)
            : gen_lock_(0), size_(other.size_), length_(other.length_) {
        if (length_ > 0) {
            memcpy(buffer(), other.buffer(), length_);
        }
    }*/

    ~ByteArrayValue() {

    }

    inline uint32_t size() const {
        return size_;
    }

    inline bool operator==(const ByteArrayValue &other) const {
        if (this->length_ != other.length_) return false;
        return memcmp(buffer(), other.buffer(), length_) == 0;
    }

    inline bool operator!=(const ByteArrayValue &other) const {
        if (this->length_ != other.length_) return true;
        return memcmp(buffer(), other.buffer(), length_) != 0;
    }

    inline const jbyte* getBuffer() const {
        return buffer();
    }

    friend class ReadContext;
    friend class UpsertContext;

private:
    AtomicGenLock gen_lock_;
    uint32_t size_;
    uint32_t length_;

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
            : key_{key, key_length}, output(nullptr), length(0) {
    }

    ReadContext(const ReadContext &other)
            : key_{other.key_}, output(nullptr), length(0) {
    }

    ~ReadContext() {
        if (output != nullptr) {
            free((void*)output);
        }
    }

    inline const ByteArrayKey &key() const {
        return key_;
    }

    inline void Get(const ByteArrayValue &value) {
        if (value.length_ != 0) {
            length = value.length_;
            output = (jbyte*) malloc(value.length_);
            memcpy(output, value.buffer(), value.length_);
        } else {
            length = 0;
            output = nullptr;
        }
    }

    inline void GetAtomic(const ByteArrayValue &value) {
        GenLock before, after;
        do {
            before = value.gen_lock_.load();
            // No concurrent read happens - so just use Get()
            if (value.length_ != 0) {
                length = value.length_;
                output = (jbyte *) malloc(value.length_);
                memcpy(output, value.buffer(), value.length_);
            } else {
                length = 0;
                output = nullptr;
            }
            after = value.gen_lock_.load();
        } while(before.gen_number != after.gen_number);
    }

protected:
    Status DeepCopy_Internal(IAsyncContext *&context_copy) {
        return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    ByteArrayKey key_;
public:
    jbyte* output;
    uint64_t length;
};

class UpsertContext : public IAsyncContext {

public:

    typedef ByteArrayKey key_t;
    typedef ByteArrayValue value_t;

    UpsertContext(jbyte *key, uint64_t key_length, jbyte *value, uint64_t value_length)
            : key_{key, key_length}, value_byte_(value), length_(value_length) {
    }

    UpsertContext(const UpsertContext &other)
            : key_{other.key_}, value_byte_(other.value_byte_), length_(other.length_) {
    }

    ~UpsertContext() {

    }

    inline const ByteArrayKey &key() const {
        return key_;
    }

    inline uint32_t value_size() const {
        // std::cout << "value_size() is called. return = " << sizeof(ByteArrayValue) + length_ << std::endl;
        return sizeof(ByteArrayValue) + length_;
    }

    inline void Put(ByteArrayValue &value) {
        value.size_ = sizeof(ByteArrayValue) + length_;
        value.length_ = length_;
        memcpy(value.buffer(), value_byte_, length_);
    }

    inline bool PutAtomic(ByteArrayValue &value) {
        bool replaced;
        while (!value.gen_lock_.try_lock(replaced) && !replaced) {
            std::this_thread::yield();
        }
        if (replaced) {
            return false;
        }
        value.size_ = sizeof(ByteArrayValue) + length_;
        value.length_ = length_;
        memcpy(value.buffer(), value_byte_, length_);
        value.gen_lock_.unlock(false);
        return true;
    }

protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext *&context_copy) {
        return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:

    ByteArrayKey key_;
    jbyte* value_byte_;
    uint64_t length_;
};

class DeleteContext : public IAsyncContext {

public:
    typedef ByteArrayKey key_t;
    typedef ByteArrayValue value_t;

    DeleteContext(jbyte *key, uint64_t key_length, uint64_t value_length)
            : key_{key, key_length}, length_(value_length) {
    }

    DeleteContext(const DeleteContext &other) {
        key_ = other.key_;
        length_ = other.length_;
    }

    inline const ByteArrayKey& key() const {
        return key_;
    }

    inline uint32_t value_size() const {
        return sizeof(ByteArrayValue) + length_;
    }

protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext *&context_copy) {
        return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    ByteArrayKey key_;
    uint64_t length_;
};

typedef FASTER::environment::QueueIoHandler handler_t;
// typedef FASTER::device::FileSystemDisk<handler_t, 1073741824ull> disk_t;
typedef FASTER::device::NullDisk disk_t;

/*
 * Class:     edu_useoul_streamix_faster_flink_FasterKv
 * Method:    open
 * Signature: (IILjava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_edu_useoul_streamix_faster_1flink_FasterKv_open
        (JNIEnv *env, jobject object, jint table_size, jlong log_size, jstring jfilename) {
    const char *cstr = env->GetStringUTFChars(jfilename, nullptr);
    std::string filename = std::string(cstr);
    FasterKv<ByteArrayKey, ByteArrayValue, disk_t> *fasterKv
            = new FasterKv<ByteArrayKey, ByteArrayValue, disk_t>(
                    128,
                    268435456,
                    "");
    fasterKv->StartSession();
    return reinterpret_cast<jlong>(fasterKv);
}

/*
 * Class:     edu_useoul_streamix_faster_flink_FasterKv
 * Method:    read
 * Signature: (J[B)[B
 */
JNIEXPORT jbyteArray JNICALL Java_edu_useoul_streamix_faster_1flink_FasterKv_read
        (JNIEnv *env, jobject object, jlong handle, jbyteArray key) {
    auto fasterKv = reinterpret_cast<FasterKv<ByteArrayKey, ByteArrayValue, disk_t>*>(handle);
    // Convert jbyteArray to uint8_t array
    uint64_t key_len = env->GetArrayLength(key);
    jbyte *key_bytes = env->GetByteArrayElements(key, nullptr);
    jbyte *copied_key_bytes = (jbyte*) malloc(key_len);
    memcpy(copied_key_bytes, key_bytes, key_len);
    auto callback = [](IAsyncContext *ctxt, Status result) {
        std::cout << "Read callback is called!" << endl;
        CallbackContext<ReadContext> context{ctxt};
    };
    ReadContext context{copied_key_bytes, key_len};
    Status result = fasterKv->Read(context, callback, 1);
    if (result == Status::NotFound) {
        return nullptr;
    } if (result == Status::Pending) {
        fasterKv->CompletePending(true);
        std::cout << "Status: Pending" << std::endl;
    } else {
        jbyteArray javaBytes = env->NewByteArray(context.length);
        env->SetByteArrayRegion(javaBytes, 0, context.length, context.output);
        return javaBytes;
    }
}

void InternalDelete(FasterKv<ByteArrayKey, ByteArrayValue, disk_t>* fasterKv, jbyte* key_bytes, uint64_t key_len) {
    jbyte *copied_key_bytes = (jbyte*) malloc(key_len);
    memcpy(copied_key_bytes, key_bytes, key_len);

    // Need to read first because it is necessary to get the size of value for DeleteContext.
    auto read_callback = [](IAsyncContext *ctxt, Status result) {
        CallbackContext<ReadContext> context{ctxt};
    };
    ReadContext read_context{copied_key_bytes, key_len};
    Status read_result = fasterKv->Read(read_context, read_callback, 1);

    fasterKv->CompletePending(true);

    jbyte *copied_copied_key_bytes = (jbyte*) malloc(key_len);
    memcpy(copied_copied_key_bytes, key_bytes, key_len);

    auto callback = [](IAsyncContext *ctxt, Status result) {
        CallbackContext<DeleteContext> context{ctxt};
    };
    DeleteContext context{copied_copied_key_bytes, key_len, static_cast<uint64_t>(read_context.length)};
    Status result = fasterKv->Delete(context, callback, 1);
}

/*
 * Class:     edu_useoul_streamix_faster_flink_FasterKv
 * Method:    upsert
 * Signature: (J[B[B)V
 */
JNIEXPORT void JNICALL Java_edu_useoul_streamix_faster_1flink_FasterKv_upsert
        (JNIEnv *env, jobject object, jlong handle, jbyteArray key, jbyteArray value) {
    auto fasterKv = reinterpret_cast<FasterKv<ByteArrayKey, ByteArrayValue, disk_t> *>(handle);
    // Convert jbyteArray to uint8_t array
    uint64_t key_len = env->GetArrayLength(key);
    jbyte *key_bytes = env->GetByteArrayElements(key, nullptr);
    // Delete first because in-place updating variable-sized value is not possible in FasterKv.
    InternalDelete(fasterKv, key_bytes, key_len);
    jbyte *copied_key_bytes = (jbyte*) malloc(key_len);
    memcpy(copied_key_bytes, key_bytes, key_len);
    uint32_t value_len = env->GetArrayLength(value);
    jbyte *value_bytes = env->GetByteArrayElements(value, nullptr);
    auto callback = [](IAsyncContext *ctxt, Status result) {
        CallbackContext<UpsertContext> context{ctxt};
    };
    UpsertContext context{copied_key_bytes, key_len, value_bytes, value_len};
    Status result = fasterKv->Upsert(context, callback, 1);
    fasterKv->CompletePending(true);
    // assert(result == Status::Ok);
}

/*
 * Class:     edu_useoul_streamix_faster_flink_FasterKv
 * Method:    delete
 * Signature: (J[B)V
 */
JNIEXPORT void JNICALL Java_edu_useoul_streamix_faster_1flink_FasterKv_delete
        (JNIEnv *env, jobject object, jlong handle, jbyteArray key) {

    auto fasterKv = reinterpret_cast<FasterKv<ByteArrayKey, ByteArrayValue, disk_t> *>(handle);
    uint64_t key_len = env->GetArrayLength(key);
    jbyte *key_bytes = env->GetByteArrayElements(key, nullptr);
    InternalDelete(fasterKv, key_bytes, key_len);
    fasterKv->CompletePending(true);
}

/*
 * Class:     edu_useoul_streamix_faster_flink_FasterKv
 * Method:    close
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_edu_useoul_streamix_faster_1flink_FasterKv_close
        (JNIEnv *env, jobject object, jlong handle) {
    cout << "Close FasterKv Session..." << endl;
    auto fasterKv = reinterpret_cast<FasterKv<ByteArrayKey, ByteArrayValue, disk_t> *>(handle);
    fasterKv->StopSession();
    delete fasterKv;
}