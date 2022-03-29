#pragma once

namespace qdb::jni::signature::detail
{
template <typename T>
struct signature_util
{
    static inline constexpr char const * type();
};

template <>
struct signature_util<jlong>
{
    static inline constexpr char const * type()
    {
        return "J";
    }
};

template <>
struct signature_util<jlongArray>
{
    static inline constexpr char const * type()
    {
        return "[J";
    }

    static inline constexpr char const * points_subtype()
    {
        return "net/quasardb/qdb/ts/Series$Int64Data";
    }
};

template <>
struct signature_util<jdouble>
{
    static inline constexpr char const * type()
    {
        return "D";
    }
};

template <>
struct signature_util<jdoubleArray>
{
    static inline constexpr char const * type()
    {
        return "[D";
    }

    static inline constexpr char const * points_subtype()
    {
        return "net/quasardb/qdb/ts/Series$DoubleData";
    }
};

}; // namespace qdb::jni::signature::detail

namespace qdb::jni::signature
{

template <typename T>
inline constexpr char const * of_type()
{
    return detail::signature_util<T>::type();
};

template <typename T>
inline constexpr char const * points_subtype_of()
{
    return detail::signature_util<T>::points_subtype();
};

}; // namespace qdb::jni::signature
