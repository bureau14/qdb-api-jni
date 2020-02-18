#pragma once

#include <string>

namespace qdb
{
namespace jni
{

class env;

class debug
{
  public:
    static void println(env &env, std::string const &msg);

    static void println(env &env, char const *msg);

  private:
    static void hexdump(env &env, void const *buf, size_t len);
};
}; // namespace jni
}; // namespace qdb
