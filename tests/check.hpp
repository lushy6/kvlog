// Tiny test harness: CHECK records a failure and keeps going; main returns the failure count.
#pragma once
#include <cstdio>

inline int g_failures = 0;
#define CHECK(cond)                                                                     \
    do {                                                                                \
        if (!(cond)) {                                                                  \
            ++g_failures;                                                               \
            std::fprintf(stderr, "%s:%d: CHECK failed: %s\n", __FILE__, __LINE__, #cond); \
        }                                                                               \
    } while (0)

inline int finish(const char* name) {
    if (g_failures) std::fprintf(stderr, "%s: %d failure(s)\n", name, g_failures);
    else std::printf("%s: all checks passed\n", name);
    return g_failures ? 1 : 0;
}
