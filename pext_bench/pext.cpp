// ChatGPT generated

#include <iostream>
#include <chrono>

int main() {
    constexpr int numIterations = 1000000000;
    uint64_t source = 0xAAAAAAAAAAAAAAAA;
    uint64_t mask = 0x5555555555555555;
    uint64_t result;

    auto startTime = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < numIterations; ++i) {
        asm volatile ("pext %1, %0, %0" : "+r"(result) : "r"(mask));
    }
    auto endTime = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(endTime - startTime).count();

    // Calculate throughput and latency
    double throughput = static_cast<double>(numIterations) / (duration / 1e9); // operations per second
    double latency = static_cast<double>(duration) / numIterations; // nanoseconds per operation

    // Print results
    std::cout << "Throughput: " << throughput << " operations/second\n";
    std::cout << "Latency: " << latency << " nanoseconds/operation\n";

    return 0;
}
