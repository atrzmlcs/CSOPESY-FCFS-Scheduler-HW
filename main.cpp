#include <iostream>
#include <fstream>
#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <string>
#include <memory>

enum class ProcessState { READY, RUNNING, FINISHED };

struct Process {
    std::string name;
    std::string creationTime;
    int totalInstructions = 100;
    std::atomic<int> executedInstructions{0};
    std::atomic<int> assignedCore{-1};
    std::atomic<ProcessState> state{ProcessState::READY};

    Process(const std::string& pName, int cmdCount = 100)
        : name(pName), totalInstructions(cmdCount) {
        auto now = std::chrono::system_clock::now();
        std::time_t in_time_t = std::chrono::system_clock::to_time_t(now);
        std::stringstream ss;
        ss << std::put_time(std::localtime(&in_time_t), "%m/%d/%Y %I:%M:%S%p");
        creationTime = ss.str();
    }
};

class FCFSScheduler {
private:
    static const int NUM_CORES = 4;
    std::vector<std::thread> workerThreads;
    std::thread schedulerThread;

    std::queue<std::shared_ptr<Process>> readyQueue;
    std::vector<std::shared_ptr<Process>> allProcesses;

    std::mutex queueMutex;
    std::mutex fileMutex;
    std::condition_variable cvScheduler;
    std::condition_variable cvWorkers[NUM_CORES];

    std::shared_ptr<Process> coreAssignedProcess[NUM_CORES];
    bool coreBusy[NUM_CORES] = {false};
    std::atomic<bool> stopScheduler{false};

    std::string getCurrentFormattedTime() {
        auto now = std::chrono::system_clock::now();
        std::time_t in_time_t = std::chrono::system_clock::to_time_t(now);
        std::stringstream ss;
        ss << std::put_time(std::localtime(&in_time_t), "%m/%d/%Y %I:%M:%S%p");
        return ss.str();
    }

    void executePrintInstruction(const std::shared_ptr<Process>& proc, int coreId) {
        std::lock_guard<std::mutex> lock(fileMutex);
        std::ofstream logFile(proc->name + ".txt", std::ios::app);
        if (logFile.is_open()) {
            logFile << "(" << getCurrentFormattedTime() << ") Core:" << coreId 
                    << " \"Hello world from " << proc->name << "!\"\n";
            logFile.close();
        }
    }

    // 1 Scheduler Thread: Assigns processes to free cores in FCFS order
    void schedulerLoop() {
        while (!stopScheduler) {
            std::unique_lock<std::mutex> lock(queueMutex);
            cvScheduler.wait(lock, [this]() {
                return stopScheduler || !readyQueue.empty();
            });

            if (stopScheduler) break;

            for (int coreId = 0; coreId < NUM_CORES; ++coreId) {
                if (!coreBusy[coreId] && !readyQueue.empty()) {
                    auto proc = readyQueue.front();
                    readyQueue.pop();

                    proc->state = ProcessState::RUNNING;
                    proc->assignedCore = coreId;
                    coreAssignedProcess[coreId] = proc;
                    coreBusy[coreId] = true;

                    cvWorkers[coreId].notify_one();
                }
            }
        }
    }

    // 1 Thread per CPU Worker Core (4 Cores Total)
    void workerLoop(int coreId) {
        while (!stopScheduler) {
            std::shared_ptr<Process> proc = nullptr;

            {
                std::unique_lock<std::mutex> lock(queueMutex);
                cvWorkers[coreId].wait(lock, [this, coreId]() {
                    return stopScheduler || (coreBusy[coreId] && coreAssignedProcess[coreId] != nullptr);
                });

                if (stopScheduler) break;
                proc = coreAssignedProcess[coreId];
            }

            if (proc) {
                while (proc->executedInstructions < proc->totalInstructions && !stopScheduler) {
                    executePrintInstruction(proc, coreId);
                    proc->executedInstructions++;

                    // Delays each instruction slightly (50ms) so execution spans a few seconds,
                    // giving time to enter "screen -ls" multiple times.
                    std::this_thread::sleep_for(std::chrono::milliseconds(50));
                }

                {
                    std::unique_lock<std::mutex> lock(queueMutex);
                    proc->state = ProcessState::FINISHED;
                    coreAssignedProcess[coreId] = nullptr;
                    coreBusy[coreId] = false;
                    cvScheduler.notify_one(); // Signal scheduler that core is free
                }
            }
        }
    }

public:
    FCFSScheduler() {
        for (int i = 0; i < NUM_CORES; ++i) {
            coreAssignedProcess[i] = nullptr;
        }
    }

    ~FCFSScheduler() {
        stop();
    }

    void start() {
        stopScheduler = false;
        for (int i = 0; i < NUM_CORES; ++i) {
            workerThreads.emplace_back(&FCFSScheduler::workerLoop, this, i);
        }
        schedulerThread = std::thread(&FCFSScheduler::schedulerLoop, this);
    }

    void stop() {
        stopScheduler = true;
        cvScheduler.notify_all();
        for (int i = 0; i < NUM_CORES; ++i) {
            cvWorkers[i].notify_all();
        }

        if (schedulerThread.joinable()) {
            schedulerThread.join();
        }
        for (auto& t : workerThreads) {
            if (t.joinable()) {
                t.join();
            }
        }
    }

    void createTestProcesses(int count = 10, int commandsPerProcess = 100) {
        std::unique_lock<std::mutex> lock(queueMutex);
        for (int i = 1; i <= count; ++i) {
            std::stringstream ss;
            ss << "process" << std::setw(2) << std::setfill('0') << i;
            auto proc = std::make_shared<Process>(ss.str(), commandsPerProcess);

            // Create initial text file header matching required format
            std::ofstream logFile(proc->name + ".txt", std::ios::trunc);
            if (logFile.is_open()) {
                logFile << "Process name: " << proc->name << "\n";
                logFile << "Logs:\n\n";
                logFile.close();
            }

            allProcesses.push_back(proc);
            readyQueue.push(proc);
        }
        cvScheduler.notify_one();
    }

    void printScreenList() {
        std::unique_lock<std::mutex> lock(queueMutex);

        std::cout << "--------------------------------------------------\n";
        std::cout << "Running processes:\n";
        for (const auto& proc : allProcesses) {
            if (proc->state == ProcessState::RUNNING) {
                std::cout << std::left << std::setw(12) << proc->name
                          << "(" << proc->creationTime << ")    "
                          << "Core: " << proc->assignedCore << "    "
                          << proc->executedInstructions << " / " << proc->totalInstructions << "\n";
            }
        }

        std::cout << "\nFinished processes:\n";
        for (const auto& proc : allProcesses) {
            if (proc->state == ProcessState::FINISHED) {
                std::cout << std::left << std::setw(12) << proc->name
                          << "(" << proc->creationTime << ")    "
                          << "Finished    "
                          << proc->executedInstructions << " / " << proc->totalInstructions << "\n";
            }
        }
        std::cout << "--------------------------------------------------\n\n";
    }
};

int main() {
    FCFSScheduler scheduler;

    // Start 1 scheduler thread and 4 core worker threads
    scheduler.start();

    // Homework Test Case: Create 10 processes with 100 print commands each
    scheduler.createTestProcesses(10, 100);

    std::cout << "FCFS CPU Scheduler Started with 4 Cores.\n";
    std::cout << "Commands:\n";
    std::cout << "  screen -ls  : View running and finished processes\n";
    std::cout << "  exit        : Exit program\n\n";

    std::string input;
    while (true) {
        std::cout << "root@emulator:~# ";
        if (!std::getline(std::cin, input)) break;

        if (input == "screen -ls") {
            scheduler.printScreenList();
        } else if (input == "exit") {
            break;
        } else if (!input.empty()) {
            std::cout << "Unknown command: " << input << "\n";
        }
    }

    scheduler.stop();
    return 0;
}