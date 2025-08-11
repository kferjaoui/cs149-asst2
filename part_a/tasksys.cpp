#include "tasksys.h"
#include <atomic>
#include <algorithm>

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads), num_threads(num_threads) {}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {

    const int T = std::max(1, std::min(num_total_tasks, this->num_threads));

    std::atomic<int> next;
    next.store(0);

    auto workFn = [&]() {
        for (;;){
            int taskId = next.fetch_add(1, std::memory_order_relaxed);
            if (taskId >= num_total_tasks) break;
            runnable->runTask(taskId, num_total_tasks);  
        }
    };
    
    std::vector<std::thread> workers; // Vector to hold threads
    workers.reserve(T-1); // Reserve space for threads

    for (int i = 1; i < T; i++) workers.emplace_back(workFn);
    workFn(); //call from main thread 

    for (auto& worker: workers) worker.join();    
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

// TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads, int num_total_tasks): ITaskSystem(num_threads), _num_threads(num_threads) {
TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads), _num_threads(num_threads) {
    
    (this->_threadPool).reserve(num_threads);

    auto loopFn = [this](){
        while(true){
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lock(this->_queue_mutex);
                this->_cv.wait(lock, [this](){ return !this->_task_queue.empty() || this->_stop; });
                if (this->_stop && this->_task_queue.empty()) return;
                task = std::move(_task_queue.front());
                _task_queue.pop();
            }
            task();
        }
    };

    for(int t=0; t<num_threads; t++){
        _threadPool.emplace_back(loopFn);
    }

}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    {
        std::unique_lock<std::mutex> lock(this->_queue_mutex);
        this->_stop = true;
    }

    this->_cv.notify_all();

    for(auto& thread : this->_threadPool) thread.join();
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {

    std::atomic<int> remaining{num_total_tasks};
    std::mutex done_m;
    std::condition_variable done_cv;

    // Enqueue tasks under lock
    {
        std::lock_guard<std::mutex> lk(_queue_mutex);
        for (int i = 0; i < num_total_tasks; ++i) {
            _task_queue.emplace([=, &remaining, &done_m, &done_cv]() {
                runnable->runTask(i, num_total_tasks);

                // completion accounting
                if (remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                    std::lock_guard<std::mutex> g(done_m);
                    done_cv.notify_one();
                }
            });
        }
    }
    _cv.notify_all(); // wake workers to start consuming

    // Wait until all tasks complete
    {
        std::unique_lock<std::mutex> lk(done_m);
        done_cv.wait(lk, [&]{ return remaining.load(std::memory_order_acquire) == 0; });
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
