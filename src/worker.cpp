#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <sstream>
#include <glog/logging.h>

#include "server/messages.h"
#include "server/worker.h"
#include "tools/cycle_timer.h"
#include "tools/work_queue.h"

#define MAX_CPU_THREADS_PER_WORKER 32    // 2 * 6 * 2 = 24
#define MAX_IO_THREADS_PER_WORKER 2
#define IO_TASK_NAME "projectidea"

pthread_t pthreads[MAX_CPU_THREADS_PER_WORKER + MAX_IO_THREADS_PER_WORKER];
WorkQueue<Request_msg *> cpu_work_queue = WorkQueue<Request_msg *>();
WorkQueue<Request_msg *> io_work_queue = WorkQueue<Request_msg *>();

void *pull_cpu_work(__attribute__((__unused__)) void *arg) {

  while (1) {

    Request_msg *reqp = cpu_work_queue.get_work();
    Request_msg req = *reqp;

    // Make the tag of the reponse match the tag of the request.  This
    // is a way for your master to match worker responses to requests.
    Response_msg resp(req.get_tag());

    execute_work(req, resp);

    // send a response string to the master
    worker_send_response(resp);

    delete ((Request_msg *)reqp);
  }
  
  // Shouldn't reach here
  return NULL;
}

void *pull_io_work(__attribute__((__unused__)) void *arg) {

  while (1) {

    Request_msg *reqp = io_work_queue.get_work();
    Request_msg req = *reqp;

    // Make the tag of the reponse match the tag of the request.  This
    // is a way for your master to match worker responses to requests.
    Response_msg resp(req.get_tag());

    execute_work(req, resp);

    // send a response string to the master
    worker_send_response(resp);

    delete ((Request_msg *)reqp);
  }
  
  // Shouldn't reach here
  return NULL;
}

void worker_node_init(__attribute__((__unused__)) const Request_msg& params) {

  // This is your chance to initialize your worker.  For example, you
  // might initialize a few data structures, or maybe even spawn a few
  // pthreads here.  Remember, when running on Amazon servers, worker
  // processes will run on an instance with a dual-core CPU.

  // Create multiple cpu workers
  for (int i = 0; i < MAX_CPU_THREADS_PER_WORKER; i++) {
    pthread_create(&pthreads[i], NULL, pull_cpu_work, NULL);
  }

  // Create multiple io workers
  for (int i = 0; i < MAX_IO_THREADS_PER_WORKER; i++) {
    pthread_create(&pthreads[MAX_CPU_THREADS_PER_WORKER + i], NULL, 
        pull_io_work, NULL);
  }
}

void worker_handle_request(const Request_msg& req) {

  Request_msg *reqp = new Request_msg(req);

  if (req.get_arg("cmd").compare(IO_TASK_NAME) == 0) {
    io_work_queue.put_work(reqp);
  } else {
    cpu_work_queue.put_work(reqp);
  }

}
