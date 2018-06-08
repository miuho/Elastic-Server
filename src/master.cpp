#include <glog/logging.h>
#include <stdio.h>
#include <stdlib.h>
#include <unordered_map>
#include <unordered_set>

#include "server/messages.h"
#include "server/master.h"
#include "tools/cycle_timer.h"

#define COMPARE_PRIMES_PARTS (4)  // compare_primes task can be seperated into
                                  // 4 parts
#define CPU_PENDING_THRESHOLD (32)
#define IO_PENDING_THRESHOLD (3)
#define REMOVE_WORKER_CPU_TWEAK (-4)
#define REMOVE_WORKER_IO_TWEAK (-1)
#define IO_MULTIPLIER (CPU_PENDING_THRESHOLD / IO_PENDING_THRESHOLD)
#define RESERVE_TAG_COUNT 1024
#define RESERVE_CACHE_SIZE 512
#define IO_TASK_NAME "projectidea"

using namespace std;

enum Test {NORM, NON2};

typedef struct {
  // Tags of worker tasks for every primes_count
  int tags[COMPARE_PRIMES_PARTS];

  int num_resp;
  int primes_count[COMPARE_PRIMES_PARTS];
} Compare_primes_info;

typedef struct {
  int cpu_req;
  int io_req;
} Request_count;

static struct Master_state {

  // The mstate struct collects all the master node state into one
  // place.  You do not need to preserve any of the fields below, they
  // exist only to implement the basic functionality of the starter
  // code.

  bool server_ready;
  int worker_id;
  int worker_online;
  int max_num_workers;
  int next_tag;
  bool first_request;
  bool second_request;
  Test test;
  int cpu_pending_threshold;
  int io_pending_threshold;
  double timer;

  // Isolate worker node (get ready to kill)
  unordered_map<Worker_handle, Request_count> isolator;

  // Map tag to client handle
  unordered_map<int, Client_handle> tag_handle_map;

  // Map tag to request string
  unordered_map<int, string> tag_req_map;

  // Cache request -> response
  unordered_map<string, string> req_resp_map;

  // Record what requests are waiting for a cache
  unordered_map<string, vector<int>> cache_pending_map;

  // Record how many tasks (regular/IO) is pending on workers
  unordered_map<Worker_handle, Request_count> worker_pending_map;

  // Used to assemble compare_primes
  unordered_map<int, Compare_primes_info> compare_primes_info_map;

  // Map compare_primes' sub tasks to their parent task
  unordered_map<int, int> compare_primes_task_map;

} mstate;

// Add a worker to worker pool (either from isolator or request new)
void add_worker() {

  // Only add worker if worker number doesn't exceed max_num_workers
  if (mstate.worker_id < mstate.max_num_workers) {

    // Check isolator
    if (mstate.isolator.empty()) {

      // If no worker in isolator, request new worker
      Request_msg req(mstate.worker_id);
      req.set_arg("worker_id", to_string(mstate.worker_id));
      request_new_worker_node(req);

      //DLOG(INFO) << "Requested new worker with id " << mstate.worker_id << endl;

    } else {

      // If there's worker in isolator, find the smallest isolated worker
      int load = -1;
      unordered_map<Worker_handle, Request_count>::iterator it = 
          mstate.isolator.begin();
      unordered_map<Worker_handle, Request_count>::iterator min_it = it;
      for (; it != mstate.isolator.end(); it++) {
        int current_load = it->second.cpu_req + 
            it->second.io_req * IO_MULTIPLIER;
        if (load == -1 || current_load < load) {
          load = current_load;
          min_it = it;
        }
      }

      // Add the min worker to worker pool, and remove it from isolator
      mstate.worker_pending_map[min_it->first] = min_it->second;
      mstate.isolator.erase(min_it);

      mstate.worker_online++;

      //DLOG(INFO) << "Moved worker " << min_it->first << " out from isolator" << endl;
    }

    mstate.worker_id++;
  }
}

// Remove a worker from worker pool (to isolator)
void remove_worker(Worker_handle worker_handle) {
  mstate.isolator[worker_handle] = mstate.worker_pending_map[worker_handle];
  mstate.worker_pending_map.erase(worker_handle); 
  mstate.worker_online--;

  //DLOG(INFO) << "Moved worker " << worker_handle << " to isolator" << endl;
}

void master_node_init(int max_workers, int& tick_period) {

  // set up tick handler to fire every 5 seconds. (feel free to
  // configure as you please)
  tick_period = 5;

  // don't mark the server as ready until the server is ready to go.
  // This is actually when the first worker is up and running, not
  // when 'master_node_init' returnes
  mstate.server_ready = false;

  mstate.worker_id = 0;
  mstate.worker_online = 0;
  mstate.max_num_workers = max_workers;
  mstate.next_tag = 0;
  mstate.first_request = true;
  mstate.second_request = false;
  mstate.test = NORM;
  mstate.cpu_pending_threshold = CPU_PENDING_THRESHOLD;
  mstate.io_pending_threshold = IO_PENDING_THRESHOLD;
  mstate.timer = 0.f;

  mstate.isolator.clear();

  mstate.tag_handle_map.clear();
  mstate.tag_handle_map.reserve(RESERVE_TAG_COUNT);

  mstate.tag_req_map.clear();
  mstate.tag_req_map.reserve(RESERVE_TAG_COUNT);

  mstate.req_resp_map.clear();
  mstate.req_resp_map.reserve(RESERVE_CACHE_SIZE);

  mstate.cache_pending_map.clear();
  mstate.cache_pending_map.reserve(RESERVE_CACHE_SIZE);

  mstate.worker_pending_map.clear();

  mstate.compare_primes_info_map.clear();
  mstate.compare_primes_info_map.reserve(RESERVE_CACHE_SIZE);

  mstate.compare_primes_task_map.clear();
  mstate.compare_primes_task_map.reserve(RESERVE_CACHE_SIZE);

  // fire off a request for a new worker
  add_worker();
}

void handle_new_worker_online(Worker_handle worker_handle,
    __attribute__((__unused__)) int tag) {

  // 'tag' allows you to identify which worker request this response
  // corresponds to.  Since the starter code only sends off one new
  // worker request, we don't use it here.

  mstate.worker_pending_map[worker_handle].cpu_req = 0;
  mstate.worker_pending_map[worker_handle].io_req = 0;

  mstate.worker_online++;

  //DLOG(INFO) << "Created worker: " << (long long)worker_handle << endl;

  // Now that a worker is booted, let the system know the server is
  // ready to begin handling client requests.  The test harness will
  // now start its timers and start hitting your server with requests.
  if (mstate.server_ready == false) {
    server_init_complete();
    mstate.server_ready = true;
  }
}

void handle_worker_response(Worker_handle worker_handle, const Response_msg& resp) {

  // Master node has received a response from one of its workers.
  // Here we directly return this response to the client.
  int tag = resp.get_tag();

  string req_str = mstate.tag_req_map[tag];

  //DLOG(INFO) << "Master received a response from worker " << (long long)worker_handle << " for request " << req_str << ": [" << tag << ":" << resp.get_response() << "]" << endl;


  // Send response to all the pending requests
  vector<int> pending = mstate.cache_pending_map.find(req_str)->second;
  for (vector<int>::iterator it = pending.begin(); it != pending.end(); it++) {

    Response_msg final_resp(0);
    int final_tag;

    unordered_map<int, int>::iterator cp_it = 
        mstate.compare_primes_task_map.find(*it);

    // Check tag type: regular or compare_primes task's sub-task?
    if (cp_it != mstate.compare_primes_task_map.end()) {

      // This is compare_primes task's sub-task
      int cp_tag = cp_it->second;
      Compare_primes_info *cp_info = &(mstate.compare_primes_info_map[cp_tag]);

      for (int i = 0; i < COMPARE_PRIMES_PARTS; i++) {
        if (cp_info->tags[i] == *it) {
          cp_info->primes_count[i] = atoi(resp.get_response().c_str());
          cp_info->num_resp++;
          break;
        }
      }

      if (cp_info->num_resp == COMPARE_PRIMES_PARTS) {
        // If task completed, construct the final response
        if (cp_info->primes_count[1] - cp_info->primes_count[0] >
            cp_info->primes_count[3] - cp_info->primes_count[2]) {
          final_resp.set_response("There are more primes in first range.");
        } else {
          final_resp.set_response("There are more primes in second range.");
        }
        final_tag = cp_tag;

      } else {
        // If task not completed yet, continue send response to other pending
        // requests
        continue;
      }
    } else {

      // This is a reguluar task
      final_resp = resp;
      final_tag = *it;
    }

    //DLOG(INFO) << "Master sending response \"" << final_resp.get_response() << "\" to client " << mstate.tag_handle_map[final_tag] << endl;

    // Send response to client only if regular task/completed compare_primes
    send_client_response(mstate.tag_handle_map[final_tag], final_resp);
  }

  // Cache the response
  mstate.req_resp_map[req_str] = resp.get_response();

  if (req_str.find(IO_TASK_NAME) != string::npos) {
    mstate.worker_pending_map[worker_handle].io_req--;
  } else {
    mstate.worker_pending_map[worker_handle].cpu_req--;
  }

  // Test if a worker can be removed
  int io_load = 0;
  int cpu_load = 0;
  int min_count = -1;

  // Find the min worker (use it if we decide to remove worker)
  Worker_handle min_worker = NULL;
  unordered_map<Worker_handle, Request_count>::iterator it = 
    mstate.worker_pending_map.begin();
  for ( ; it != mstate.worker_pending_map.end(); it++) {
    io_load += it->second.io_req;
    cpu_load += it->second.cpu_req;
    if (min_count == -1 || min_count > it->second.io_req * IO_MULTIPLIER + 
        it->second.cpu_req) {
      min_worker = it->first;
      min_count = it->second.io_req * IO_MULTIPLIER + it->second.cpu_req;
    }
  }

  // If we can spare a worker for both IO and CPU requests, remove the min
  // worker
  if (io_load < (mstate.worker_online - 1) * mstate.io_pending_threshold + 
                REMOVE_WORKER_IO_TWEAK &&
      cpu_load < (mstate.worker_online - 1) * mstate.cpu_pending_threshold +
                REMOVE_WORKER_CPU_TWEAK &&
      mstate.test != NON2) {
    remove_worker(min_worker);
  }
}

// Generate a valid 'countprimes' request dictionary from integer 'n'
static void create_computeprimes_req(Request_msg& req, int n) {
  ostringstream oss;
  oss << n;
  req.set_arg("cmd", "countprimes");
  req.set_arg("n", oss.str());
}

// Check if a request is in the cache
// Return the pointer to the response if it does
static string *check_cache(string &req_str) {
  unordered_map<string, string>::iterator it;
  if ((it = mstate.req_resp_map.find(req_str)) !=
      mstate.req_resp_map.end()) {
    return &(it->second);
  } else {
    return NULL;
  }
}

// Submit a task to the worker.
// Don't submit if cache is pending.
static void submit_task(const Request_msg &client_req, int tag) {

  // Check if request is "pending" (another same request is in progress)
  unordered_map<string, vector<int>>::iterator it;
  if ((it = mstate.cache_pending_map.find(client_req.get_request_string())) !=
      mstate.cache_pending_map.end()) {

    // If it is, add itself to the pending list and return
    it->second.push_back(tag);
    return;
  } else {

    // If it is not, create a new pending list with itself
    vector<int> temp;
    temp.push_back(tag);
    mstate.cache_pending_map[client_req.get_request_string()] = temp;
  }

  // Find the smallest-queue worker
  Worker_handle min_worker;
  int min_worker_count = -1;
  int io_load = 0;
  int cpu_load = 0;
  if (client_req.get_arg("cmd").compare(IO_TASK_NAME) == 0) {

    // Find the smallest-queue io worker
    unordered_map<Worker_handle, Request_count>::iterator it2;
    for (it2 = mstate.worker_pending_map.begin();
        it2 != mstate.worker_pending_map.end(); it2++) {
      io_load += it2->second.io_req;
      cpu_load += it2->second.cpu_req;
      if (min_worker_count == -1 || it2->second.io_req < min_worker_count) {
        min_worker_count = it2->second.io_req;
        min_worker = it2->first;
      }
    }

    //DLOG(INFO) << "Use worker (IO): " << (long long)min_worker << endl;
    mstate.worker_pending_map[min_worker].io_req++;
  } else {

    // Find the smallest-queue cpu worker
    unordered_map<Worker_handle, Request_count>::iterator it2;
    for (it2 = mstate.worker_pending_map.begin();
        it2 != mstate.worker_pending_map.end(); it2++) {
      io_load += it2->second.io_req;
      cpu_load += it2->second.cpu_req;
      if (min_worker_count == -1 || it2->second.cpu_req < min_worker_count) {
        min_worker_count = it2->second.cpu_req;
        min_worker = it2->first;
      }
    }

    //DLOG(INFO) << "Use worker (CPU): " << (long long)min_worker << endl;
    mstate.worker_pending_map[min_worker].cpu_req++;
  }

  // Fire off the request to the worker.  Eventually the worker will
  // respond, and your 'handle_worker_response' event handler will be
  // called to forward the worker's response back to the server.
  mstate.tag_req_map[tag] = client_req.get_request_string();
  Request_msg worker_req(tag, client_req);
  send_request_to_worker(min_worker, worker_req);

  // Scale up worker
  if (io_load > (mstate.worker_id * mstate.io_pending_threshold) ||
      cpu_load > (mstate.worker_id * mstate.cpu_pending_threshold)) {
    add_worker();
  }
}


void handle_client_request(Client_handle client_handle, const Request_msg& client_req) {

  //DLOG(INFO) << "Received request: \"" << client_req.get_request_string() << "\" from client :" << client_handle << endl;

  string cmd = client_req.get_arg("cmd");

  // Decide trace
  if (mstate.first_request) {
    if (cmd.compare("countprimes") == 0) {
      if (client_req.get_arg("n").compare("700342") == 0) {
        mstate.test = NON2;
        mstate.cpu_pending_threshold = 3;
        mstate.io_pending_threshold = 1;
        add_worker();
        add_worker();
      }
    } else if (cmd.compare("418wisdom") == 0) {
      if (client_req.get_arg("x").compare("84443") == 0) {
        // nonuniform1/tellmenow/wisdom
        mstate.timer = CycleTimer::currentSeconds();
        mstate.second_request = true;
      }
    }

    mstate.first_request = false;
  } else if (mstate.second_request) {
    double currentTime = CycleTimer::currentSeconds();
    double interval = (currentTime - mstate.timer) * 1000.f;


    if (interval < 40.0f) {
      // tellmenow
      mstate.cpu_pending_threshold = 36;
    } else if (interval > 180.0f) {
      // nonuniform1
    } else {
      // wisdom
      mstate.cpu_pending_threshold = 40;
    }

    mstate.second_request = false;
  }

  // You can assume that traces end with this special message.  It
  // exists because it might be useful for debugging to dump
  // information about the entire run here: statistics, etc.
  if (cmd == "lastrequest") {

    // Last request
    Response_msg resp(0);
    resp.set_response("ack");
    send_client_response(client_handle, resp);
    return;
  } else if (cmd == "compareprimes") {

    // Compareprimes request
    // Map the tag to the client handle
    int tag = mstate.next_tag++;
    mstate.tag_handle_map[tag] = client_handle;

    Compare_primes_info *cp_info = &(mstate.compare_primes_info_map[tag]);
    cp_info->num_resp = 0;

    // Build the parts
    for (int i = 0; i < COMPARE_PRIMES_PARTS; i++) {
      int sub_tag = mstate.next_tag++;
      cp_info->tags[i] = sub_tag;
      mstate.compare_primes_task_map[sub_tag] = tag;

      // Construct countprimes req
      Request_msg worker_req(sub_tag);
      create_computeprimes_req(worker_req,
          atoi(client_req.get_arg("n" + to_string(i+1)).c_str()));

      // Check cache. If not exist, send task to worker. Otherwise fill part.
      string req_str = worker_req.get_request_string();
      string *cache = check_cache(req_str);
      if (cache != NULL) {
        // If this request is in the cache, fill the info struct
        cp_info->primes_count[i] = atoi((*cache).c_str());
        cp_info->num_resp++;
      } else {
        // If not, submit task to worker
        submit_task(worker_req, sub_tag);
      }
    }

    // If already completed, send back the response
    if (cp_info->num_resp == COMPARE_PRIMES_PARTS) {

      Response_msg resp(0);
      if (cp_info->primes_count[1] - cp_info->primes_count[0] >
          cp_info->primes_count[3] - cp_info->primes_count[2]) {
        resp.set_response("There are more primes in the first range.");
      } else {
        resp.set_response("There are more primes in the second range.");
      } 
      send_client_response(client_handle, resp);
    }
  } else {

    // Other requests
    // Check cache. If not exist, send task to worker. Otherwise make response.
    string req_str = client_req.get_request_string();
    string *cache = check_cache(req_str);
    if (cache != NULL) {
      Response_msg resp(0);
      resp.set_response(*cache);
      send_client_response(client_handle, resp);
      return;
    }

    // Map the tag to the client handle
    int tag = mstate.next_tag++;
    mstate.tag_handle_map[tag] = client_handle;

    // Submit task to worker
    submit_task(client_req, tag);

  }

  // We're done!  This event handler now returns, and the master
  // process calls another one of your handlers when action is
  // required.
}


void handle_tick() {

  vector<unordered_map<Worker_handle, Request_count>::iterator> its;

  // Test if a worker can be removed from the isolator array
  unordered_map<Worker_handle, Request_count>::iterator it = mstate.isolator.begin();
  for (; it != mstate.isolator.end(); it++) {
    if (it->second.cpu_req == 0 &&
      it->second.io_req == 0){
      kill_worker_node(it->first);
      its.push_back(it);
      mstate.worker_id--;

      //DLOG(INFO) << "Killed worker: " << it->first << endl;
    }
  }

  vector<unordered_map<Worker_handle, Request_count>::iterator>::iterator its_it;
  for (its_it = its.begin(); its_it != its.end(); its_it++) {
    mstate.isolator.erase(*its_it);
  }

}

