#include <iostream>
#include <string>
#include <thread>
#include <chrono>
#include <unistd.h>
#include <cascade/utils.hpp>
#include <cascade/debug_util.hpp>
#include <cascade/service_client_api.hpp>

#define UDL_SUBGROUP_INDEX 0
#define UDL_SHARD_INDEX 0
#define TLT_READY_TO_SEND    (10000)
#define TLT_EC_SENT          (10001)
#define CLIENT_TIMESTAMP_FILE "client.dat"
#define UDLS_SUBGROUP_TYPE VolatileCascadeStoreWithStringKey 
#define PREFIX    "noop_udl/"


using namespace derecho::cascade;

bool eval_put_and_forget(uint64_t max_operation_per_second,uint64_t duration_secs) {
    std::vector<ObjectWithStringKey> objects;
    uint32_t object_size = derecho::getConfUInt32(derecho::Conf::DERECHO_MAX_P2P_REQUEST_PAYLOAD_SIZE);
    uint32_t num_distinct_objects = std::min(static_cast<uint64_t>(max_num_distinct_objects), max_workload_memory / object_size);
    make_workload<std::string, ObjectWithStringKey>(object_size, num_distinct_objects, PREFIX, objects);

    uint64_t interval_ns = (max_operation_per_second==0)?0:static_cast<uint64_t>(INT64_1E9/max_operation_per_second);
    uint64_t next_ns = get_walltime();
    uint64_t end_ns = next_ns + duration_secs*1000000000ull;
    uint64_t message_id = 0;
    const uint32_t num_distinct_objects = objects.size();
    // control read_write_ratio
    while(true) {
        uint64_t now_ns = get_walltime();
        if (now_ns > end_ns) {
            break;
        }
        // we leave 500 ns for loop overhead.
        if (now_ns + 500 < next_ns) {
            usleep((next_ns - now_ns - 500)/1000); // sleep in microseconds.
        }
        next_ns += interval_ns;
        // set message id.
        objects.at(now_ns%num_distinct_objects).set_message_id(message_id);
        // log time.
        TimestampLogger::log(TLT_READY_TO_SEND,this->capi.get_my_id(),message_id);
        // send it
        this->capi.template put_and_forget<UDLS_SUBGROUP_TYPE>(objects.at(now_ns%num_distinct_objects), UDL_SUBGROUP_INDEX, UDL_SHARD_INDEX, true);
       
        // log time.
        TimestampLogger::log(TLT_EC_SENT,this->capi.get_my_id(),message_id);
        message_id ++;
    }
    Blob finish_blob;
    finish_blob.size = 0;
    finish_blob.bytes = nullptr;
    ObjectWithStringKey finish_obj;
    finish_obj.key = PREFIX + "finish";
    finish_obj.blob = finish_blob;
    this->capi.template put_and_forget<UDLS_SUBGROUP_TYPE>(finish_obj, UDL_SUBGROUP_INDEX, UDL_SHARD_INDEX, true);
    return true;
}


int main(){

    std::cout << "Starting noop udl client..." << std::endl;
    uint64_t max_operation_per_second = 100;
    uint64_t duration_secs = 60;

    while ((c = getopt(argc, argv, "r:d:")) != -1){
        switch(c){
            case 'r':
                max_operation_per_second = strtoul(optarg,NULL,10);
                break;
            case 'd':
                duration_secs = strtoul(optarg,NULL,10);
                break;
            case '?':
            case 'h':
            default:
                std::cout << "usage: " << argv[0] << " [-r max_operation_per_second] [-d duration_secs]" << std::endl;
                return 0;
        }
    }

    eval_put_and_forget(max_operation_per_second,duration_secs);

    return 0;
}
