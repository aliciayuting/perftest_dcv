#include <iostream>
#include <string>
#include <thread>
#include <chrono>
#include <unistd.h>
#include <cascade/object.hpp>
#include <cascade/utils.hpp>
#include <cascade/service_client_api.hpp>

#define UDL_SUBGROUP_INDEX 0
#define UDL_SHARD_INDEX 0
#define CLIENT_TIMESTAMP_FILE "client.dat"
#define UDLS_SUBGROUP_TYPE VolatileCascadeStoreWithStringKey 
#define PREFIX    "/noop_udl/"


using namespace derecho::cascade;

bool eval_put_and_forget( ServiceClientAPI& capi,
                        uint64_t max_operation_per_second,
                        uint64_t duration_secs,
                        uint32_t object_size = 1024) {
    std::vector<ObjectWithStringKey> objects;
    uint32_t num_distinct_objects = 100;
    make_workload<std::string, ObjectWithStringKey>(object_size, num_distinct_objects, PREFIX, objects);

    uint64_t interval_ns = (max_operation_per_second==0)?0:static_cast<uint64_t>(INT64_1E9/max_operation_per_second);
    uint64_t next_ns = get_walltime();
    uint64_t end_ns = next_ns + duration_secs*1000000000ull;
    uint64_t message_id = 0;

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
        TimestampLogger::log(TLT_READY_TO_SEND,message_id,capi.get_my_id());
        // send it
        capi.put_and_forget(objects.at(now_ns%num_distinct_objects), true);
       
        // log time.
        TimestampLogger::log(TLT_EC_SENT,message_id,capi.get_my_id());
        message_id ++;
    }

    // send finish signal
    std::string key = std::string(PREFIX) + "/finish";
    const uint8_t one_byte[] = { static_cast<uint8_t>('0') };
    ObjectWithStringKey finish_obj(key, one_byte, sizeof(one_byte));
    capi.put_and_forget(finish_obj, true);
    TimestampLogger::flush(CLIENT_TIMESTAMP_FILE);
    return true;
}




int main(int argc, char** argv){

    char c;
    std::cout << "Starting noop udl client..." << std::endl;
    uint64_t max_operation_per_second = 100;
    uint64_t duration_secs = 60;
    uint32_t object_size = 1024;

    while ((c = getopt(argc, argv, "r:d:s:")) != -1){
        switch(c){
            case 'r':
                max_operation_per_second = strtoul(optarg,NULL,10);
                break;
            case 'd':
                duration_secs = strtoul(optarg,NULL,10);
                break;
            case 's':
                object_size = strtoul(optarg,NULL,10);
                break;
            case '?':
            case 'h':
            default:
                std::cout << "usage: " << argv[0] << " [-r max_operation_per_second] [-d duration_secs] [-s object_size(bytes)]" << std::endl;
                return 0;
        }
    }

    ServiceClientAPI& capi = ServiceClientAPI::get_service_client();
    uint32_t my_id = capi.get_my_id();

    std::cout << "  creating object pool for receiving results: " << PREFIX << std::endl;
    std::string obj_pool_name = std::string("/noop_udl");
    auto res = capi.template create_object_pool<UDLS_SUBGROUP_TYPE>(obj_pool_name,UDL_SUBGROUP_INDEX,HASH,{});
    for (auto& reply_future:res.get()) {
        reply_future.second.get(); // wait for the object pool to be created
    }

    eval_put_and_forget(capi, max_operation_per_second,duration_secs);

    return 0;
}
