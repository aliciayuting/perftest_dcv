#include <cascade/user_defined_logic_interface.hpp>
#include <iostream>
#include <cascade/utils.hpp>


namespace derecho{
namespace cascade{

#define MY_UUID     "20e60f1b-2522-22aa-2222-0366ac110006"
#define MY_DESC     "Noop UDL for benchmark purposes"
#define NEXT_UDL_SUBGROUP_ID 0
#define NEXT_UDL_SHARD_ID 1
#define INTERNAL_SENDER_TIMESTAMP_FILE "sender.dat"
#define RECV_PREFIX    "/noop_udl/"



std::string get_uuid() {
    return MY_UUID;
}

std::string get_description() {
    return MY_DESC;
}


class InternalPutOCDPO: public DefaultOffCriticalDataPathObserver {

    int my_id;

    virtual void ocdpo_handler (const node_id_t sender,
                               const std::string& object_pool_pathname,
                               const std::string& key_string,
                               const ObjectWithStringKey& object,
                               const emit_func_t& emit,
                               DefaultCascadeContextType* typed_ctxt,
                               uint32_t worker_id) override {
        
        uint32_t max_operation_per_second = 500;
        uint64_t duration_secs = 10; // run for 10 seconds
        uint32_t object_size = 1024; // 1 KB objects


        std::vector<ObjectWithStringKey> objects;
        uint32_t num_distinct_objects = 100;
        make_workload<std::string, ObjectWithStringKey>(object_size, num_distinct_objects, RECV_PREFIX, objects);

        uint64_t interval_ns = (max_operation_per_second==0)?0:static_cast<uint64_t>(INT64_1E9/max_operation_per_second);
        uint64_t next_ns = get_walltime();
        uint64_t end_ns = next_ns + duration_secs*1000000000ull;
        uint64_t message_id = 0;
        uint64_t now_ns = get_walltime();

        // control read_write_ratio
        while(now_ns < end_ns) {
            // we leave 500 ns for loop overhead.
            if (now_ns + 500 < next_ns) {
                usleep((next_ns - now_ns - 500)/1000); // sleep in microseconds.
            }
            next_ns += interval_ns;
            // set message id.
            objects.at(now_ns%num_distinct_objects).set_message_id(message_id);
            // log time.
            TimestampLogger::log(INTERNAL_CLIENT_READY_TO_SEND,my_id,message_id);
            // send it
            // std::cout << "Node " << my_id << " sending message id " << message_id << std::endl;
            typed_ctxt->get_service_client_ref().put_and_forget<VolatileCascadeStoreWithStringKey>(
                                                objects.at(now_ns%num_distinct_objects),
                                                NEXT_UDL_SUBGROUP_ID, NEXT_UDL_SHARD_ID, true);
            message_id ++;
        }

        // send finish signal
        std::string key = std::string(RECV_PREFIX) + "/finish";
        const uint8_t one_byte[] = { static_cast<uint8_t>('0') };
        ObjectWithStringKey finish_obj(key, one_byte, sizeof(one_byte));
        typed_ctxt->get_service_client_ref().put_and_forget<VolatileCascadeStoreWithStringKey>(
                                                finish_obj,
                                                NEXT_UDL_SUBGROUP_ID, NEXT_UDL_SHARD_ID, true);
        std::cout << "Node " << my_id << " finished internal put_and_forget test, sent finish signal." << std::endl;
        TimestampLogger::flush(INTERNAL_SENDER_TIMESTAMP_FILE);
    }

    static std::shared_ptr<OffCriticalDataPathObserver> ocdpo_ptr;
public:
    static void initialize() {
        if(!ocdpo_ptr) {
            ocdpo_ptr = std::make_shared<InternalPutOCDPO>();
        }
    }
    static auto get() {
        return ocdpo_ptr;
    }

    void set_config(DefaultCascadeContextType* typed_ctxt, const nlohmann::json& config){
        this->my_id = typed_ctxt->get_service_client_ref().get_my_id();
    }

};

std::shared_ptr<OffCriticalDataPathObserver> InternalPutOCDPO::ocdpo_ptr;

void initialize(ICascadeContext* ctxt) {
    InternalPutOCDPO::initialize();
}

std::shared_ptr<OffCriticalDataPathObserver> get_observer(
        ICascadeContext* ctxt,const nlohmann::json& config) {
    auto typed_ctxt = dynamic_cast<DefaultCascadeContextType*>(ctxt);
    std::static_pointer_cast<InternalPutOCDPO>(InternalPutOCDPO::get())->set_config(typed_ctxt,config);
    return InternalPutOCDPO::get();
}

void release(ICascadeContext* ctxt) {
    // nothing to release
    return;
}

} // namespace cascade
} // namespace derecho