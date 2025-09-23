#include <cascade/user_defined_logic_interface.hpp>
#include <iostream>
#include <cascade/utils.hpp>


namespace derecho{
namespace cascade{

#define MY_UUID     "48e60f7c-8500-11eb-8755-0242ac110002"
#define MY_DESC     "Noop UDL for benchmark purposes"
#define UDL_TIMESTAMP_FILE "udl.dat"


std::string get_uuid() {
    return MY_UUID;
}

std::string get_description() {
    return MY_DESC;
}


class NoopOCDPO: public DefaultOffCriticalDataPathObserver {

    int my_id;

    virtual void ocdpo_handler (const node_id_t sender,
                               const std::string& object_pool_pathname,
                               const std::string& key_string,
                               const ObjectWithStringKey& object,
                               const emit_func_t& emit,
                               DefaultCascadeContextType* typed_ctxt,
                               uint32_t worker_id) override {
        

        if (key_string == "/finish") {
            TimestampLogger::flush(UDL_TIMESTAMP_FILE);
            return;
        }
        uint64_t msg_id = object.get_message_id();
        std::cout << "[noop ocdpo]: key_string=" << key_string << ", msg_id is=" << msg_id << std::endl;
        TimestampLogger::log(UDL_FLAG, msg_id, my_id, 0);
    }

    static std::shared_ptr<OffCriticalDataPathObserver> ocdpo_ptr;
public:
    static void initialize() {
        if(!ocdpo_ptr) {
            ocdpo_ptr = std::make_shared<NoopOCDPO>();
        }
    }
    static auto get() {
        return ocdpo_ptr;
    }

    void set_config(DefaultCascadeContextType* typed_ctxt, const nlohmann::json& config){
        this->my_id = typed_ctxt->get_service_client_ref().get_my_id();
    }

};

std::shared_ptr<OffCriticalDataPathObserver> NoopOCDPO::ocdpo_ptr;

void initialize(ICascadeContext* ctxt) {
    NoopOCDPO::initialize();
}

std::shared_ptr<OffCriticalDataPathObserver> get_observer(
        ICascadeContext* ctxt,const nlohmann::json& config) {
    auto typed_ctxt = dynamic_cast<DefaultCascadeContextType*>(ctxt);
    std::static_pointer_cast<NoopOCDPO>(NoopOCDPO::get())->set_config(typed_ctxt,config);
    return NoopOCDPO::get();
}

void release(ICascadeContext* ctxt) {
    // nothing to release
    return;
}

} // namespace cascade
} // namespace derecho