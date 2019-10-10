import json
c = 0
with open("./output.jsonl") as f:
    for line in f:
        try:
            j = json.loads(line)
            j["device_is_bot"] = j.pop("ua_detected_is_bot")
            j["device_client_name"] = j.pop("ua_detected_client_name")
            j["device_client_version"] = j.pop("ua_detected_client_version")
            j["device_os_name"] = j.pop("ua_detected_os_name")
            j["device_os_version"] = j.pop("ua_detected_os_version")
            j["device_device_type"] = j.pop("ua_detected_device_type")
            j["device_is_mobile"] = j.pop("ua_detected_is_mobile")
            j["device_device_name"] = j.pop("ua_detected_device_name")
            j["device_device_brand"] = j.pop("ua_detected_device_brand")
            j["device_device_model"] = j.pop("ua_detected_device_model")
            j["device_device_input"] = j.pop("ua_detected_device_input")
            j["device_device_info"] = j.pop("ua_detected_device_info")
        except Exception as e:
            print(e) 
        else:
            c += 1
            print(c)
            with open("./output_formatted.jsonl", "a+") as o:
                o.write(json.dumps(j) + "\n")

        if c == 1000000:
            break
