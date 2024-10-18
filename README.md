# IED Server


### init version
>mqtt_2_opcua_server.py  "D:\project\IED\mqtt2opcua_part1\mqtt_2_opcua_server_main.py"

>mqtt_publish.py 
"D:\project\IED\mqttTesting\publisherTesting3.py"



### mosquitto 
> **WIN**    
mosquitto_pub -h 127.0.0.1 -t Topic/DI/P31025 -f "D:\project\IED\mqtt2opcua_part2\mqtt_pub_test.json"

>**LINUX**  
mosquitto_pub -h 192.168.1.84 -t test/topic -m '{"name":"MyVariable8","tag":"ns=2;s=MyVariable8","value":88,"quality":"Good","timestamp":"2024-10-02 23:38:02.425118"}'
