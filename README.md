# IED Server


### file_monitor.py
> * This script is used to monitor the changes of a specific file(iec2opcua_mapping.csv) using watchdog. When the csv file is changed, the script will ask the user whether to run a Python script(mqtt_2_opcua_server.py) again. If the user chooses "Yes", the script will run the script. The script will check the PID file to ensure that the script is only running once.
> 
> * 使用 watchdog 來監控特定文件(iec2opcua_mapping.csv)的變化，一旦監測到csv變化，就會詢問使用者是否希望重新執行一個 Python 腳本(mqtt_2_opcua_server.py)。當使用者選擇「是」的時候，程式會執行該腳本。使用PID檢查，確保程式只會在一個執行中。



#### init version
> mqtt_2_opcua_server.py  "D:\project\IED\mqtt2opcua_part1\mqtt_2_opcua_server_main.py"
>   
> mqtt_publish.py 
"D:\project\IED\mqttTesting\publisherTesting3.py"



#### mosquitto 
> **WIN**    
mosquitto_pub -h 127.0.0.1 -t Topic/DI/P31025 -f "D:\project\IED\mqtt2opcua_part2\mqtt_pub_test.json"
>  
>**LINUX**  
mosquitto_pub -h 192.168.1.84 -t test/topic -m '{"name":"MyVariable8","tag":"ns=2;s=MyVariable8","value":88,"quality":"Good","timestamp":"2024-10-02 23:38:02.425118"}'



