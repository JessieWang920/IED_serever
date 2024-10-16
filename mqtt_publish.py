import paho.mqtt.client as mqtt
from datetime import datetime
import json,time,psutil,threading,os, random
from concurrent.futures import ThreadPoolExecutor,as_completed
import pandas as pd

csv_file_path = r"D:\project\IED\mqtt2opcua_part2\iec2opcua_mapping.csv"

def get_iec_paths(csv_file_path):
    # 讀取 CSV 檔案
    df = pd.read_csv(csv_file_path)
    # 取得 IECPath 欄位的所有內容
    iec_paths = df['IECPath'].tolist()
    return iec_paths


# 創建兩個 MQTT 客戶端，分別連接到不同的 broker
def setup_mqtt_clients_multi():
    client_1 = mqtt.Client(client_id="mqtt_client_192_168_1_84", clean_session=False)
    client_2 = mqtt.Client(client_id="mqtt_client_localhost", clean_session=False)

    # 為每個客戶端設置 on_publish 回調函數
    client_1.on_publish = on_publish
    client_2.on_publish = on_publish

    try:
        # 連接到 192.168.1.84 的 MQTT broker
        client_1.connect("192.168.1.84", 1883, 60)
    except Exception as e:
        print(f"連線失敗到 192.168.1.84: {e}")

    try:
        # 連接到本地的 MQTT broker
        client_2.connect("127.0.0.1", 1883, 60)
    except Exception as e:
        print(f"連線失敗到 localhost: {e}")

    return client_1, client_2

def setup_mqtt_clients():
    client = mqtt.Client(client_id="mqtt_client_localhost", clean_session=False)
    client.on_publish = on_publish
    try:
        # 連接到本地的 MQTT broker
        client.connect("127.0.0.1", 1883, 60)
    except Exception as e:
        print(f"連線失敗到 localhost: {e}")

    return client


def publish_message(i,client):
    # datetime.now().second
    # sample 1
    # data_test = {
    # "name":f"MyVariable{i}",
    # "tag": f"ns=2;s=MyVariable{i}",
    # "value":sec,
    # "quality": "Good",
    # "timestamp": f"{datetime.now()}"
    # }
    data = {
    "Subscriber": "Topic/DI/P31025",
    "Content":
        {
        "IECPath":f"{i}", #"P31025$P31025Relay$Obj1CSWI1$Pos$stVal",
        "Type": "DP",
        "Value":f"{sec}",
        "Quality": "FFFF",
        "SourceTime":"2024-09-28 17:24:18.258"#f"{datetime.now()}"
        }
    }    
    # 將 JSON 轉換為字串
    text = json.dumps(data)
    # text_test = json.dumps(data_test)

    # clients[0].publish("test/topic", text_test, qos=qos_set)
    client.publish(data["Subscriber"], text, qos=qos_set)

    # for client in clients:
    #     try:
    #         client.publish(data["Subscriber"], text, qos=qos_set)
    #         # print(f"發送至 {client._client_id.decode()} 的消息：{text}")
    #     except Exception as e:
    #         print(f"發布消息時發生錯誤 (客戶端 {client._client_id.decode()}): {e}")


def publish_message_count_per_second (client):  
    with ThreadPoolExecutor(max_workers=4) as executor:
        # 隨機產生 1000筆1~10000之間
        # futures = [executor.submit(publish_message, random.randint(1, 10000)) for _ in range(1000)]
        # testing 前2000筆
        # futures = [executor.submit(publish_message, i, client) for i in range(2000)]
        # publish csv IECPath
        futures = [executor.submit(publish_message, i, client) for i in get_iec_paths(csv_file_path)]
        for future in as_completed(futures):
            try:
                result = future.result()
                # print(result)
            except Exception as e:
                print(f"執行時發生錯誤: {e}")


sec_flag = 0
def on_publish(client, userdata, mid):    
    global sec_flag
    if sec_flag != sec:
        print(sec)
        sec_flag = sec    
        print(f"[{client._client_id.decode()}] Message {mid} published successfully!")


if __name__ == '__main__':
    qos_set = 1
    client_id = "my_mqtt_client"
    sec = 0
    
    try:
        # 設置 MQTT 客戶端
        client = setup_mqtt_clients()
    except Exception as e:
        print(f"客戶端設置失敗: {e}")

    # 啟動循環以發布消息
    # for client in clients:
    client.loop_start()

    try:
        while True:
            publish_message_count_per_second(client)
            sec += 1
            time.sleep(0.7)
    except KeyboardInterrupt:
        print("程序被手動終止")
    except Exception as e:
        print(f"發生未預期的錯誤: {e}")


    # 停止循環並斷開連接
    # for client in clients:
    try:
        client.loop_stop()
        client.disconnect()
    except Exception as e:
            print(f"停止客戶端時發生錯誤: {e}")




# def check_system_status():
#     print(f"目前活跃的线程数量: {threading.active_count()}") #只包括主線程和一些系統線程
#     physical_cores = psutil.cpu_count(logical=False)
#     logical_processors = psutil.cpu_count(logical=True)
#     cpu_usage = psutil.cpu_percent(percpu=True)
#     print(f"物理核心数量: {physical_cores}")
#     print(f"逻辑处理器数量: {logical_processors}")
#     print(f"每个核心的使用率: {cpu_usage}")
#     max_workers = os.cpu_count()
#     print(f"建議最大工作线程数量: {max_workers}")



# def publish_message_count(flag,n=1000):
#     for i in range(n): #發送n筆資料
#         if flag == 0:
#                 publish_message(str(i))
#         else:
#             if i < 10:
#                 publish_message(str(flag) + "0" + str(i))
#             else:
#                 publish_message(str(flag) +str(i))