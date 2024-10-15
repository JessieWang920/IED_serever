import paho.mqtt.client as mqtt
from datetime import datetime
import json,time,psutil,threading,os, random
from concurrent.futures import ThreadPoolExecutor,as_completed



def publish_message(i):
    # datetime.now().second
    # sample 1
    # data = {
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
        "IECPath":"P31025$P31025Relay$Obj1CSWI1$Pos$stVal",
        "Type": "DP",
        "Value":f"{sec}",
        "Quality": "FFFF",
        "SourceTime":"2024-09-28 17:24:18.258"#f"{datetime.now()}"
        }
    }
    


    # print(sec)

    # 將 JSON 轉換為字串
    text = json.dumps(data)
    client.publish(data["Subscriber"], text, qos=qos_set)
    # print(f"\n發送{text}\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}")


def publish_message_count_per_second ():  
    with ThreadPoolExecutor(max_workers=4) as executor:
        # 隨機產生 1000筆1~10000之間
        # futures = [executor.submit(publish_message, random.randint(1, 10000)) for _ in range(1000)]
        # testing 前n筆
        futures = [executor.submit(publish_message, i) for i in range(2000)]

        for future in as_completed(futures):
            try:
                result = future.result()
                # print(result)
            except Exception as e:
                print(f"執行時發生錯誤: {e}")


a = 0
def on_publish(client, userdata, mid):    
    global a
    if a != sec:
        print(sec)
        a = sec
    
        print(f"Message {mid} published successfully!")

if __name__ == '__main__':
    qos_set = 1
    sec = 0
    client_id = "my_mqtt_client"
    try:
        client = mqtt.Client(client_id=client_id,clean_session=False)
        client.on_publish = on_publish
        client.connect("127.0.0.1", 1883, 60)
    except:
        print("連線失敗, IP error or broker not running.")

    client.loop_start()

    while True:
    # for i in range(30000):
        publish_message_count_per_second()

        sec += 1
        time.sleep(0.7)

    client.loop_stop()
    client.disconnect()




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