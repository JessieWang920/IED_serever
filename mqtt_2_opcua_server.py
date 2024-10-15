# 接收 新 JSON
# LINUX
# mosquitto_pub -h 192.168.1.84 -t test/topic -m '{"name":"MyVariable8","tag":"ns=2;s=MyVariable8","value":88,"quality":"Good","timestamp":"2024-10-02 23:38:02.425118"}'
# windows 
# mosquitto_pub -h 192.168.1.84 -t test/topic -m "{\"name\":\"MyVariable8\",\"tag\":\"ns=2;s=MyVariable8\",\"value\":88,\"quality\":\"Good\",\"timestamp\":\"2024-10-02 23:38:02.425118\"}"

import paho.mqtt.client as mqtt
from datetime import datetime,timezone, timedelta
import queue,json, asyncio , logging, csv
from asyncua import ua, Server  
import os
# LINUX
# path = os.path.expanduser('~/Project/IED/code')
path = r"D:\project\IED\mqtt2opcua_part2"


import psutil
p = psutil.Process()
p.cpu_affinity([1,2,3])
affinity = p.cpu_affinity()

# Set up logging
log_file_path = os.path.join(path, 'opcua_trigger.log')
logger = logging.getLogger("opcua_server")
logger.setLevel(logging.DEBUG)
# File handler for log file
file_handler = logging.FileHandler(log_file_path)
file_handler.setLevel(logging.WARNING)  # Log only WARNING and above to the file
file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)
# Console handler for cmd output
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)  # Log DEBUG and above to the console
console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)


message_queue = queue.Queue()

def on_message(client, userdata, message):
    try:
        msg = json.loads(message.payload)["Content"]
        message_queue.put(msg)  # Put the message into the queue
        # logger.info(f"Message received: {msg}")

    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode message: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in on_message: {e}")

def on_disconnect(client, userdata, rc):
    logger.info(f"Disconnected with result code {rc}")
    if rc != 0:
        logger.info("Unexpected disconnection. Trying to reconnect...")
        try:
            client.reconnect()
        except Exception as e:
            logger.error(f"Error while reconnecting: {e}")

iec_to_opcua_mapping = {}
def load_iec_to_opcua_mapping(csv_path):
    """
    Load the mapping between IEC paths and OPC UA nodes from a CSV file
    """
    global iec_to_opcua_mapping
    try:
        with open(csv_path, mode='r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                iec_path = row['IECPath']
                opcua_node = row['OpcuaNode']
                iec_to_opcua_mapping[iec_path] = opcua_node
    except Exception as e:
        print(f"Error loading CSV file: {e}")

csv_path = r"./iec2opcua_mapping.csv"
load_iec_to_opcua_mapping(csv_path)


a = []
async def send_to_opcua(server, data):
    """
    Process messages in the queue and send them to OPC UA
    """
    try:
        try:            
            node = server.get_node("ns=2;s="+iec_to_opcua_mapping[data['IECPath']])
        except Exception as e:
            logger.warning(f"Failed to get node: {e}")
            return
        
        local_timezone = timezone(timedelta(hours=8))  # 设定时区为 UTC+8
        # server_timestamp = datetime.now(local_timezone)  # 当前时间为 UTC+8
        try:
            source_timestamp = datetime.strptime(data["SourceTime"], "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=local_timezone)
        except ValueError:
            source_timestamp = datetime.strptime(data["SourceTime"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=local_timezone)
        
        try:
            if data['Type'] == 'Float':
                value = float(data['Value'])
                value_type = ua.VariantType.Float
            else:
                value = int(data['Value'])
                value_type = ua.VariantType.Int64
                
            data_value = ua.DataValue(
                ua.Variant(value, value_type),  # Set variable type
                ua.StatusCode(int("0x0000" + data['Quality'], 16)),  # Set StatusCode
                source_timestamp
            )
        except ValueError as e:
            logger.warning(f"Failed to convert data value: {e}")
            return

        try:
            await node.write_value(data_value)
        except Exception as e:
            logger.warning(f"Failed to write value to node: {e}")
            return

        a.append([datetime.now(), data['IECPath'], data['Value']])

        if len(a) > 1000:
            file_name = os.path.join(path, 'data.txt')
            with open(file_name, mode='a') as file:
                # Append the accumulated data
                for row in a:
                    file.write("\t".join(map(str, row)) + "\n")  # 將資料寫入檔案
            a.clear()

    except Exception as e:
        logger.warning(f"{data['IECPath']} Error sending to OPC UA: {e}")


async def create_node(objects, i):
    """
    Create a variable and set its writable properties
    """
    try:
        # Create variable
        node = await objects.add_variable(f"ns=2;s=var{i}", f"var{i}", i)
        # Set writable properties
        await node.set_writable()
        # await node.set_attribute(ua.AttributeIds.StatusCode, ua.DataValue(ua.Variant(ua.StatusCodes.Good)))
        status_code_value = ua.DataValue(ua.Variant(ua.StatusCodes.Good))
        await node.write_value(status_code_value)
        return node
    except Exception as e:
        logger.error(f"Error occurred while creating MyVariable{i}: {e}")
        return None  # Return None for subsequent processing
    
async def start_processing(server):
    await process_messages(server)

async def process_messages(server):
    while True:
        await asyncio.sleep(0.5)  # Process messages every 0.1 seconds
        if not message_queue.empty():
            messages = []
            while not message_queue.empty():
                # Batch process 
                messages.append(message_queue.get())
            # 批量处理消息
            # logger.info(f"Processing {len(messages)} messages")

            for msg in messages:
                try:
                    await send_to_opcua(server, msg)
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON: {e}")
                except Exception as e:
                    print(f"Error sending to OPC UA: {e}")




client_id = "my_mqtt_client_2"
# Create MQTT client
client = mqtt.Client(client_id=client_id, clean_session=False)
client.on_message = on_message
client.on_disconnect = on_disconnect
# Connect to MQTT broker
try:
    
    client.connect("127.0.0.1", 1883, 60)
    # Linux
    # client.connect("0.0.0.0", 1883, 60)
    logger.info("Connected to MQTT broker.")
except Exception as e:
    logger.error(f"Failed to connect to MQTT broker: {e}")

try:
    topic = "Topic/#"
    client.subscribe(topic, qos=1)
    logger.info(f"Subscribed to {topic}.")
except Exception as e:
    logger.error(f"Failed to subscribe to topic: {e}")


# Start MQTT client loop
client.loop_start()
from concurrent.futures import ProcessPoolExecutor
# Create OPC UA server
async def start_opcua_server():
    server = Server()
    try:
        await server.init()

        server.set_endpoint("opc.tcp://desktop-apkvnmm:62640/IntegrationObjects/ServerSimulator")  
        # LINUX
        # server.set_endpoint("opc.tcp://192.168.1.84:62640/IntegrationObjects/ServerSimulator")

        custom_node_id = f"ns=2;s=MyObjectNode"

        objects = await server.nodes.objects.add_object(custom_node_id, "MyObject")
        n_create_node = 100+1
        await asyncio.gather(*(create_node(objects, i) for i in range(n_create_node)))
        logger.info(f"{n_create_node} node created successfully")

        print("OPC UA Server started.")
        asyncio.create_task(start_processing(server))

        async with server:
            print("OPC UA Server running...")
            while True:
                await asyncio.sleep(1)  # Keep the server running
    except Exception as e:
        logger.error(f"Error starting OPC UA server: {e}")


if __name__ == "__main__":
    try:
        asyncio.run(start_opcua_server())  # Start OPC UA server
    except KeyboardInterrupt:
        logger.warning("Exiting MQTT client...")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        client.loop_stop()  # Stop the event loop
        client.disconnect()  # Disconnect
        logger.warning("MQTT client disconnected.")