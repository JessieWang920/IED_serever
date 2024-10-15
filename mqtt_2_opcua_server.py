# 接收 新 JSON
# LINUX
# mosquitto_pub -h 192.168.1.84 -t test/topic -m '{"name":"MyVariable8","tag":"ns=2;s=MyVariable8","value":88,"quality":"Good","timestamp":"2024-10-02 23:38:02.425118"}'
# windows 
# mosquitto_pub -h 192.168.1.84 -t test/topic -m "{\"name\":\"MyVariable8\",\"tag\":\"ns=2;s=MyVariable8\",\"value\":88,\"quality\":\"Good\",\"timestamp\":\"2024-10-02 23:38:02.425118\"}"


import paho.mqtt.client as mqtt
from datetime import datetime, timezone, timedelta
import queue, json, asyncio, logging, csv
from asyncua import ua, Server
import os
import psutil



# Configuration
LINUX = False  # Set to False for Windows


LINUX_PATH = '~/Project/IED/code'
WINDOWS_PATH = r'D:\project\IED\mqtt2opcua_part2'
LOG_FILE_NAME = 'opcua_trigger.log'
DATA_FILE_NAME = 'data.txt'
MQTT_BROKER = '127.0.0.1'
MQTT_TOPIC = 'Topic/#'
OPC_UA_ENDPOINT_LINUX = 'opc.tcp://192.168.1.84:62640/IntegrationObjects/ServerSimulator'
OPC_UA_ENDPOINT_WINDOWS = 'opc.tcp://desktop-apkvnmm:62640/IntegrationObjects/ServerSimulator'
CSV_PATH = 'iec2opcua_mapping.csv'

opc_ua_endpoint = OPC_UA_ENDPOINT_LINUX if LINUX else OPC_UA_ENDPOINT_WINDOWS
# Set paths based on the operating system
if LINUX:
    path = os.path.expanduser(LINUX_PATH)
else:
    path = WINDOWS_PATH


# Set CPU affinity
psutil.Process().cpu_affinity([1, 2, 3])

# Set up logging
log_file_path = os.path.join(path, LOG_FILE_NAME)
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler(log_file_path, mode='a'), logging.StreamHandler()])
logger = logging.getLogger("opcua_server")

# Initialize global variables
message_queue = queue.Queue()
iec_to_opcua_mapping = {}
data_buffer = []

# Load IEC to OPC UA mapping from CSV
def load_iec_to_opcua_mapping(csv_path):
    """
    load csv file and create a dictionary of IECPath to OPC UA node mapping
    """
    global iec_to_opcua_mapping
    try:
        with open(os.path.join(path, csv_path), mode='r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                iec_to_opcua_mapping[row['IECPath']] = row['OpcuaNode']
    except Exception as e:
        logger.error(f"Error loading CSV file: {e}")

# MQTT callbacks
def on_message(client, userdata, message):
    try:
        msg = json.loads(message.payload)["Content"]
        message_queue.put(msg)
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

# Send data to OPC UA server
async def send_to_opcua(server, data):
    try:
        try:            
            node = server.get_node("ns=2;s="+iec_to_opcua_mapping[data['IECPath']])
        except Exception as e:
            logger.warning(f"Failed to get node: {e}")
            return
        
        local_timezone = timezone(timedelta(hours=8))
        # server_timestamp = datetime.now(local_timezone)  # 当前时间为 UTC+8

        try:
            source_timestamp = datetime.strptime(data["SourceTime"], "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=local_timezone)
        except ValueError:
            source_timestamp = datetime.strptime(data["SourceTime"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=local_timezone)
        try:
            value_type = ua.VariantType.Float if data['Type'] == 'Float' else ua.VariantType.Int64
            value = float(data['Value']) if data['Type'] == 'Float' else int(data['Value'])
            data_value = ua.DataValue(
                ua.Variant(value, value_type), 
                ua.StatusCode(int("0x0000" + data['Quality'], 16)), 
                source_timestamp)
        except ValueError as e:
            logger.warning(f"Failed to convert data value: {e}")
            return
        
        try:
            await node.write_value(data_value)
        except Exception as e:
            logger.warning(f"Failed to write value to node: {e}")
            return
        
        data_buffer.append([datetime.now(), data['IECPath'], data['Value']])
        # write to file every 1000 data points
        if len(data_buffer) > 1000:
            flush_data()

    except Exception as e:
        logger.warning(f"Error sending to OPC UA: {e}")

# Flush buffered data to file
def flush_data():
    try:
        file_name = os.path.join(path, DATA_FILE_NAME)
        with open(file_name, mode='a') as file:
            for row in data_buffer:
                file.write("\t".join(map(str, row)) + "\n")
        data_buffer.clear()
    except Exception as e:
        logger.error(f"Error flushing data: {e}")

# Create OPC UA variable node
async def create_node(objects, i):
    """
    Create a variable and set its writable properties
    """
    try:
        node = await objects.add_variable(f"ns=2;s=var{i}", f"var{i}", i)
        await node.set_writable()
        status_code_value = ua.DataValue(ua.Variant(ua.StatusCodes.Good))
        await node.write_value(status_code_value)
    except Exception as e:
        logger.error(f"Error occurred while creating MyVariable{i}: {e}")

# Process messages from MQTT queue
async def process_messages(server):
    while True:
        await asyncio.sleep(0.5)
        if not message_queue.empty():
            while not message_queue.empty():
                try:
                    await send_to_opcua(server, message_queue.get())
                except Exception as e:
                    logger.error(f"Error processing message: {e}")

# Start OPC UA server
async def start_opcua_server():
    server = Server()
    await server.init()
    server.set_endpoint(opc_ua_endpoint)
    objects = await server.nodes.objects.add_object("ns=2;s=MyObjectNode", "MyObject")
    await asyncio.gather(*(create_node(objects, i) for i in range(101)))
    logger.info(f"nodes created successfully")
    asyncio.create_task(process_messages(server))
    async with server:
        logger.info("OPC UA Server running...")
        while True:
            await asyncio.sleep(1)

# Main function
if __name__ == "__main__":
    load_iec_to_opcua_mapping(CSV_PATH)
    client = mqtt.Client(client_id="my_mqtt_client_2", clean_session=False)
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    try:
        client.connect(MQTT_BROKER, 1883, 60)
        client.subscribe(MQTT_TOPIC, qos=1)
        logger.info("Connected to MQTT broker and subscribed to topic.")
    except Exception as e:
        logger.error(f"Failed to connect or subscribe: {e}")
    client.loop_start()
    try:
        asyncio.run(start_opcua_server())
    except KeyboardInterrupt:
        logger.warning("Exiting...")
    finally:
        client.loop_stop()
        client.disconnect()
        logger.warning("MQTT client disconnected.")



