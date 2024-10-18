# 3000 個資料每秒更新，publish 到 opcua client + publish mqtt 約延遲4秒

import paho.mqtt.client as mqtt
from datetime import datetime,timezone, timedelta
import queue,json, asyncio , logging, csv
from asyncua import ua, Server  
import os
import psutil

# config
LINUX = False  # Set to False for Windows
LINUX_PATH = os.path.expanduser('~/Project/IED/code')
WINDOWS_PATH = r'D:\project\IED\mqtt2opcua_part2'
LOG_FILE_NAME = 'log/opcua_trigger.log'
DATA_FILE_NAME = 'log/data.txt'
CSV_FILE_NAME = 'config/iec2opcua_mapping.csv'
LINUX_BROKER = "0.0.0.0"
WINDOWS_BROKER = '127.0.0.1'
MQTT_TOPIC = 'Topic/#'
OPC_UA_ENDPOINT_LINUX = 'opc.tcp://192.168.1.84:62640/IntegrationObjects/ServerSimulator'
OPC_UA_ENDPOINT_WINDOWS = 'opc.tcp://desktop-apkvnmm:62640/IntegrationObjects/ServerSimulator'

# system config
opc_ua_endpoint = OPC_UA_ENDPOINT_LINUX if LINUX else OPC_UA_ENDPOINT_WINDOWS
path = LINUX_PATH if LINUX else WINDOWS_PATH
mqtt_broker = LINUX_BROKER if LINUX else WINDOWS_BROKER

# Set CPU affinity
psutil.Process().cpu_affinity([1, 2, 3])

# Set up logging
log_file_path = os.path.join(path, LOG_FILE_NAME)
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler(log_file_path, mode='a'), logging.StreamHandler()])
logger = logging.getLogger("opcua_server")
logger.error("==================================================================")

# Initialize global variables   
message_queue = queue.Queue()
iec_to_opcua_mapping = {}
data_buffer = []

def on_message(client, userdata, message):
    try:
        msg = json.loads(message.payload)        
        if "Publisher" in msg:
            sub_topic = "test/publisher"
            client.publish(sub_topic, json.dumps(msg))
            logger.info(f"Republished message to topic: {sub_topic}")
        else:
            message_queue.put(msg["Content"])  # Put the message into the queue
    
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

# Load IEC to OPC UA mapping from CSV
def load_iec_to_opcua_mapping():
    """
    Load the mapping between IEC paths and OPC UA nodes from a CSV file
    """
    global iec_to_opcua_mapping
    try:
        with open(os.path.join(path,CSV_FILE_NAME), mode='r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                iec_to_opcua_mapping[row['IECPath']] = row['OpcuaNode']
    except Exception as e:
        logger.error(f"Error loading CSV file: {e}")

# Send data to OPC UA server
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
        
        local_timezone = timezone(timedelta(hours=8))  
        # server_timestamp = datetime.now(local_timezone)  # UTC+8 
        try:
            source_timestamp = datetime.strptime(data["SourceTime"], "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=local_timezone)
        except ValueError:
            source_timestamp = datetime.strptime(data["SourceTime"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=local_timezone)
        
        try:
            value_type = ua.VariantType.Float if data['Type'] == 'Float' else ua.VariantType.Int64
            value = float(data['Value']) if data['Type'] == 'Float' else int(data['Value'])                
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

        data_buffer.append([datetime.now(), data['IECPath'], data['Value']])
        # write to file every 1000 data 
        if len(data_buffer) > 1000:
            flush_data()

    except Exception as e:
        logger.warning(f"{data['IECPath']} Error sending to OPC UA: {e}")

# Flush buffered data to file
def flush_data():
    try:
        file_name = os.path.join(path,DATA_FILE_NAME)
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
        status_code_value = ua.DataValue(ua.StatusCodes.Good)
        await node.write_value(status_code_value)
        # return node
    except Exception as e:
        logger.error(f"Error occurred while creating MyVariable{i}: {e}")
        # return None  # Return None for subsequent processing

async def start_processing(server):
    await process_messages(server)

# Process messages from MQTT queue
async def process_messages(server):
    try:
        while True:
            await asyncio.sleep(0.5)  # Process messages every 0.1 seconds
            if not message_queue.empty():
                messages = []
                while not message_queue.empty():
                    # Batch process 
                    messages.append(message_queue.get())
                # logger.info(f"Processing {len(messages)} messages")

                for msg in messages:
                    try:
                        await send_to_opcua(server, msg)
                    except json.JSONDecodeError as e:
                        logger.error(f"Error decoding JSON: {e}")
                    except Exception as e:
                        logger.error(f"Error sending to OPC UA: {e}")
    except Exception as e:
        logger.error(f"Error processing messages: {e}")

# Start OPC UA server
async def start_opcua_server():    
    try:
        n_create_node = 10000+1
        server = Server()
        await server.init()
        server.set_endpoint(opc_ua_endpoint)  
        objects = await server.nodes.objects.add_object("ns=2;s=MyObjectNode", "MyObject")        
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
    load_iec_to_opcua_mapping()
    
    # Create MQTT client
    client = mqtt.Client(client_id="my_mqtt_client_2", clean_session=False)
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    # Connect to MQTT broker
    try:    
        client.connect(mqtt_broker, 1883, 60)
        client.subscribe(MQTT_TOPIC, qos=1)
        logger.info("Connected to MQTT broker and subscribed to topic {MQTT_TOPIC}.")
    except Exception as e:
        logger.error(f"Failed to connect or subscribe: {e}")

    client.loop_start()

    try:
        asyncio.run(start_opcua_server())  # Start OPC UA server
    except KeyboardInterrupt:
        logger.warning("Exiting...")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        client.loop_stop()  # Stop the event loop
        client.disconnect()  # Disconnect
        logger.warning("MQTT client disconnected.")