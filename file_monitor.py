import os, time, subprocess
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import tkinter as tk
from tkinter import messagebox
import psutil
import platform
import sys 

logger = logging.getLogger(__name__)

class FileModifiedEventHandler(FileSystemEventHandler):
    def __init__(self, target_file_path, script_path):
        self.target_file_path = target_file_path
        self.script_path = script_path
        self.previous_file_size = os.path.getsize(target_file_path)

    def on_modified(self, event):
        try:
            logger.info(f"File modified : {event.src_path}")
            if event.src_path == self.target_file_path:
                time.sleep(0.5)
                current_file_size = os.path.getsize(self.target_file_path)
                if current_file_size != self.previous_file_size:
                    self.previous_file_size = current_file_size
                    self.prompt_to_restart_script()
        except Exception as e:
            logger.error(f"Error during file modification handling: {e}")

    def prompt_to_restart_script(self):
        try:
            try:
                root = tk.Tk()
                root.withdraw()
                root.attributes('-topmost', True)
                result = messagebox.askyesno("File Changed", "The file has been changed. Do you want to re-run the script?")
            except Exception as e:
                logger.error(f"LINUX can't show tkinter prompt, using input() instead")
                response = input("The file has been changed. Do you want to re-run the script? (yes/no): ")
                if response.lower() in ['yes', 'y']:
                    result = True
                    logger.info("User chose to re-run the script")
                else:
                    result = False
                    logger.info("User chose not to re-run the script")
            
            if result:
                # close existing process and restart script
                self.terminate_existing_process()
                # start script only
                # self.start_script()
        except Exception as e:
            logger.error(f"Error during prompting to restart script: {e}")

    def terminate_existing_process(self):
        try:
            if os.path.exists(lock_file_path):
                with open(lock_file_path, "r") as file:
                    pid = int(file.read().strip())

                if psutil.pid_exists(pid):
                    process = psutil.Process(pid)
                    process.terminate()
                    process.wait(timeout=2)
                    logger.info(f"Process PID: {pid} terminated successfully.")
                else:
                    logger.warning(f"PID: {pid} does not exist, process may have already ended.")
            else:
                logger.warning("Lock file not found.")
        except (FileNotFoundError, ValueError, psutil.NoSuchProcess, psutil.AccessDenied, psutil.TimeoutExpired) as e:
            logger.error(f"Error during process termination: {e}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        finally:
            self.start_script()

    def start_script(self):
        try:
            logger.info("Starting the script...")
            if LINUX:
                logger.info('close mqtt_server screen')
                os.system('screen -S mqtt_server -X quit')
                logger.info('start mqtt_server and create screen')
                os.system(f'screen -dmS mqtt_server /home/user/Software/Python/IED_env/bin/python {server_script_path}')
            else:
                logger.info('try startfile ')
                os.startfile(self.script_path)
        except Exception as e:
            logger.error(f"Error during starting the script: {e}")

def watch_file(target_file_path, script_path):
    try:
        logger.info(f"Watching file: {target_file_path}")
        event_handler = FileModifiedEventHandler(target_file_path, script_path)
        observer = Observer()
        observer.schedule(event_handler, path=os.path.dirname(target_file_path), recursive=False)
        observer.start()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    except Exception as e:
        logger.error(f"Error during file watching setup: {e}")
    finally:
        observer.join()

if __name__ == "__main__":
    # config
    LINUX_PATH = os.path.expanduser('~/Project/IED_server')
    WINDOWS_PATH = r'D:\project\IED\mqtt2opcua_part2'
    # system
    LINUX = platform.system() == "Linux"
    path = LINUX_PATH if LINUX else WINDOWS_PATH
    # config
    
    lock_file_path= os.path.join(path, 'log','pid.lock')
    target_file_name = os.path.join(path,'config','iec2opcua_mapping.csv')
    server_script_path = os.path.join(path, 'mqtt_2_opcua_server.py')
    log_file_path = os.path.join(path, 'log','file_monitor.log')


    logging.basicConfig(level=logging.INFO, 
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        handlers=[logging.FileHandler(log_file_path, mode='a'), logging.StreamHandler()])
    try:
        watch_file(target_file_name, server_script_path)
    except Exception as e:
        logger.error(f"Error in main execution: {e}")

