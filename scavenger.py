import yaml
import time
import json
import sys
import argparse
import os
import logging

from pyrogram import Client
from pyrogram.errors import FloodWait, UsernameNotOccupied

from utils import read_lines, save_messages, message_to_dict, get_message_ids
from get_threads import export_threads
from multiprocessing import Pool, Process, current_process


logger = logging.getLogger("pyrogram")
logger.setLevel(logging.ERROR)
logger.handlers = []


def load_yaml(f):
    with open(f) as fi:
        o = yaml.load(fi)
    return o

def get_local_rank(n_workers):
    rank = int(current_process()._identity[0])%n_workers
    return rank


class Args:
    def __init__(self, app_config, worker):
        
        self.app_config = app_config
        self.phone = None
        self.phone_code = None
        
        self.worker_id = worker
        self.delay = 3.0
        self.data_dir = 'data'
        self.print_every = 1000
        self.save_every = 5000
        self.target_file = "target.txt"

        self.session_name = f"session_{self.worker_id}"
        self.session_dir = "sessions"
        self.base_dir = "/home/gaphex/Scavenger"
        
        keys_auth = list(self.app_config['api_keys'].keys())
        self.n_workers = len(keys_auth)
        
        self.my_key_src = keys_auth[self.worker_id - 1]
        self.api_hash = self.app_config['api_keys'][self.my_key_src]['api_hash']
        self.api_id = self.app_config['api_keys'][self.my_key_src]['api_id']        
        
        self.workdir_path = os.path.join(self.base_dir, self.session_dir)
        self.target_path = os.path.join(self.base_dir, self.target_file)
         
            
def main(scan_chats):
    
    app_config = load_yaml("app_config.yml")
    n_workers = len(app_config['api_keys'])
    
    local_rank = get_local_rank(n_workers)

    args = Args(app_config, local_rank)
    app = Client(
        session_name=args.session_name,
        phone_code=args.phone_code,
        phone_number=args.phone,
        api_hash=args.api_hash,
        api_id=args.api_id,
        workdir=args.workdir_path
    )

    dirname = args.base_dir
    unknown_exceptions_count = 0
    if type(scan_chats) is not list:
        scan_chats = [scan_chats]
    total_chats = len(scan_chats)
    exit = False
    
    with app:

        for current, target in enumerate(scan_chats):
            if exit:
                break
                
            print(f"worker [{local_rank}]: begins processing [{target}]")

            save_result_path = os.path.join(dirname, args.data_dir, "{}.json".format(target))

            if os.path.exists(save_result_path):
                with open(save_result_path, "r") as fi:
                    messages = json.load(fi)
                offset_id = min(get_message_ids(messages))
                print(f"worker [{local_rank}]: loaded [{len(messages)}] samples, "
                      f"proceeding from message_id [{offset_id}]")
            else:
                messages = []
                offset_id = 0        

            while True:
                chat_history = []
                unknown_exceptions_count = 0
                try:
                    chat_history = app.get_history(target, offset_id=offset_id)
                    chat_history = chat_history[::-1]
                    time.sleep(args.delay)
                except UsernameNotOccupied as e:
                    print(f"worker [{local_rank}]: chat [{target}] does not exist, skipping .")
                    break
                except Exception as e:
                    print(f"worker [{local_rank}]: unknown exception [{e}], waiting 60 seconds .")
                    unknown_exceptions_count += 1
                    time.sleep(60)
                    continue
                except KeyboardInterrupt:
                    print(f"worker [{local_rank}]: caught KeyboardInterrupt, saving and exiting .")
                    exit = True
                    break

                if len(chat_history) == 0:
                    print(f"worker [{local_rank}]: finished processing [{target}]")
                    break

                try:
                    mids = get_message_ids(chat_history)
                    offset_id = min(mids)

                    messages += map(message_to_dict, chat_history)
                    total = len(set(get_message_ids(messages)))

                    if not total%args.print_every:
                        print(f"worker [{local_rank}]: got [{total}] messages @ offset [{offset_id}] | "
                              f"@{target} - {current + 1} of {total_chats}")

                    if not total%args.save_every and not exit:
                        save_messages(messages, save_result_path)
                        print(f"worker [{local_rank}]: saving to {save_result_path}")
                except KeyboardInterrupt:
                    print(f"worker [{local_rank}]: caught KeyboardInterrupt, saving and exiting .")
                    exit = True
                    break
            
        if len(chat_history) > 0:
            save_messages(messages, save_result_path)
            print(f"worker [{local_rank}]: saving to {save_result_path}")

if __name__ == "__main__":
    targets = read_lines("target.txt")
    p = Pool(processes=2)
    p.map(main, targets, 1)

