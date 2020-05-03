import time
import json
import sys
import argparse
import os

from pyrogram import Client
from pyrogram.errors import FloodWait, UsernameNotOccupied
from utils import read_lines, safe_open_w, save_messages, message_to_dict, get_message_ids


class Args:
    def __init__(self):
        self.phone = None
        self.phone_code = None
        self.data_dir = 'data'
        self.delay = 3.0
        self.session_name = 'account'
        self.api_config_path = "./config.ini"
        self.base_dir = "/Users/aphex/Developer/Scavenger"
        self.save_every = 5000
        self.target_file = "target.txt"
        
        target_path = os.path.join(self.base_dir, self.target_file)
        if os.path.exists(target_path):
            self.target = read_lines(target_path)
            print(f"Loaded targets : {self.target}")
        else:
            raise
        
        
def main():

    args = Args()
    app = Client(
        session_name=args.session_name,
        phone_code=args.phone_code,
        phone_number=args.phone,
        config_file=args.api_config_path
    )

    dirname = args.base_dir
    unknown_exceptions_count = 0
    scan_chats = args.target

    total_chats = len(scan_chats)
    with app:
        for current, target in enumerate(scan_chats):
            print("@{} processing...".format(target))

            save_result_path = os.path.join(dirname, args.data_dir, "{}.json".format(target))

            if os.path.exists(save_result_path):
                with open(save_result_path, "r") as fi:
                    messages = json.load(fi)
                offset_id = min(get_message_ids(messages))
                print(f"Loaded [{len(messages)}] samples, proceeding from message_id [{offset_id}]")
            else:
                messages = []
                offset_id = 0        

            while True:
                unknown_exceptions_count = 0
                try:
                    chat_history = app.get_history(target, offset_id=offset_id)
                    chat_history = chat_history[::-1]
                    time.sleep(args.delay)

                except FloodWait as e:
                    print("waiting {}".format(e.x))
                    time.sleep(e.x)
                    continue
                except UsernameNotOccupied as e:
                    print(f"Chat {target} does not exist")
                    break
                except Exception as e:
                    print(e)
                    print("Unknown exception, waiting 60 seconds.")
                    unknown_exceptions_count += 1
                    time.sleep(60)
                    continue
                except KeyboardInterrupt:
                    break

                if len(chat_history) == 0:
                    break

                mids = get_message_ids(chat_history)
                offset_id = min(mids)

                messages += map(message_to_dict, chat_history)

                total = len(set(get_message_ids(messages)))

                if not total%args.save_every:
                    print("Messages: {0} | @{1} - {2} of {3}".format(total, target, current + 1, total_chats))
                    save_messages(messages, save_result_path)

            save_messages(messages, save_result_path)

            
if __name__ == "__main__":
    main()