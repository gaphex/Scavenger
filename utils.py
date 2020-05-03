import os
import json


def read_lines(path):
    with open(path, "r") as fi:
        data = fi.read()
    lines = [l.strip() for l in data.strip().split("\n")]
    return lines
        
def mkdir_p(path):
    os.makedirs(path, exist_ok=True)

def safe_open_w(file):
    ''' Open "path" for writing, creating any parent directories as needed. '''
    mkdir_p(os.path.dirname(file))
    return open(file, 'w', encoding="utf-8")

def save_messages(msg_list, fpath):
    print("Saving to {}".format(fpath))
    with safe_open_w(fpath) as fo:
        json.dump(msg_list, fo)

def message_to_dict(message):
    reply_message_id = None
    if message.reply_to_message:
        reply_message_id = message.reply_to_message.message_id
    return dict(
        message_id=message.message_id,
        date=message.date,
        reply_message_id=reply_message_id,
        user_id= message.from_user.id if message.from_user else None,
        text=message.text
    )

def get_message_ids(mdicts):
    return [m['message_id'] for m in mdicts]