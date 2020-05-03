import os
import json


def read_lines(path):
    with open(path, "r") as fi:
        data = fi.read()
    lines = [l.strip() for l in data.strip().split("\n")]
    return lines

def save_messages(msg_list, fpath):
    os.makedirs(os.path.dirname(fpath), exist_ok=True)
    print("Saving to {}".format(fpath))
    with open(fpath, 'w', encoding="utf-8") as fo:
        json.dump(msg_list, fo, indent=4, ensure_ascii=False)

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
