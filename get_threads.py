import pandas as pd
import json

from tensorflow.keras.utils import Progbar
from glob import glob
from collections import defaultdict
from utils import save_messages


EXPORT_PATH = "./export.json"
GLOB = "./data/*.json"
MSG_KEY = "messages"
IDS_KEY = "ids"
MID_KEY = "message_id"
RID_KEY = "reply_message_id"
TXT_KEY = "text"


def get_thread(message, current_thread, i2m):
    if current_thread is None:
        current_thread = []
        
    msg_id = message[MID_KEY]
    reply_id = message[RID_KEY]
    do_return = False
    
    if reply_id is None or reply_id not in i2m or msg_id in current_thread:
        do_return = True
    else:
        current_thread.append(msg_id)
        
    if do_return:
        return current_thread[::-1]
    else:
        next_msg = i2m[reply_id]
        return get_thread(next_msg, current_thread, i2m)

def get_threads(filename, min_len=3):
    with open(filename) as fi:
        dataset = json.load(fi)
        
    id2msg = {k[MID_KEY]: k for i,k in enumerate(dataset)}
    
    threads = []
    for msg in dataset:
        thr = get_thread(msg, None, id2msg)
        if len(thr) > min_len:
            threads.append(thr)
            
    threads = deduplicate_threads(threads)
    msg_threads = [[id2msg[m][TXT_KEY] for m in thread] 
                   for thread in threads]
    
    return msg_threads


def already_exists(seq, filtered_seqs):
    exists = False
    for testseq in filtered_seqs:
        if len(testseq) >= len(seq):
            if seq == testseq[:len(seq)]:
                exists = True
    return exists

def deduplicate_threads(threads):
    ids = threads
    start2id = defaultdict(list)
    for i, k in enumerate(ids):
        start2id[k[0]].append(i)

    filtered = []
    for k in start2id:
        sub_filtered = []
        for test_seq in sorted([ids[s] for s in start2id[k]], 
                               key = lambda x: len(x))[::-1]:
            if not already_exists(test_seq, sub_filtered):
                sub_filtered.append(test_seq)
        filtered += sub_filtered
        
    return filtered

def main():
    all_threads = []
    chats = glob(GLOB)
    bar = Progbar(len(chats))

    for shard in chats:
        all_threads += get_threads(shard)
        bar.add(1)
        
    save_messages(all_threads, EXPORT_PATH)


if __name__ == "__main__":
    main()
