import os
import sys
import glob
import json
import datetime
import re
from collections import Counter
from collections import Counter

import pandas as pd
from matplotlib import pyplot as plt
import seaborn as sns
from nltk.corpus import stopwords


def break_combined_weeks(combined_weeks):
    """
    Breaks combined weeks into separate weeks.
    
    Args:
        combined_weeks: list of tuples of weeks to combine
        
    Returns:
        tuple of lists of weeks to be treated as plus one and minus one
    """
    plus_one_week = []
    minus_one_week = []

    for week in combined_weeks:
        if week[0] < week[1]:
            plus_one_week.append(week[0])
            minus_one_week.append(week[1])
        else:
            minus_one_week.append(week[0])
            plus_one_week.append(week[1])

    return plus_one_week, minus_one_week

def get_msgs_df_info(df):
    msgs_count_dict = df.user.value_counts().to_dict()
    replies_count_dict = dict(Counter([u for r in df.replies if r != None for u in r]))
    mentions_count_dict = dict(Counter([u for m in df.mentions if m != None for u in m]))
    links_count_dict = df.groupby("user").link_count.sum().to_dict()
    return msgs_count_dict, replies_count_dict, mentions_count_dict, links_count_dict



def get_messages_dict(msgs):
    msg_list = {
            "msg_id":[],
            "text":[],
            "attachments":[],
            "user":[],
            "mentions":[],
            "emojis":[],
            "reactions":[],
            "replies":[],
            "replies_to":[],
            "ts":[],
            "links":[],
            "link_count":[]
            }

    for index,msg in msgs.iterrows():
        if "subtype" not in msg:
            try:
                msg_list["msg_id"].append(msg["client_msg_id"])
            except:
                msg_list["msg_id"].append(None)
            msg_list["text"].append(msg["msg_content"])
            msg_list["user"].append(msg["sender_name"])
            msg_list["ts"].append(msg["Timestamp"])
            
            if "reactions" in msg:
                msg_list["reactions"].append(msg["reactions"])
            else:
                msg_list["reactions"].append(None)

            if "parent_user_id" in msg:
                msg_list["replies_to"].append(msg["ts"])
            else:
                msg_list["replies_to"].append(None)

            if "thread_ts" in msg and "reply_users" in msg:
                msg_list["replies"].append(msg["replies"])
            else:
                msg_list["replies"].append(None)
            
            if "blocks" in msg:
                emoji_list = []
                mention_list = []
                link_count = 0
                links = []
                
                for blk in msg["blocks"]:
                    if "elements" in blk:
                        for elm in blk["elements"]:
                            if "elements" in elm:
                                for elm_ in elm["elements"]:
                                    
                                    if "type" in elm_:
                                        if elm_["type"] == "emoji":
                                            emoji_list.append(elm_["name"])

                                        if elm_["type"] == "user":
                                            mention_list.append(elm_["user_id"])
                                        
                                        if elm_["type"] == "link":
                                            link_count += 1
                                            links.append(elm_["url"])


                msg_list["emojis"].append(emoji_list)
                msg_list["mentions"].append(mention_list)
                msg_list["links"].append(links)
                msg_list["link_count"].append(link_count)
            else:
                msg_list["emojis"].append(None)
                msg_list["mentions"].append(None)
                msg_list["links"].append(None)
                msg_list["link_count"].append(0)
    
    return msg_list

def from_msg_get_replies(msg):
    replies = []
    if "thread_ts" in msg and "replies" in msg:
        try:
            for reply in msg["replies"]:
                reply["thread_ts"] = msg["thread_ts"]
                reply["message_id"] = msg["client_msg_id"]
                replies.append(reply)
        except:
            pass
    return replies

def msgs_to_df(msgs):
    msg_list = get_messages_dict(msgs)
    df = pd.DataFrame(msg_list)
    return df

def process_msgs(msg):
    '''
    select important columns from the message
    '''

    keys = ["client_msg_id", "type", "text", "user", "ts", "team", 
            "thread_ts", "reply_count", "reply_users_count"]
    msg_list = {k:msg[k] for k in keys}
    rply_list = from_msg_get_replies(msg)

    return msg_list, rply_list

def get_messages_from_channel(channel_path):
    '''
    get all the messages from a channel        
    '''
    channel_json_files = os.listdir(channel_path)
    channel_msgs = [json.load(open(channel_path + "/" + f)) for f in channel_json_files]

    df = pd.concat([pd.DataFrame(get_messages_dict(msgs)) for msgs in channel_msgs])
    print(f"Number of messages in channel: {len(df)}")
    
    return df


def convert_2_timestamp(column, data):
    """convert from Unix time to Pandas timestamp
        args:
            column: column that needs to be converted to timestamp
            data: data that has the specified column
    """
    if column in data.columns.values:
        timestamps = []
        for time_unix in data[column]:
            if time_unix == 0:
                timestamps.append(pd.Timestamp('1970-01-01 00:00:00'))
            else:
                timestamp = pd.to_datetime(time_unix, unit='s')
                timestamps.append(timestamp)
        return timestamps
    else:
        print(f"{column} not in data")

def most_frequent_time_range(column, data, interval_minutes=30):
    """Find the time range during which the most frequent messages were sent.
        args:
            column: column that needs to be converted to timestamp
            data: data that has the specified column
            interval_minutes: time interval in minutes for binning
    """
    if column not in data.columns:
        print(f"Column '{column}' not found in the data.")
        return None

    if data.empty:
        print("No data found.")
        return None

    data['Timestamp'] = pd.to_datetime(data[column], unit='s')
    
    # Create a new column representing time intervals
    data['TimeInterval'] = (data['Timestamp'].dt.hour * 60 + data['Timestamp'].dt.minute) // interval_minutes

    # Count occurrences of each time interval
    time_interval_counts = data['TimeInterval'].value_counts()

    if time_interval_counts.empty:
        print("No valid time intervals found.")
        return None

    # Identify the time interval with the maximum count
    most_frequent_interval = time_interval_counts.idxmax()

    # Calculate start and end times of the most frequent time range
    start_time = pd.to_datetime(f"{most_frequent_interval * interval_minutes}min", format='%H%M')
    end_time = start_time + pd.Timedelta(minutes=interval_minutes)

    return start_time, end_time

def get_tagged_users(df):
    """get all @ in the messages"""

    return df['msg_content'].map(lambda x: re.findall(r'@U\w+', x))