import xml.etree.ElementTree as ET
import mysql.connector
from mysql.connector import errorcode

tree = ET.parse('ddsserver.xml')
root = tree.getroot()


def channel_group_parse(type_of_group):  # function to parse channels group from file and return list with dictionary.
    channel_group_list = []  # In one  dict one channel group
    for child in root.findall("n"):
        if child.attrib['n'] == type_of_group:
            for child_2 in child.findall("n"):
                channels = [child_2.get("n")]
                if child_2.attrib['n'] in channels:
                    one_channel_dict = {}
                    for child_3 in child_2.findall("n"):
                        one_channel_dict[child_3.get('n')] = child_3.get("v")
                    channel_group_list.append(one_channel_dict)
    return channel_group_list


def channels_parse(type_of_group):  # function to parse channels group from file and return list with dictionary.
    channels_list = []  # In one  dict one channel group. List of dict channels
    for child in root.findall("n"):
        if child.attrib['n'] == type_of_group:
            for child_2 in child.findall("n"):
                channels = [child_2.get("n")]
                if child_2.attrib['n'] in channels:
                    one_channel_dict = {}
                    for child_3 in child_2.findall("n"):
                        one_channel_dict[child_3.get('n')] = child_3.get("v")
                        for child_4 in child_3.findall("n"):
                            for child_5 in child_4.findall("n"):
                                one_channel_dict["Destination" + child_5.get('n')] = child_5.get(
                                    "v")  # add Destinationaddr and port to dict.
                    one_channel_dict.pop('Destination', None)  # Delete Destination key
                    channels_list.append(one_channel_dict)

    return channels_list


channel_group_table_query = """
    CREATE TABLE ChannelGroups(
    ID INT,
    Enable INT,
    DisplayName TEXT,
    DirectionType TEXT,
    CheckDuplicates INT,
    CacheTimeout_ms INT,
    CacheSkipTimeout_ms INT,
    StandAlone INT,
    EnableRecording INT,
    MaxRecordingDays INT,
    MaxRecordingCapacity DOUBLE,
    Description TEXT
)
"""

channels_table_query = """
CREATE TABLE Channels(
    ID INT,
    Enable INT,
    DisplayName TEXT,
    Description TEXT,
    DirectionType INT,
    ThroughputAlarmLevel INT,
    InactivityTimeOut INT,
    ThroughputCapacityBPS DOUBLE,
    Type TEXT,
    StreamFormat TEXT,
    GroupID INT,
    Mode INT,
    LocalAddr TEXT,
    LocalPort DOUBLE,
    TTL INT,
    DestinationAddr TEXT,
    DestinationPort TEXT,
    IpStSourceName TEXT,
    IpStMulticastAddr TEXT,
    IpStInterfaces TEXT,
    DisableAlarm TEXT,
    ReconnectTimeOut TEXT,
    EnableKeepAlive TEXT,
    KeepAliveTime INT,
    KeepAliveInterval INT,
    KeepAliveProbes INT,
    FMTPLocalID TEXT,
    FMTPRemoteID TEXT,
    Ti INT,
    Tr INT,
    Ts INT
)
"""


def insert_query_to_base(query, value):  # Connect to base and execute query
    try:
        cnx = mysql.connector.connect(user='root',
                                      password='root',
                                      database='base',
                                      host='192.168.2.131')
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    else:
        cursor = cnx.cursor()
        cursor.execute(query, value)
        cnx.commit()
        cursor.close()
        cnx.close()


def insert_query(query):  # Connect to base and execute query
    try:
        cnx = mysql.connector.connect(user='root',
                                      password='root',
                                      database='base',
                                      host='192.168.2.131')
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    else:
        cursor = cnx.cursor()
        cursor.execute(query)
        cnx.commit()
        cursor.close()
        cnx.close()


# Search in make_group_lst and insert to SQL


def xml_branch_to_sql(branch_name):
    count = 0
    if branch_name == "Channels":
        for dict_for_sql in channels_parse(branch_name):
            values = []
            for value in dict_for_sql.values():
                values.append(value)
            placeholders = ', '.join(['%s'] * len(dict_for_sql))
            columns = ', '.join(dict_for_sql.keys())
            sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % (branch_name, columns, placeholders)
            insert_query_to_base(sql, values)
            count += 1
        print("Added {k} rows".format(k=count))
    elif branch_name == "ChannelGroups":
        for dict_for_sql in channel_group_parse(branch_name):
            values = []
            for value in dict_for_sql.values():
                values.append(value)
            placeholders = ', '.join(['%s'] * len(dict_for_sql))
            columns = ', '.join(dict_for_sql.keys())
            sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % (branch_name, columns, placeholders)
            insert_query_to_base(sql, values)
            count += 1
        print("Added {k} rows".format(k=count))


insert_query(channels_table_query)
insert_query(channel_group_table_query)
xml_branch_to_sql('ChannelGroups')
xml_branch_to_sql("Channels")