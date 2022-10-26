import xml.etree.ElementTree as ET
import mysql.connector
from mysql.connector import errorcode

tree = ET.parse('ddsserver.xml')
root = tree.getroot()


def prepare_channels_values(channel_dict):

    def_dict_channels = {'ID': 0, 'Enable': 0, 'DisplayName': 'NULL', 'Description': "NULL", 'DirectionType': 1,
                         'ThroughputAlarmLevel': 0, 'InactivityTimeOut': 0, 'ThroughputCapacityBPS': 0,
                         'Type': "NULL", 'StreamFormat': "NULL", 'GroupID': 0, 'Mode': 0, 'LocalAddr': "NULL",
                         'LocalPort': 0, 'TTL': 0, 'DestinationAddr': "NO", 'DestinationPort': "NULL",
                         'IpStSourceName': "NULL", 'IpStMulticastAddr': "NULL", 'IpStInterfaces': "NULL",
                         'DisableAlarm': "NULL", 'ReconnectTimeOut': "NULL", 'EnableKeepAlive': "TEXT",
                         'KeepAliveTime': 0, 'KeepAliveInterval': 0, 'KeepAliveProbes': 0, 'FMTPLocalID': "NULL",
                         'FMTPRemoteID': "NULL", 'Ti': 0, 'Tr': 0, 'Ts': 0}
    for name in def_dict_channels.keys():
        def_dict_channels[name] = channel_dict.get(name)
    return def_dict_channels


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
                    channels_list.append(prepare_channels_values(one_channel_dict))
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


def call_procedure_in_base(query, value):  # Connect to base and call procedure to update or insert data
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
        update_args = cursor.callproc(query, value)
        cnx.commit()
        cursor.close()
        cnx.close()
        return update_args


def update_channel_group():
    operations = 0
    updates = 0
    inserts = 0
    print("Operations in ChannelGroups table")
    for dict_for_sql in channel_group_parse('ChannelGroups'):  #
        values = []
        for value in dict_for_sql.values():
            values.append(value)
        if len(values) == 11:
            values.insert(3, "None")
        values.append(0)
        check_update_insert = call_procedure_in_base('ImportDDSChanelGroups', values)
        print("Operation {} executed with success".format(operations))
        if check_update_insert[12] == 1:
            inserts += 1
        elif check_update_insert[12] == 2:
            updates += 1
        operations += 1
    print("Execute {o} operations".format(o=operations))
    print("Inserted {i} rows".format(i=inserts))
    print("Updated {u} rows".format(u=updates))


def update_channels():
    operations = 0
    updates = 0
    inserts = 0
    print("Operations in Channels table")
    for dict_for_sql in channels_parse('Channels'):
        values = []
        for value in dict_for_sql.values():
            values.append(value)
        values.append(0)
        check_update_insert = call_procedure_in_base('ImportDDSChannels', values)
        operations += 1
        print("Operation {} executed with success".format(operations))
        if check_update_insert[31] == 1:
            inserts += 1
        elif check_update_insert[31] == 2:
            updates += 1
    print("Execute {k} operations".format(k=operations))
    print("Inserted {i} rows".format(i=inserts))
    print("Updated {u} rows".format(u=updates))


# update_channel_group()
update_channels()


