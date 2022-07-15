"""
TxRealm Simulator for RxRealm Outdoor Testing on NSF POWDER

Author: Bharath Keshavamurthy <bkeshava@purdue.edu>
Organization: School of Electrical & Computer Engineering, Purdue University, West Lafayette, IN.
Copyright (c) 2021. All Rights Reserved.
"""

# The imports
import time
import json
import traceback
from enum import Enum
from typing import Any
from datetime import datetime
from kafka import KafkaProducer
from dataclasses import dataclass, asdict

"""
General Project Odin Enumerations
"""


class Units(Enum):
    """
    An enumeration listing all possible units for GPSEvent members
    """
    METERS = 0
    CENTIMETERS = 1
    MILLIMETERS = 2
    DEGREES = 3
    MINUTES = 4
    SECONDS = 5
    INCHES = 6
    FEET = 7
    YARDS = 8
    DIMENSIONLESS = 9
    UNKNOWN = 10


class FixType(Enum):
    """
    An enumeration listing all possible fix-types supported by the GPS receiver
    """
    NO_FIX = 0
    DEAD_RECKONING = 1
    TWO_DIMENSIONAL = 2
    THREE_DIMENSIONAL = 3
    GNSS = 4
    TIME_FIX = 5


class CarrierSolutionType(Enum):
    """
    An enumeration listing all possible carrier solution-types supported by the GPS receiver
    """
    NO_SOLUTION = 0
    FLOAT_SOLUTION = 1
    FIXED_SOLUTION = 2


@dataclass(order=True)
class Member:
    """
    Secondary information tier in GPSEvent

    A core member which encapsulates highly specific details about latitude, longitude, altitude, speed, heading, and
    other components in the primary information tier of GPSEvent
    """
    is_high_precision: bool = False
    main_component: float = 0.0
    high_precision_component: float = 0.0
    component: float = 0.0
    precision: float = 0.0
    units: Units = Units.DIMENSIONLESS


"""
Core TxRealm Simulation Routines
"""


# The Core GPSEvent Member Dataclass
@dataclass(order=True)
class GPSEvent:
    """
    Primary information tier in GPSEvent

    The GPSEvent class: ODIN_GPS_EVENT communicated over the ODIN_GPS_EVENTS Kafka topic encapsulates this data object

    DESIGN NOTES:
    1. Non-core members include those that are not part of the NMEA message from uC-GPS: seq_number and timestamp;
    2. Update the core_length and total_length parameters when you add/delete core/non-core members to this dataclass
       (think of this as the trailer of a comm packet);
    3. In situations where in the interfaced uC has low-memory problems, the "reduced" flag triggers population and use
       of only a subset of the members listed below (e.g., reduced = 1 ==> ([seq_number, timestamp], is_gnss_fix_ok,
       siv, fix_type, carrier_solution_type, latitude (x3), longitude (x3), altitude_ellipsoid (x3), altitude_msl (x3))
    """
    seq_number: int = 0
    timestamp: str = str(datetime.utcnow())
    is_gnss_fix_ok: bool = True
    siv: int = 25
    fix_type: FixType = FixType.THREE_DIMENSIONAL
    carrier_solution_type: CarrierSolutionType = CarrierSolutionType.FLOAT_SOLUTION
    latitude: Member = Member(component=40.76617367)
    longitude: Member = Member(component=-111.84793933)
    altitude_ellipsoid: Member = Member(component=1451.121)
    altitude_msl: Member = Member()
    speed: Member = Member()
    heading: Member = Member()
    horizontal_acc: Member = Member()
    vertical_acc: Member = Member()
    speed_acc: Member = Member()
    heading_acc: Member = Member()
    ned_north_vel: Member = Member()
    ned_east_vel: Member = Member()
    ned_down_vel: Member = Member()
    pdop: Member = Member()
    mag_acc: Member = Member()
    mag_dec: Member = Member()
    geometric_dop: Member = Member()
    position_dop: Member = Member()
    time_dop: Member = Member()
    horizontal_dop: Member = Member()
    vertical_dop: Member = Member()
    northing_dop: Member = Member()
    easting_dop: Member = Member()
    horizontal_accuracy: Member = Member()
    vertical_accuracy: Member = Member()
    # The members listed below are not considered for packet/event length determination
    ultra_core_length: int = 16
    core_length: int = 37
    total_length: int = 39


# Default JSON Representation for Non-Serializable Objects
def default_json(t: Any) -> str:
    """
    Get the formatted string representation of a data object in Project Odin that is not JSON serializable

    Args:
        t: The data object in Project Odin that is not JSON serializable

    Returns: The formatted string representation of the provided non-JSON-serializable data object
    """
    return f'{t}'


# File name determination
def get_file_name_for_events(seq_number: int, key: str) -> str:
    """
    Get the event log file name in accordance with the specified key and the event sequence number

    Args:
        seq_number: The event sequence number that is incorporated into the log file's name
        key: The key (an identifier tagged with this event) that is incorporated into the log file's name

    Returns: The event log file name w.r.t the specified key and the event sequence number
    """
    return ''.join(['E:/Odin/logs/rx-realm/controller/simulator-logs/', key, '_', 'event_', str(seq_number), '.json'])


# JSON Representation Routine for GPS Events
def json_repr_for_events(dataclass_obj: dataclass, seq_number: int, key: str) -> str:
    """
    A utility method to save the JSON GPSEvent to a file (for logging) AND return the JSON-formatted string for the
    Kafka publish routine

    Args:
        dataclass_obj: The dataclass instance which is to be represented as a JSON string
        seq_number: The sequence number of event that is to be logged and represented as a JSON string
        key: The key (an identifier tagged with this event) that is incorporated into the log file's name

    Returns: JSON representation of the provided dataclass instance
    """
    with open(get_file_name_for_events(seq_number, key), 'w') as f:
        d = asdict(dataclass_obj)
        json.dump(d, f, sort_keys=True, indent=4, default=default_json)
    return json.dumps(d, default=default_json)


# GPS Kafka Publish
def gps_fetch_publish() -> None:
    """
    A utility method to publish pre-defined GPS messages to a Kafka topic
    """
    seq_num = 0
    kafka_producer = None
    kafka_topic_name = 'ODIN_TX_GPS_EVENTS'
    try:
        kafka_producer = KafkaProducer(client_id='Pseudo_Heimdall_Tx', bootstrap_servers='155.98.37.207:9092', acks=1,
                                       retry_backoff_ms=3000, api_version=(0, 10))
        # Receive, Parse, and Publish: indefinitely
        while 1:
            kafka_producer.send(kafka_topic_name, key=bytes('TxRealmSimulator', encoding='UTF-8'),
                                value=bytes(json_repr_for_events(GPSEvent(seq_number=seq_num), seq_num, 'gps'),
                                            encoding='UTF-8'))
            seq_num += 1
            time.sleep(1.0)
    except Exception as e:
        print('[ERROR] Exception caught while publishing GPS data at the TxRealmSimulator: {}'.format(
            traceback.print_tb(e.__traceback__)))
    finally:
        if kafka_producer is not None:
            kafka_producer.flush()


# Run Trigger
if __name__ == '__main__':
    print('[INFO] Started the TxRealm Simulator for RxRealm Outdoor Testing at POWDER')
    gps_fetch_publish()
    print('[INFO] Stopped the TxRealm Simulator for RxRealm Outdoor Testing at POWDER')
