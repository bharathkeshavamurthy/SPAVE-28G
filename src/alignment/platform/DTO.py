"""
This script defines the following dataclasses used for inter-system/inter-module communication in Project Odin (v21.06):

    a. ODIN_GPS_EVENT (GPSEvent contract between the TxRealm and the RxRealm Python Controllers);

    b. BIFROST (SerialCommConfig for data transfer between the uC and the TxRealm/RxRealm Python Controller); and

    c. HEIMDALL (KafkaConfig for publish/subscribe between the TxRealm and RxRealm Python Controllers).

Author: Bharath Keshavamurthy <bkeshava@purdue.edu>
Organization: School of Electrical & Computer Engineering, Purdue University, West Lafayette, IN.
Copyright (c) 2021. All Rights Reserved.
"""

# The imports
from enum import Enum
from typing import Tuple, Any
from datetime import datetime
from dataclasses import dataclass
from collections import namedtuple
from kafka import KafkaProducer, KafkaConsumer

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


class Mobility(Enum):
    """
    An enumeration listing the mobility configurations of a Realm in Project Odin
    """
    IMMOBILE = 0
    CONTINUOUS_ROVER = 1
    CONTINUOUS_DRONE = 2
    DISCONTINUOUS_ROVER = 3
    DISCONTINUOUS_DRONE = 4


class KafkaAPIs(Enum):
    """
    An enumeration outlining the APIs provided by the Kafka Message Oriented Middleware (MOM) framework
    """
    PRODUCER = 0
    CONSUMER = 1
    STREAM = 2
    CONNECTOR = 3


class RealmTypes(Enum):
    """
    An enumeration listing the various supported Realm Types (Node Types) in Project Odin
    """
    TRANSMISSION = 'Tx'
    RECEPTION = 'Rx'
    GATEWAY = 'Gw'
    AGGREGATOR = 'Agg'
    REPEATER = 'Rp'


class DataTypes(Enum):
    """
    An enumeration listing the various types of data exchanged among realms in Project Odin
    """
    GPS = 'GPS'
    COMM = 'COMM'
    CONTROL = 'CONTROL'


"""
General Project Odin Dataclasses
"""


@dataclass(order=True)
class KafkaTopicConfig:
    """
    A dataclass encapsulating all the configs needed to create a Kafka topic at the Centralized Realms Python Controller
    """
    name: str = 'ODIN_XX_XXXX_EVENTS'
    partitions: int = 1
    replication_factor: int = 1


@dataclass(order=True)
class SerialCommConfig:
    """
    A dataclass defining the args associated with the serial communication interface between the uC and TxRealm/RxRealm
    Python Controller
    """
    id: str = 'xxx'
    is_wireless: bool = False
    port: str = 'COMx'
    baud_rate: int = 115200
    timeout: float = 0.1
    sleep_duration: float = 0.1


@dataclass(order=True)
class KafkaConfig:
    """
    A dataclass defining the args associated with the Apache Kafka publish/subscribe framework between the TxRealm and
    RxRealm Python Controllers
    """
    client_id: str = 'xxx'
    group_id: str = 'yyy'
    broker_id: int = 0
    acks: bool = True
    bootstrap_server_config: str = '<ip_address:port>'
    zookeeper_config: str = '<ip_address:port>'
    retry_backoff: float = 3.0
    poll_interval: float = 3.0
    commit_each_poll: bool = True
    auto_commit_interval: float = 0.1
    use_synch_mode: bool = True
    api_version: Tuple = (0, 10)
    auto_offset_reset: str = 'earliest'
    consumer_timeout: float = 0.1


@dataclass(order=True)
class KafkaAPIImplPair:
    """
    A dataclass encapsulating a standard Kafka API impl pair used in Project Odin
    """

    """
    TODO (v21.09): Scale it to a 4-tuple once all 4 KafkaAPIs are supported in Project Odin
    """
    producer: KafkaProducer = None
    consumer: KafkaConsumer = None


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
    is_gnss_fix_ok: bool = False
    siv: int = 0
    fix_type: FixType = FixType.NO_FIX
    carrier_solution_type: CarrierSolutionType = CarrierSolutionType.NO_SOLUTION
    latitude: Member = Member()
    longitude: Member = Member()
    altitude_ellipsoid: Member = Member()
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


@dataclass(order=True)
class IMUTrace:
    """
    Primary information tier in IMUTrace

    The IMUTrace class: The data from the principal axes positioning is logged as an IMUTrace
    """
    seq_number: int = 0
    timestamp: str = str(datetime.utcnow())
    yaw_angle: float = 0
    pitch_angle: float = 0


@dataclass(order=True)
class CommEvent:
    """
    Primary information tier in CommEvent

    The CommEvent class: ODIN_COMM_EVENT communicated over the ODIN_COMM_EVENTS Kafka topic encapsulates this
    data object
    """

    """
    TODO (v21.09): Implement this dataclass
    """
    pass


@dataclass(order=True)
class ControlEvent:
    """
    Primary information tier in ControlEvent

    The ControlEvent class: ODIN_CONTROL_EVENT communicated over the ODIN_CONTROL_EVENTS Kafka topic encapsulates this
    data object
    """

    """
    TODO (v21.09): Implement this dataclass
    """
    pass


"""
NamedTuples and Dataclasses relevant to the Centralized Realms Python Controller's startup pipeline
"""

# An immutable collection housing Apache Zookeeper and Apache Kafka properties
# Message Oriented Middleware (MOM) framework configurations are encapsulated by design in its *.properties files
MOM_PROPERTIES = namedtuple('MOMProperties', ['zookeeper_properties', 'kafka_server_properties'], defaults=['', ''])

# The internal data object passed around in the startup pipeline
# Immutability is mandated in a way, by design, due to the handling of asynchronous operations in the startup pipeline
PIPELINE_INTERNAL_CAPSULE = namedtuple('PipelineInternalCapsule',
                                       ['go_ahead', 'next_stage_config', 'next_stage_input', 'error_message'],
                                       defaults=[False, None, None, []])

# An immutable pair mapping a DataType enumeration member with its associated publish and subscribe routines
"""
TODO (v21.09): Scale it to a 4-tuple once all 4 KafkaAPIs are supported in Project Odin
"""
MOM_ROUTINE_PAIR = namedtuple('MOMRoutinePair', ['publish_routine', 'subscribe_routine'])

"""
Enumerations and Dataclasses relevant to the Kafka Message Oriented Middleware (MOM) framework's API calls within 
XXRealm Python Controllers
"""


class KafkaTopics(Enum):
    """
    An enumeration listing the various topics employed in Project Odin within the Kafka publish/subscribe framework

    DESIGN NOTES:
        a. Publishes to Kafka topics happen based on the Realm of the publisher and the type of data being communicated;

        b. Subscriptions also happen based on the realm of the subscriber and the type of data being communicated, but
           the key-based message filtering for actual message consumption from the subscribed topic will be done based
           on the allowed_publishers filter (v21.09); and

        c. Generally, the Kafka topic to which a XXRealm Python Controller subscribes-to belongs to the opposite Realm.
    """

    """
    TODO (v21.09): A better fault-tolerant cluster design with multiple brokers (multiple replications across topics)
    """
    ODIN_TX_GPS_EVENTS = KafkaTopicConfig(name='ODIN_TX_GPS_EVENTS')
    ODIN_RX_GPS_EVENTS = KafkaTopicConfig(name='ODIN_RX_GPS_EVENTS')
    ODIN_TX_COMM_EVENTS = KafkaTopicConfig(name='ODIN_TX_COMM_EVENTS')
    ODIN_RX_COMM_EVENTS = KafkaTopicConfig(name='ODIN_RX_COMM_EVENTS')
    ODIN_CONTROL_EVENTS = KafkaTopicConfig(name='ODIN_CONTROL_EVENTS')
    ODIN_GRAVEYARD = KafkaTopicConfig(name='ODIN_GRAVEYARD')


@dataclass(order=True)
class ControllerMandate:
    """
    A Message Oriented Middleware (Kafka MOM) framework mandate for an XXRealm Python Controller in Project Odin
    """

    """
    TODO (v21.09): Employ a subscription filter to specify which 'production_key' the messages need to be tagged with 
                   in order for them to be used in the consumption_routine, as a part of this ControllerMandate instance
    """
    data_type: DataTypes = DataTypes.CONTROL
    production_topic: KafkaTopics = KafkaTopics.ODIN_GRAVEYARD
    consumption_topic: KafkaTopics = KafkaTopics.ODIN_GRAVEYARD
    production_routine: Any = None
    consumption_routine: Any = None
