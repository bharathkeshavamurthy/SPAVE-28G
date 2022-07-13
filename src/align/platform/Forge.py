"""
This script defines the necessary utilities for Project Odin--such as, logging parameters, decorator methods,
representation callees, message validations, message parsers, and core utility methods (v21.06).

Author: Bharath Keshavamurthy <bkeshava@purdue.edu>
Organization: School of Electrical & Computer Engineering, Purdue University, West Lafayette, IN.
Copyright (c) 2021. All Rights Reserved
"""

# Project Odin Utilities (The Forge: Nidavellir) | Eitri (The Arduino Metal/Silicon Worker) fits in here...

# The imports
import time
import json
import numpy
import logging
import warnings
import functools
import traceback
import dataclasses
from Raven import *
from typing import Dict
from logging import Logger
from threading import Lock
from dataclasses import asdict
from serial import Serial, SerialException
from abc import ABC, ABCMeta, abstractmethod

"""
The keywords and separators for identifier/registration-key building
"""

# The name of this project
PROJECT_NAME = 'Odin'

# The Realm string key for Kafka topic selection and logging
REALM_KEY = 'Realm'

# The separator character for building registration keys
REGISTRATION_KEY_SEPARATOR = '-'

# The topic name separator character
TOPIC_NAME_SEPARATOR = '_'

# The string identifier (key) for data sent-to/received-from ODIN_XX_XXX_EVENTS Kafka topics
EVENTS_KEY = 'events'

# The timestamp query address for the NTPLib client
NTPLIB_QUERY_ADDRESS = 'pool.ntp.org'

"""
The environment variables (system properties) associated with the various modules in Project Odin: note here that these 
are set during installation via the Docker CLI
"""

# The string UID environment variable key (system property) for the unique identifier associated with any installed
#   XXRealm Python Controller
CONTROLLER_UID_ENVIRONMENT_VARIABLE = 'CONTROLLER_UID'

# The environment variable key (system property) for the hostname of the Centralized Realms Python Controller's
#   threaded server
REALMS_HOSTNAME_ENVIRONMENT_VARIABLE = 'REALMS_HOSTNAME'

# The environment variable key (system property) for the port of the Centralized Realms Python Controller's
#   ThreadedServer
REALMS_PORT_ENVIRONMENT_VARIABLE = 'REALMS_PORT'

"""
Logging
"""
# ODIN_*_EVENTs are logged for post-operational analyses in this directory
EVENTS_LOG_DIRECTORY = '../logs/events/'

# The directory in which the more general logs from the XXRealm Python Controllers are logged
GENERAL_LOGS_DIRECTORY = '../logs/traces/'

# The directory in which the console logs of the Apache Zookeeper service are logged
ZOOKEEPER_CONSOLE_LOGS_DIRECTORY = '../logs/traces/Zookeeper.log'

# The directory in which the console logs of the Apache Kafka service are logged
KAFKA_CONSOLE_LOGS_DIRECTORY = '../logs/traces/Kafka.log'

# The log display format
LOGGING_FORMAT = '[%(asctime)s] | %(levelname)s | %(name)s | %(funcName)s | Line#: %(lineno)d | %(message)s'

# The logging date-time format
"""
DESIGN NOTE: This timestamp--in addition to the synchronized UTC NTPLib-retrieved timestamp--will help us get a 
             better idea of the time-offsets involved in Project Odin - across several disparate modules
"""
LOGGING_DATE_TIME_FORMAT = '%Y-%m-%dT%H:%M:%S'

"""
The rpyc.utils.server.ThreadedServer properties defined as a namedtuple utility
"""
# The immutable rpyc.utils.server.ThreadedServer hostname and port of Centralized Realms Python Controller
REALMS_THREADED_SERVER_DETAILS = namedtuple('RealmsThreadedServerDetails', ['hostname', 'port'])

"""
Decorators
"""


def accepts(*types):
    """
    A decorator for function arg type checking

    Args:
        *types: The supported data types that are to be verified against the arguments provided to a method that is
                decorated with this routine.

    Returns: check_accepts assertion
    """

    def check_accepts(f):
        """
        The check_accepts wrapper

        Args:
            f: The function annotated with the @accepts decorator

        Returns: check_accepts assertions on the argcount and typing requirements of the method f
        """
        assert len(types) == f.__code__.co_argcount

        @functools.wraps(f)
        def new_f(*args, **kwds):
            """
            A method for arg typing assertions

            Args:
                *args: The non-keyword arguments passed to the decorated function
                **kwds: The keyword arguments passed to the decorated function

            Returns: check_accepts assertions on typing
            """
            for (a, t) in zip(args, types):
                assert isinstance(a, t), \
                    "arg %r does not match %s" % (a, t)
            return f(*args, **kwds)

        new_f.__name__ = f.__name__
        return new_f

    return check_accepts


def deprecated(func):
    """
    This is a decorator which can be used to mark functions as deprecated

    Args:
        func: The method (decorated with @deprecated) against which this deprecation check is made

    Returns: Warnings for deprecation outlined in new_func
    """

    @functools.wraps(func)
    def new_func(*args, **kwargs):
        """
        A method for outputting deprecation warnings

        Args:
            *args: The non-keyword arguments passed to the decorated method
            **kwargs: The keyword arguments passed to the decorated method

        Returns: A DeprecationWarning when the decorated method is called
        """
        warnings.simplefilter('always', DeprecationWarning)
        warnings.warn("Call to deprecated function {}.".format(func.__name__),
                      category=DeprecationWarning,
                      stacklevel=2)
        warnings.simplefilter('default', DeprecationWarning)
        return func(*args, **kwargs)

    return new_func


def connect(funcs):
    """
    A pipeline creation method

    Args:
        funcs: The routines to be pipelined in the order in which they are provided

    Returns: A wrapper method that encapsulates the provided routines (in the order in which they are provided) in a
             a data/functional pipeline
    """

    @functools.wraps(funcs)
    def wrapper(*args, **kwargs):
        """
        A wrapper method that encapsulates the provided routines (in the order in which they are provided) in a
        a data/functional pipeline

        Args:
            *args: The non-keyword arguments that serve as the input to the created pipeline
            **kwargs: The keyword arguments that serve as the input to the created pipeline

        Returns: A functional data input-output pipeline from the provided ordered routine collection and the specified
                 pipeline inputs
        """
        data_out = yield from funcs[0](*args, **kwargs)
        for func in funcs[1:]:
            data_out = yield from func(data_out)
        return data_out

    return wrapper


"""
Custom Exceptions
"""


class NMEAValidationError(Exception):
    """
    NMEA Validation Error

    This exception is raised when the NMEA validation of the GPS data received over the serial communication
    interface between the uC and the XXRealm Python Controller FAILS.
    """
    pass


class KafkaConnectionFactoryInstantiationError(Exception):
    """
    Kafka Connection Factory Instantiation Error

    This exception is raised when instantiation of the Singleton KafkaConnectionFactory instance FAILS.
    """
    pass


class KafkaClientNotRegisteredError(IndexError):
    """
    Kafka Client Not-Registered Error

    This exception is raised when a connection creation request is placed by an un-registered KafkaClient to the
    KafkaConnectionFactory.
    """
    pass


class KafkaUnknownConnectionError(Exception):
    """
    Kafka Unknown Connection Error

    This exception is raised when production/consumption is initiated for a KafkaTopic without creating its
    associated connections in the KafkaConnectionFactory.
    """
    pass


class KafkaProductionError(Exception):
    """
    Kafka Production Error

    This exception is raised when something went wrong while publishing the given message to the specified
    Kafka topic.
    """
    pass


class KafkaConsumptionError(Exception):
    """
    Kafka Consumption Error

    This exception is raised when something went wrong while consuming from the specified Kafka topic.

    DEPRECATION NOTE: This exception is no longer necessary due to a re-design which involves a better way to handle
                      consumption errors.
    """

    @deprecated
    def __init__(self, *args, **kwargs):
        super.__init__(*args, **kwargs)


class InvalidDictError(Exception):
    """
    Invalid Dict Error

    This exception is raised when an invalid/unsupported Python dictionary is provided in order to pack it into a
    Python dataclass instance.
    """
    pass


class RealmsInstantiationError(Exception):
    """
    Realms Instantiation Error

    This exception is raised when instantiation of the Singleton Realms instance FAILS.
    """
    pass


class XXRealmPythonControllerInstantiationError(Exception):
    """
    XXRealm Python Controller Instantiation Error

    This exception is raised when instantiation of the Singleton XXRealm Python Controller instance FAILS.
    """
    pass


class InvalidDataAssociationQueryError(Exception):
    """
    Invalid Data-Association Query Error
    This exception is raised when an invalid data association query has been made to the Centralized Realms
    Python Controller
    """
    pass


class RealmsStartupPipelineExecutionError(Exception):
    """
    Realms Startup Pipeline Execution Error

    This exception is raised when an error has occurred during the execution of the startup pipeline in the Centralized
    Realms Python Controller. The error could be due to the following sequenced reasons (note the persistence of error):
        a. Unsupported platform encountered: The Centralized Realms Python Controller can only be run on Linux;
        b. A valid, single properties file could not be found for either Zookeeper or the Kafka server;
        c. Zookeeper startup failed;
        d. Kafka server startup failed; or
        e. Kafka topic creation failed.
    """
    pass


class ThreadedServerNotConfiguredError(Exception):
    """
    Threaded Server Not Configured Error

    This exception is raised when either the hostname or the port is not configured as an environment variable (or
    system property) on the Centralized Realms Python Controller's platform.

    On the other hand, use defaults within the XXRealm Python Controllers--if these are not defined there as well.
    """
    pass


class RealmControllerNotRegisteredError(Exception):
    """
    Realm Controller Not Registered Error

    This exception is raised when accesses are made to core methods in an unregistered XXRealm Python Controller.
    """
    pass


class InvalidControllerConfiguration(Exception):
    """
    Invalid Controller Configuration Error

    This exception is raised when invalid configurations are received for the XXRealm Python Controller that is to be
    registered with the Centralized Realms Python Controller.
    """
    pass


"""
Popular utility class definitions, Connection factory singletons, and Abstract contracts 
"""


class KafkaClient(object):
    """
    A Kafka client encapsulation
    """

    def __init__(self, config: KafkaConfig):
        """
        The initialization sequence

        Args:
            config: The configuration for this Kafka client
        """
        self.config = config
        self.registry = {topic.value.name: KafkaAPIImplPair() for topic in KafkaTopics}

    def set_producer(self, topic: KafkaTopics, producer: KafkaProducer) -> None:
        """
        Set Kafka producer for the specified topic

        Args:
            topic: The KafkaTopics enumeration member for which the producer is to be set
            producer: The KafkaProducer instance that is being registered with the given topic for this KafkaClient
                      instance
        """
        self.registry[topic.value.name].producer = producer

    def get_producer(self, topic: KafkaTopics) -> KafkaProducer:
        """
        Get Kafka producer for the specified topic

        Args:
            topic: The KafkaTopics enumeration member--the producer for which is to be returned

        Returns: The KafkaProducer instance associated with the given topic
        """
        return self.registry[topic.value.name].producer

    def set_consumer(self, topic: KafkaTopics, consumer: KafkaConsumer) -> None:
        """
        Set Kafka consumer for the specified topic

        Args:
            topic: The KafkaTopics enumeration member for which the consumer is to be set
            consumer: The KafkaConsumer instance that is being registered with the given topic for this KafkaClient
                      instance
        """
        self.registry[topic.value.name].consumer = consumer

    def get_consumer(self, topic: KafkaTopics) -> KafkaConsumer:
        """
        Get Kafka consumer for the specified topic

        Args:
            topic: The KafkaTopics enumeration member--the consumer for which is to be returned

        Returns: The KafkaConsumer instance associated with the given topic
        """
        return self.registry[topic.value.name].consumer

    def set_kafka_api_impl_pair(self, topic: KafkaTopics, producer: KafkaProducer, consumer: KafkaConsumer) -> None:
        """
        Set Kafka producer/consumer API implementation pair for the specified topic

        Args:
            topic: The KafkaTopics enumeration member for which the producer-consumer pair is to be set
            producer: The KafkaProducer instance constituting one-half of the KafkaAPIImplPair
            consumer: The KafkaConsumer instance constituting the other-half of the KafkaAPIImplPair
        """
        self.registry[topic.value.name].producer = producer
        self.registry[topic.value.name].consumer = consumer

    def get_kafka_api_impl_pair(self, topic: KafkaTopics) -> KafkaAPIImplPair:
        """
        Get Kafka producer/consumer API implementation pair for the specified topic

        Args:
            topic: The KafkaTopics enumeration member for which the producer-consumer pair is to be returned

        Returns: The producer-consumer pair associated with the specified topic
        """
        return self.registry[topic.value.name]


class KafkaConnectionFactory(object):
    """
    A Singleton class to create connections (producers/consumers) for Kafka clients
    """

    _factory = None

    @staticmethod
    def get_factory():
        """
        Instance access method for this Singleton

        Returns: The ONE and ONLY instance of this Kafka connection factory
        """
        if KafkaConnectionFactory._factory is None:
            KafkaConnectionFactory()
        return KafkaConnectionFactory._factory

    def __init__(self):
        """
        The initialization sequence for this Singleton
        """
        if KafkaConnectionFactory._factory is not None:
            raise KafkaConnectionFactoryInstantiationError('Only one instance of KafkaConnectionFactory is allowed, '
                                                           'i.e., this class is a Singleton.')
        else:
            KafkaConnectionFactory._factory = self
            self.registry = list()

    def get_registration_number(self, client: KafkaClient) -> int:
        """
        Get the registration number for the given KafkaClient

        Args:
            client: The KafkaClient whose registration number is to be returned

        Returns: The registration number of the specified Kafka client
        """
        return (lambda: -1, lambda: self.registry.index(client))[client in self.registry]()

    def register(self, client: KafkaClient) -> int:
        """
        Register the Kafka client with this connection factory--and, return its registration number

        Args:
            client: The KafkaClient that is to be registered with this connection factory

        Returns: The client's registration number post-registration
        """
        if client not in self.registry:
            self.registry.append(client)
        return self.registry.index(client)

    def force_register(self, client: KafkaClient, registration_number: int = -1) -> int:
        """
        Use this method to force re-registration of your Kafka client

        Args:
            client: The KafkaClient that is to be registered with this connection factory
            registration_number: If this argument is specified, remove this indexed KafkaClient from the registry, and
                                 re-register it

        Returns: The new registration number
        """
        if client in self.registry:
            if registration_number == -1:
                registration_number = self.registry.index(client)
            self.registry.remove(self.registry[registration_number])
        return self.register(client)

    def deregister_with_client(self, client: KafkaClient) -> None:
        """
        De-register the given Kafka client

        Args:
            client: The KafkaClient that is to be unregistered from this connection factory
        """
        if client in self.registry:
            self.registry.remove(client)

    def deregister_with_registration_number(self, registration_number: int) -> None:
        """
        De-register the Kafka client indexed by its registration number

        Args:
            registration_number: Unregister the KafkaClient using this argument
        """
        if registration_number < len(self.registry):
            self.registry.remove(self.registry[registration_number])

    def create_connection(self, registration_number: int, api: KafkaAPIs, topic: KafkaTopics) -> None:
        """
        Create an API-specific connection for a registered Kafka client--with respect to the given topic

        Args:
            registration_number: The KafkaClient's registration number
            api: The API implementation instance that is to be created for this client
            topic: The KafkaTopic for which an API implementation is to be associated w.r.t the KafkaClient
                    indexed by the provided registration number

        Raises:
            NotImplementedError: Method or function hasn't been implemented yet.
            KafkaClientNotRegisteredError: This exception is raised when a connection creation request is placed by an
                                           unregistered KafkaClient to the KafkaConnectionFactory.
        """
        if registration_number < len(self.registry):
            client = self.registry[registration_number]
            if api.value == KafkaAPIs.PRODUCER.value:
                client.set_producer(topic, self.__producer(client.config))
            elif api.value == KafkaAPIs.CONSUMER.value:
                client.set_consumer(topic, self.__consumer(topic, client.config))
            else:
                """
                TODO (v21.09): Support connection implementations for all 4 Kafka APIs
                """
                raise NotImplementedError('Stream and Connector APIs have not yet been implemented in Project Odin.')
        else:
            raise KafkaClientNotRegisteredError('The provided registration_number cannot be found in the registry. '
                                                'Please register your client before creating connections for it.')

    @staticmethod
    def __producer(kafka_config: KafkaConfig) -> KafkaProducer:
        """
        A private utility method (oxymoron?) to create a Kafka producer

        Use name mangling for external calls

        Args:
            kafka_config: The KafkaConfig instance which specifies the KafkaProducer instance creation configuration

        Returns: The Kafka producer instance with the provided configuration
        """
        return KafkaProducer(client_id=kafka_config.client_id,
                             bootstrap_servers=kafka_config.bootstrap_server_config,
                             acks=(lambda: 0, lambda: 1)[kafka_config.acks](),
                             retry_backoff_ms=kafka_config.retry_backoff * 1000,
                             api_version=kafka_config.api_version)

    @staticmethod
    def __consumer(topic: KafkaTopics, kafka_config: KafkaConfig) -> KafkaConsumer:
        """
        A private utility method (oxymoron?) to create a Kafka consumer

        Use name mangling for external calls

        Args:
            kafka_config: The KafkaConfig instance which specifies the KafkaConsumer instance creation configuration

        Returns: The Kafka consumer instance with the provided configuration
        """
        return KafkaConsumer(topic.value.name,
                             client_id=kafka_config.client_id,
                             bootstrap_servers=kafka_config.bootstrap_server_config,
                             consumer_timeout_ms=kafka_config.consumer_timeout * 1000,
                             auto_offset_reset=kafka_config.auto_offset_reset)


class SetupHandler(ABC):
    """
    An abstract class definition for configuration & setup handling w.r.t the XXRealm Python Controllers
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def setup(self, mandates) -> None:
        """
         Start the Python Controller's setup tasks
         For example, At the TxRealm Python Controller's setup handler (v21.06 SetupHandlerImpl): Parse the NMEA GPS
         data from the uC (received over serial) into a GPSEvent instance, publish the JSON represented GPSEvent
         as an ODIN_GPS_EVENT <module.__name__ (TxRealm), GPSEvent.json_repr> to the ODIN_TX_GPS_EVENTS Kafka
         topic, and  simultaneously subscribe to the ODIN_RX_GPS_EVENTS Kafka topic--parse the consumed JSON
         ODIN_GPS_EVENTs <module.__name__ (RxRealm), GPSEvent.json_repr> from the Rx into the GPSEvent dataclass
         for use in the TxRealm Python Controller (v21.06 ControllerImpl); and

         At the RxRealm Python Controller's setup handler (v21.06 SetupHandlerImpl): Parse the NMEA GPS
         data from the uC (received over serial) into a GPSEvent instance, publish the JSON represented GPSEvent
         as an ODIN_GPS_EVENT <module.__name__ (RxRealm), GPSEvent.json_repr> to the ODIN_RX_GPS_EVENTS Kafka
         topic, and  simultaneously subscribe to the ODIN_TX_GPS_EVENTS Kafka topic--parse the consumed JSON
         ODIN_GPS_EVENTs <module.__name__ (TxRealm), GPSEvent.json_repr> from the Tx into the GPSEvent dataclass
         for use in the RxRealm Python Controller (v21.06 ControllerImpl).

         DESIGN NOTE: The Tx in v21.06 is fixed (Mobility.IMMOBILE): So, hard-code the Tx position in the Tx uC control
                      code, and post the positional data in the agreed-upon NMEA contract on the uC-TxRealm Python
                      Controller serial interface.

         Args:
             mandates: The XXRealm Python Controller's Kafka MOM mandates
        """
        pass


class Controller(ABC):
    """
    An abstract class for XXRealm Python Controllers
    """
    __metaclass__ = ABCMeta

    @property
    @abstractmethod
    def uid(self) -> str:
        """
        The unique identifier of this controller implementation

        Returns: The unique string identifier of this controller implementation (Picks up the UID during the initial
                 installation configuration of this XXRealm Python Controller by Docker)
        """
        pass

    @property
    @abstractmethod
    def realm(self) -> RealmTypes:
        """
        The realm-type of this controller implementation

        Returns: The realm-type (member of RealmTypes enumeration) of this controller implementation
        """
        pass

    @property
    @abstractmethod
    def mobility(self) -> Mobility:
        """
        The mobility of the realm which this implementation of XXRealm Python Controller belongs to

        Returns: The mobility (member of Mobility enumeration) of this controller implementation
        """
        pass

    @property
    @abstractmethod
    def registration_key(self) -> str:
        """
        The registration key of this controller implementation obtained post-registration from the Centralized Realms
        Python Controller

        Returns: The registration key of this controller implementation: Set via a callback from an exposed method
        """
        pass

    @property
    @abstractmethod
    def serial_comm_config(self) -> SerialCommConfig:
        """
        The serial communication interface

        Returns: The uC-XXRealm Python Controller serial communication interface configuration (SerialCommConfig)
        """
        pass

    @property
    @abstractmethod
    def kafka_config(self) -> KafkaConfig:
        """
        The Kafka client configuration

        Returns: The KafkaClient configuration (KafkaConfig) associated with this XXRealm Python Controller
        """
        pass

    @property
    @abstractmethod
    def setup_handler(self) -> SetupHandler:
        """
        The setup handler

        Returns: The SetupHandler associated with this XXRealm Python Controller
        """
        pass


"""
Popular utility routines
"""


@deprecated
def get_basic_logging() -> Dict[str, Any]:
    """
    Get the basic logging configurations for all the components in Project Odin

    Returns: A Python dictionary encapsulating the basic, common configurations employed for logging in almost every
             major component in Project Odin

    DEPRECATION NOTE: This method is no longer supported due to a re-design which involves explicitly defining the
                      Keyword Args for logging.basicConfig(**kwargs) in order to provide better flexibility.
    """
    return {'level': logging.DEBUG, 'format': LOGGING_FORMAT, 'datefmt': LOGGING_DATE_TIME_FORMAT}


def default_json(t: Any) -> str:
    """
    Get the formatted string representation of a data object in Project Odin that is not JSON serializable

    Args:
        t: The data object in Project Odin that is not JSON serializable

    Returns: The formatted string representation of the provided non-JSON-serializable data object
    """
    return f'{t}'


def get_file_name_for_events(kafka_api: KafkaAPIs, seq_number: int, key: str) -> str:
    """
    Get the event log file name in accordance with the Kafka API reference, the specified key, and the event sequence
    number

    Args:
        kafka_api: The KafkaAPIs enumeration member for which the event log file name is to be returned
        seq_number: The event sequence number that is incorporated into the log file's name
        key: The key (an identifier tagged with this event) that is incorporated into the log file's name

    Returns: The event log file name w.r.t the Kafka API reference, the specified key, and the event sequence number

    Raises:
        NotImplementedError: Method or function hasn't been implemented yet.
    """
    if kafka_api.value == KafkaAPIs.STREAM.value or kafka_api.value == KafkaAPIs.CONNECTOR.value:
        """
        TODO (v21.09): Support for all 4 Kafka API implementations
        """
        raise NotImplementedError('Stream and Connector APIs have not yet been implemented in Project Odin.')
    return ''.join([EVENTS_LOG_DIRECTORY, (lambda: 'subscriptions', lambda: 'publishes')[
        kafka_api.value == KafkaAPIs.PRODUCER.value](), '/', key, '_', 'event_', str(seq_number), '.json'])


def json_repr_for_events(dataclass_obj: dataclass, kafka_api: KafkaAPIs, seq_number: int, key: str) -> str:
    """
    A utility method to save the JSON GPSEvent to a file (for logging) AND return the JSON-formatted string for the
    Kafka publish routine

    Args:
        dataclass_obj: The dataclass instance which is to be represented as a JSON string
        kafka_api: The KafkaAPI reference for event-specific log file name determination
        seq_number: The sequence number of event that is to be logged and represented as a JSON string
        key: The key (an identifier tagged with this event) that is incorporated into the log file's name

    Returns: JSON representation of the provided dataclass instance
    """
    with open(get_file_name_for_events(kafka_api, seq_number, key), 'w') as f:
        d = asdict(dataclass_obj)
        json.dump(d, f, sort_keys=True, indent=4, default=default_json)
    return json.dumps(d, default=default_json)


def get_file_name_for_traces(trace_number: int, key: str) -> str:
    """
    Get the exclusive trace log file name in accordance with the specified key and trace number

    DESIGN NOTE: Note the classification of logs - Events correspond to those exchanged over a publish/subscribe
                 framework (e.g., ODIN_GPS_EVENT over Kafka MOM); while Traces are further sub-divided into
                 "General Logs" (functional errors and warnings) and "Non-MOM Logs" (IMUTrace over Serial)

    Args:
        trace_number: The trace number that is incorporated into the log file's name
        key: The key (an identifier tagged with this exclusive trace) that is incorporated into the log file's name

    Returns: The event log file name referenced by the specified key and trace number
    """
    return ''.join([GENERAL_LOGS_DIRECTORY, key, '_', 'trace_', str(trace_number), '.json'])


def json_repr_for_traces(dataclass_obj: dataclass, trace_number: int, key: str) -> str:
    """
    A utility method to save an exclusive JSON trace to a file referenced by the specified key and trace number

    Args:
        dataclass_obj: The dataclass instance which is to be represented as a JSON string
        trace_number: The sequence number of the trace that is to be logged and represented as a JSON string
        key: The key (an identifier tagged with this exclusive trace) that is incorporated into the log file's name

    Returns: JSON representation of the provided dataclass instance
    """
    with open(get_file_name_for_traces(trace_number, key), 'w') as f:
        d = asdict(dataclass_obj)
        json.dump(d, f, sort_keys=True, indent=4, default=default_json)
    return json.dumps(d, default=default_json)


def pack_dict_into_dataclass(dict_: Dict, dataclass_: dataclass) -> dataclass:
    """
    Pack the data from a Python dictionary into a dataclass instance

    Args:
        dict_: The dictionary to be packed into a dataclass instance
        dataclass_: The dataclass whose instance is to be returned post-dictionary-packing

    Returns: An instance of the provided dataclass reference packed with fields and values from the provided dictionary

    Raises:
        InvalidDictError: This exception is raised when an invalid/unsupported Python dictionary is provided in order
                          to pack it into a Python dataclass instance.
    """
    try:
        dataclass_fields = {f.name: f.type for f in dataclasses.fields(dataclass_)}
        return dataclass_(**{f: pack_dict_into_dataclass(dataclass_fields[f], dict_[f]) for f in dict_
                             if isinstance(dataclass_fields[f], Member)})
    except Exception as e:
        raise InvalidDictError('Invalid/Unsupported dict {} provided for conversion into an instance of dataclass '
                               '{}: {}'.format(dict_, dataclass_, traceback.print_tb(e.__traceback__)))


def nmea_validate(nmea_data: str, reduced: int) -> bool:
    """
    A utility method to validate NMEA data

    Args:
        nmea_data: The NMEA GPS data string from the uC that is to be validated
        reduced: A flag to indicate that the uC being used is having low-memory problems--so, the GPS data being shared
                 by the uC-GPS combination is a reduced one

    Returns: True/False validation status
    """
    check_length = GPSEvent.ultra_core_length if reduced else GPSEvent.core_length
    return nmea_data.startswith('$') and nmea_data.count(',') == check_length - 1


def nmea_parse(timestamp: str, nmea_data: str, gps_event: GPSEvent, reduced: int) -> None:
    """
    A utility method to parse NMEA data

    DESIGN NOTE: The "reduced" flag is an integer instead of boolean because I foresee the need for versatility in uC
                 interfaces in the future--so, different uCs will have different flash memory constraints: an integer
                 member will help better capture the variance in uC potential.

    Args:
        timestamp: The synchronized and formatted UTC timestamp from the singleton NTPClient in the Centralized Realms
                   Python Controller
        nmea_data: The NMEA GPS string that is to be checked against the available GPSEvent dataclass instance
        gps_event: The available GPSEvent dataclass instance that is to be modified with the updates from the NMEA GPS
                   string from the GPS receiver over the uC-XXRealm Python Controller serial communication interface
        reduced: A flag to indicate that the uC being used is having low-memory problems--so, the GPS data being shared
                 by the uC-GPS combination is a reduced one
    """
    nmea_args = nmea_data.replace('$', '').split(',')
    gps_event.seq_number += 1
    gps_event.timestamp = timestamp
    gps_event.is_gnss_fix_ok = (lambda: False, lambda: True)[int(nmea_args[0])]()
    gps_event.siv = int(nmea_args[1])
    gps_event.fix_type = FixType(value=int(nmea_args[2]))
    gps_event.carrier_solution_type = CarrierSolutionType(value=int(nmea_args[3]))
    gps_event.latitude = Member(is_high_precision=True, main_component=float(nmea_args[4]),
                                high_precision_component=float(nmea_args[5]), component=float(nmea_args[6]),
                                precision=1e-9, units=Units.DEGREES)
    gps_event.longitude = Member(is_high_precision=True, main_component=float(nmea_args[7]),
                                 high_precision_component=float(nmea_args[8]), component=float(nmea_args[9]),
                                 precision=1e-9, units=Units.DEGREES)
    gps_event.altitude_ellipsoid = Member(is_high_precision=True, main_component=float(nmea_args[10]),
                                          high_precision_component=float(nmea_args[11]), component=float(nmea_args[12]),
                                          precision=1e-4, units=Units.METERS)
    gps_event.altitude_msl = Member(is_high_precision=True, main_component=float(nmea_args[13]),
                                    high_precision_component=float(nmea_args[14]), component=float(nmea_args[15]),
                                    precision=1e-4, units=Units.METERS)
    # Proceed with GPSEvent subsequent member population if the interfaced uC supports large-memory "sketches"
    if not reduced:
        field_names = [f.name for f in dataclasses.fields(GPSEvent)]
        for i in range(16, len(nmea_args)):
            setattr(gps_event, field_names[i - 6], Member(is_high_precision=False, component=float(nmea_args[i]),
                                                          precision=1e-4, units=Units.UNKNOWN))


def counterpart_realm(realm: RealmTypes) -> RealmTypes:
    """
    Return the counterpart of the input realm

    Args:
        realm: The RealmType enumeration member whose counterpart is to be determined and returned

    Returns: The counterpart of the input realm

    Raises:
        NotImplementedError: Method or function hasn't been implemented yet.
    """

    """
    TODO (v21.09): Implement counterpart realm determination for GATEWAY, AGGREGATOR, and REPEATER
    """
    if realm.value != RealmTypes.TRANSMISSION.value and realm.value != RealmTypes.RECEPTION.value:
        raise NotImplementedError('The provided realm has not yet been implemented--so, a counterpart realm cannot be '
                                  'found')
    return RealmTypes.RECEPTION if realm.value == RealmTypes.TRANSMISSION.value else RealmTypes.TRANSMISSION


"""
Core utility routines
"""


def gps_fetch_publish(serial_comm_config: SerialCommConfig, serial_comm_registry: Dict,
                      kafka_client: KafkaClient, kafka_topic: KafkaTopics, gps_event: GPSEvent, logger: Logger,
                      reduced: int, lock: Lock) -> None:
    """
    A utility method to connect to the uC's Serial port, extract the NMEA GPS data, process it into a GPSEvent, and
    publish it to the Apache Kafka Message Oriented Middleware (MOM) framework on the specified topic.

    DESIGN_NOTE: Although a lot of the checks in this method may seem gratuitous, they are necessary because this
                 is a utility method and external calls to this method may be totally wild and non-conforming as
                 Project Odin scales.

    Args:
        serial_comm_config: The uC-XXRealm Python Controller serial communication interface configuration
        serial_comm_registry: An XXRealm Python Controller specific registry of serial communication interfaces
        kafka_client: The KafkaClient instance whose KafkaProducer is used for the required publishes
        kafka_topic: The KafkaTopics enumeration member to which the KafkaProducer publishes processed GPS data
        gps_event: The GPSEvent dataclass instance of the caller that is to be populated in an indefinite thread
        logger: The logger instance passed down by the caller for logging and module identification
        reduced: A flag to indicate that the uC being used is having low-memory problems--so, the GPS data being shared
                 by the uC-GPS combination is a reduced one
        lock: The threading.Lock object to avoid resource access problems/race conditions

    Raises:
        SerialException: Base class for serial port related exceptions.
        KafkaClientNotRegisteredError: This exception is raised when a connection creation request is placed by an
                                       un-registered KafkaClient to the KafkaConnectionFactory.
        KafkaUnknownConnectionError: This exception is raised when production/consumption is initiated for a KafkaTopic
                                     without creating its associated connections in the KafkaConnectionFactory.
        Exception: Common base class for all non-exit exceptions.
    """
    uc = None
    kafka_producer = None
    kafka_topic_name = kafka_topic.name
    try:
        with lock:
            # uC-XXRealm Python Controller Serial Communication Interface setup
            if DataTypes.GPS.value not in serial_comm_registry:
                uc = Serial(port=serial_comm_config.port, baudrate=serial_comm_config.baud_rate,
                            timeout=serial_comm_config.timeout)
                # Bluetooth Serial | Prepare the Bluetooth Serial Interface
                #  Having opened with 115200 baud-rate, enter CMD, set the baud-rate as 9600, and re-open with 9600
                if serial_comm_config.is_wireless:
                    uc.write(bytes('$$$', encoding='UTF-8'))
                    time.sleep(serial_comm_config.sleep_duration)
                    uc.write(bytes('U,9600,N\n', encoding='UTF-8'))
                serial_comm_registry[DataTypes.GPS.value] = uc
            else:
                uc = serial_comm_registry[DataTypes.GPS.value]
            if not uc.isOpen():
                uc.open()
            # KafkaProducer creation
            if KafkaConnectionFactory.get_factory().get_registration_number(kafka_client) == -1:
                raise KafkaClientNotRegisteredError('Please register the KafkaClient before producing to a topic.')
            kafka_producer = kafka_client.get_producer(kafka_topic)
            if kafka_producer is None:
                raise KafkaUnknownConnectionError('Please create a KafkaProducer connection for this KafkaClient '
                                                  'before initiating publish calls.')
        # Receive, Parse, and Publish: indefinitely
        while 1:
            with lock:
                nmea_gps_data: str = uc.readline().decode(encoding='UTF-8')
                uc.flushInput()
                uc.flushOutput()
            if not nmea_validate(nmea_gps_data, reduced):
                logger.warning('NMEAValidationError caught while parsing the NMEA GPS data received over serial at '
                               '{}'.format(logger.name))
                continue
            nmea_parse(str(datetime.utcnow()), nmea_gps_data, gps_event, reduced)
            kafka_producer.send(kafka_topic_name, key=bytes(logger.name, encoding='UTF-8'),
                                value=bytes(json_repr_for_events(gps_event, KafkaAPIs.PRODUCER,
                                                                 gps_event.seq_number, 'gps'), encoding='UTF-8'))
    except SerialException as se:
        with lock:
            logger.critical('SerialException caught either while setting up the uC-XXRealm Python Controller serial '
                            'interface or while reading NMEA GPS data from the uC at '
                            '{}: {}'.format(logger.name, traceback.print_tb(se.__traceback__)))
    except KafkaClientNotRegisteredError as knre:
        with lock:
            logger.critical('KafkaClientNotRegisteredError caught while creating a producer for {} at {}: {}'.format(
                kafka_topic_name, logger.name, traceback.print_tb(knre.__traceback__)))
    except KafkaUnknownConnectionError as kuce:
        with lock:
            logger.critical('KafkaUnknownConnectionError caught while creating a producer for {} at {}: {}'.format(
                kafka_topic_name, logger.name, traceback.print_tb(kuce.__traceback__)))
    except Exception as e:
        with lock:
            logger.critical('Unknown exception caught while extracting & processing GPS data at {}: {}'.format(
                logger.name, traceback.print_tb(e.__traceback__)))
    finally:
        if kafka_producer is not None:
            kafka_producer.flush()
        if uc.isOpen():
            uc.close()


# noinspection PyUnusedLocal
def comm_fetch_publish(*args, **kwargs) -> None:
    """
    A utility method to fetch Comm data and publish it to the associated Kafka topic

    Args:
        *args: Non-keyword arguments
        **kwargs: Keyword arguments

    Raises:
        NotImplementedError: Method or function hasn't been implemented yet.
    """

    """
    TODO (v21.09): Implement this routine
    """
    raise NotImplementedError('This functionality has not yet been implemented')


# noinspection PyUnusedLocal
def ctrl_fetch_publish(*args, **kwargs) -> None:
    """
    A utility method to fetch Control data and publish it to the associated Kafka topic

    Args:
        *args: Non-keyword arguments
        **kwargs: Keyword arguments

    Raises:
        NotImplementedError: Method or function hasn't been implemented yet.
    """

    """
    TODO (v21.09): Implement this routine
    """
    raise NotImplementedError('This functionality has not yet been implemented')


def gps_subscribe(kafka_client: KafkaClient, kafka_topic: KafkaTopics, gps_event: GPSEvent,
                  logger: Logger, lock: Lock) -> None:
    """
    A utility method to subscribe to the specified Kafka topic

    DESIGN_NOTE: Although a lot of the checks in this method may seem gratuitous, they are necessary because this
                 is a utility method and external calls to this method may be totally wild and non-conforming as
                 Project Odin scales.

    Args:
        kafka_client: The KafkaClient instance whose KafkaConsumer is used for the required subscriptions
        kafka_topic: The KafkaTopics enumeration member that is to be subscribed to by the KafkaClient's KafkaConsumer
        gps_event: The GPSEvent dataclass instance of the caller that is to be populated in an indefinite thread
        logger: The logger instance passed down by the caller for module identification
        lock: The threading.Lock object to avoid resource access problems/race conditions

    Raises:
        KafkaClientNotRegisteredError: This exception is raised when a connection creation request is placed by an
                                       un-registered KafkaClient to the KafkaConnectionFactory.
        KafkaUnknownConnectionError: This exception is raised when production/consumption is initiated for a KafkaTopic
                                     without creating its associated connections in the KafkaConnectionFactory.
        Exception: Common base class for all non-exit exceptions.
    """
    kafka_consumer = None
    kafka_topic_name = kafka_topic.name
    try:
        # KafkaConsumer creation
        with lock:
            if KafkaConnectionFactory.get_factory().get_registration_number(kafka_client) == -1:
                raise KafkaClientNotRegisteredError('Please register the KafkaClient before consuming from a topic.')
            kafka_consumer = kafka_client.get_consumer(kafka_topic)
            if kafka_consumer is None:
                raise KafkaUnknownConnectionError('Please create a KafkaConsumer connection for this KafkaClient '
                                                  'before initiating consumer calls.')
        # Parse the messages received over the subscribed topic
        while 1:
            for msg in kafka_consumer:
                old_dict = asdict(gps_event)
                received_dict = json.loads(msg.value)
                for k in received_dict.keys():
                    if old_dict[k] != received_dict[k]:
                        setattr(gps_event, k, received_dict[k])
                json_repr_for_events(gps_event, KafkaAPIs.CONSUMER, gps_event.seq_number, 'gps')
    except KafkaClientNotRegisteredError as knre:
        with lock:
            logger.critical('KafkaClientNotRegisteredError caught while creating a consumer for {} at {}: {}'.format(
                kafka_topic_name, logger.name, traceback.print_tb(knre.__traceback__)))
    except KafkaUnknownConnectionError as kuce:
        with lock:
            logger.critical('KafkaUnknownConnectionError caught while creating a consumer for {} at {}: {}'.format(
                kafka_topic_name, logger.name, traceback.print_tb(kuce.__traceback__)))
    except Exception as e:
        with lock:
            logger.critical('Unknown exception caught while subscribing to & processing GPS data at {}: {}'.format(
                logger.name, traceback.print_tb(e.__traceback__)))
    finally:
        if kafka_consumer is not None:
            kafka_consumer.close()


# noinspection PyUnusedLocal
def comm_subscribe(*args, **kwargs) -> None:
    """
    A utility method to subscribe to the Kafka topic associated with Comm data in Project Odin

    Args:
        *args: Non-keyword arguments
        **kwargs: Keyword arguments

    Raises:
        NotImplementedError: Method or function hasn't been implemented yet.
    """

    """
    TODO (v21.09): Implement this routine
    """
    raise NotImplementedError('This functionality has not yet been implemented')


# noinspection PyUnusedLocal
def ctrl_subscribe(*args, **kwargs) -> None:
    """
    A utility method to subscribe to the Kafka topic associated with Control data in Project Odin

    Args:
        *args: Non-keyword arguments
        **kwargs: Keyword arguments

    Raises:
        NotImplementedError: Method or function hasn't been implemented yet.
    """

    """
    TODO (v21.09): Implement this routine
    """
    raise NotImplementedError('This functionality has not yet been implemented')


def principal_axes_positioning(realm_type: RealmTypes, serial_comm_config: SerialCommConfig,
                               serial_comm_registry: Dict, tx_gps_data: GPSEvent, rx_gps_data: GPSEvent,
                               logger: Logger, lock: Lock) -> None:
    """
    The principal axes positioning (yaw, pitch, and roll) logic for platforms with GPS data mandates | Tx & Rx realms

    Args:
        realm_type: The RealmType of the calling XXRealm Python Controller
        serial_comm_config: The uC-XXRealm Python Controller serial communication interface configuration
        serial_comm_registry: An XXRealm Python Controller specific registry of serial communication interfaces
        tx_gps_data: The Tx GPS dataclass (updated in the gps_fetch_publish (Tx) or gps_subscribe (Rx) thread)
        rx_gps_data: The Rx GPS dataclass (updated in the gps_subscribe (Tx) or gps_fetch_publish (Rx) thread)
        logger: The logger instance passed down by the caller for module identification
        lock: The threading.Lock object to avoid resource access problems/race conditions

    Raises:
        SerialException: Base class for serial port related exceptions.
        Exception: Common base class for all non-exit exceptions.
    """
    uc = None
    seq_number = 0
    try:
        # uC-XXRealm Python Controller Serial Communication Interface setup | GPS data extraction
        with lock:
            if DataTypes.GPS.value not in serial_comm_registry:
                uc = Serial(port=serial_comm_config.port, baudrate=serial_comm_config.baud_rate,
                            timeout=serial_comm_config.timeout)
                # Bluetooth Serial | Prepare the Bluetooth Serial Interface
                #  Having opened with 115200 baud-rate, enter CMD, set the baud-rate as 9600, and re-open with 9600
                if serial_comm_config.is_wireless:
                    uc.write(bytes('$$$', encoding='UTF-8'))
                    time.sleep(serial_comm_config.sleep_duration)
                    uc.write(bytes('U,9600,N\n', encoding='UTF-8'))
                serial_comm_registry[DataTypes.GPS.value] = uc
            else:
                uc = serial_comm_registry[DataTypes.GPS.value]
            if not uc.isOpen():
                uc.open()
        while 1:
            if not rx_gps_data.is_gnss_fix_ok:
                logger.warning('RxRealm GPS data does not have GNSS Fix | Not initiating Principal Axes Positioning | '
                               'RxRealm GNSS Fix = {}'.format(rx_gps_data.is_gnss_fix_ok))
                continue
            # v21.06: Tx is IMMOBILE
            tx_gps_dict = asdict(tx_gps_data)
            with lock:
                rx_gps_dict = asdict(rx_gps_data)
            my_gps_dict, counterpart_gps_dict = (tx_gps_dict, rx_gps_dict) \
                if realm_type.value == RealmTypes.TRANSMISSION.value else (rx_gps_dict, tx_gps_dict)
            """
            TODO (v21.09): Use the complete ODIN_GPS_EVENT dataset (attitude, siv, is_gnss_ok, ned, etc.) for position
                           processing and corrections | Support for additional RealmTypes (Gw, Agg, Rp)
                           
                           Does this mapping logic work in the Southern hemisphere and in the Eastern Hemisphere?
            """
            # Yaw correction
            numerator = tx_gps_dict['latitude']['component'] - rx_gps_dict['latitude']['component']
            denominator = numpy.cos(tx_gps_dict['latitude']['component'] * (numpy.pi / 180.0)) * (
                    tx_gps_dict['longitude']['component'] - rx_gps_dict['longitude']['component'])
            # denominator = tx_gps_dict['longitude']['component'] - rx_gps_dict['longitude']['component']
            if denominator == 0.0:
                continue
            yaw_angle = numpy.arctan(numerator / denominator) * (180.0 / numpy.pi)
            if my_gps_dict['latitude']['component'] <= counterpart_gps_dict['latitude']['component']:
                yaw_angle += 270.0 if yaw_angle >= 0.0 else 90.0
            else:
                yaw_angle += 90.0 if yaw_angle >= 0.0 else 270.0
            # Pitch correction
            numerator = counterpart_gps_dict['altitude_ellipsoid']['component'] - my_gps_dict['altitude_ellipsoid'][
                'component']
            denominator = numpy.cos(my_gps_dict['latitude']['component'] * (numpy.pi / 180.0)) * (
                    my_gps_dict['longitude']['component'] - counterpart_gps_dict['longitude']['component'])
            # denominator = my_gps_dict['longitude']['component'] - counterpart_gps_dict['longitude']['component']
            pitch_angle = numpy.arctan(numerator / numpy.abs(denominator)) * (180.0 / numpy.pi)
            # Half the pitch-angle as a first precautionary step (...to ensure design stability)
            pitch_angle /= 2
            # Second precautionary step: Adding a straight-forward pitch restriction | TxRealm 30.0 | RxRealm 10.0
            """
            TODO (v21.09): Manage angular restrictions for other RealmTypes
            """
            if realm_type.value == RealmTypes.TRANSMISSION.value:
                pitch_angle *= numpy.dot(pitch_angle, (lambda: 30.0, lambda: -30.0)[pitch_angle < 0.0]()) / numpy.dot(
                    pitch_angle, pitch_angle)
            else:
                pitch_angle *= numpy.dot(pitch_angle, (lambda: 5.0, lambda: -5.0)[pitch_angle < 0.0]()) / numpy.dot(
                    pitch_angle, pitch_angle)
            # Log the yaw and pitch corrections by creating an IMUTrace instance
            seq_number += 1
            yaw_angle = numpy.round_(yaw_angle, 3)
            pitch_angle = numpy.round_(pitch_angle, 3)
            imu_trace = IMUTrace(seq_number=seq_number, timestamp=str(datetime.utcnow()),
                                 yaw_angle=yaw_angle, pitch_angle=pitch_angle)
            json_repr_for_traces(imu_trace, seq_number, 'imu')
            # Post the "mandate" to the uC for actual implementation of this principal axes positional correction
            correction_string = ''.join(['<', str(yaw_angle), '#', str(pitch_angle), '>'])
            with lock:
                uc.flushInput()
                uc.flushOutput()
                uc.write(bytes(correction_string, encoding='UTF-8'))
                uc.flushInput()
                uc.flushOutput()
    except SerialException as se:
        logger.critical('SerialException caught either while setting up the uC-XXRealm Python Controller serial '
                        'interface or while sending principal axes positioning correction data to the uC at '
                        '{}: {}'.format(logger.name, traceback.print_tb(se.__traceback__)))
    except Exception as e:
        with lock:
            logger.critical('Unknown exception caught while performing principal axes positioning at {}: {}'.format(
                logger.name, traceback.print_tb(e.__traceback__)))
    finally:
        if uc.isOpen():
            uc.close()
