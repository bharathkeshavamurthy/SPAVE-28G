"""
This script describes the operations of the controller at the Tx side of the Odin channel statistics measurement
campaign. The operations performed by this controller include (v21.06):

    1. Subscribe to the ODIN_GPS_EVENTS Kafka topic to receive the JSON-based ODIN_GPS_EVENTs (Rx location msgs);

    2. Parse these JSON ODIN_GPS_EVENTs to extract the (time, latitude, longitude, altitude, attitude, ...)
    "data object" collection corresponding to the Rx; and

    3. Determine the rotation_angle (the angle which the Tx should turn to w.r.t the home_plate) and publish it
    (and log it with a timestamp) to the USB/BT serial monitor COM port of the microcontroller (SerialUSB/SerialBT).

DESIGN NOTES:

    1. The Tx is stationary (fixed at a mount-point) in this version of Odin (v21.06); and

    2. Make sure all the necessary environment variables have been set on this platform--namely, REALMS_HOSTNAME,
    REALMS_PORT, and CONTROLLER_UID_ENVIRONMENT_VARIABLE.

Author: Bharath Keshavamurthy <bkeshava@purdue.edu>
Organization: School of Electrical & Computer Engineering, Purdue University, West Lafayette, IN.
Copyright (c) 2021. All Rights Reserved.
"""

# Project Odin TxRealm (Midgard)

# Scale horizontally at installation via Docker (centralized distribution OR in-situ)

# The imports
import os
import rpyc
from Forge import *
import logging.handlers
from typing import List
from concurrent.futures import ThreadPoolExecutor

# The global basic logging configuration for this script
logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT, datefmt=LOGGING_DATE_TIME_FORMAT)

# The global logger object and its handlers for this script [StreamHandler, RotatingFileHandler]
LOGGER_Tx = logging.getLogger(__name__)
LOGGER_Tx.addHandler(logging.StreamHandler())
LOGGER_Tx.addHandler(logging.handlers.RotatingFileHandler(''.join([GENERAL_LOGS_DIRECTORY, 'TxRealm.log']),
                                                          maxBytes=20000000, backupCount=20))


class TxSetupHandler(SetupHandler):
    """
    The configuration details and configuration setup tasks of the Tx rotating platform
    """

    @accepts(object, Mobility, SerialCommConfig, KafkaConfig)
    def __init__(self, mobility: Mobility, serial_comm_config: SerialCommConfig, kafka_config: KafkaConfig):
        """
        The initialization sequence: XXRealm Python Controller's setup and positional configuration tasks

        Args:
            mobility: The mobility enumeration member for this XXRealm Python Controller instance
            serial_comm_config: The configuration dataclass instance for the uC-XXRealm Python Controller serial
                                communication interface
            kafka_config: The Kafka client configuration for production to AND consumption from various Kafka topics
        """
        self.mobility: Mobility = mobility
        self.serial_comm_config: SerialCommConfig = serial_comm_config
        self.serial_comm_registry: Dict = dict()
        self.tx_gps_data: GPSEvent = GPSEvent(latitude=Member(component=40.76870550),
                                              longitude=Member(component=-111.84594033),
                                              altitude_ellipsoid=Member(component=1444.7840))
        self.rx_gps_data: GPSEvent = GPSEvent()
        self.kafka_client: KafkaClient = KafkaClient(kafka_config)

    def setup(self, mandates: List[ControllerMandate]) -> None:
        """
        Start the XXRealm Python Controller's setup tasks

        Args:
            mandates: A collection of ControllerMandates for the Kafka MOM API calls

        Raises:
            NotImplementedError: Method or function hasn't been implemented yet.
        """
        try:
            # The threading.Lock object to facilitate optimal resource accesses
            lock = Lock()
            # Kafka connection factory retrieval | Kafka client registration | Setup to create API connections
            kafka_connection_factory = KafkaConnectionFactory.get_factory()
            client_registration_number = kafka_connection_factory.register(self.kafka_client)
            with ThreadPoolExecutor(max_workers=3 * DataTypes.__len__()) as executor:
                for mandate in mandates:
                    """
                    1. TODO (v21.09): Start Tx COMM data (CommEvent) fetch & publish thread
                    2. TODO (v21.09): Start Rx COMM data (CommEvent) subscribe & parse thread
                    3. TODO (v21.09): Start CONTROL data (ControlEvent) fetch & publish AND subscribe & parse thread
                    4. TODO (v21.09): Kafka stream and connector API implementations
                    """
                    if mandate.data_type.value == DataTypes.GPS.value:
                        """
                        TODO (v21.09): Kafka stream and connector API implementations | Support for other DataTypes
                        """
                        kafka_connection_factory.create_connection(client_registration_number, KafkaAPIs.CONSUMER,
                                                                   mandate.consumption_topic)
                        # No production routine here because the TxRealm (my_realm) is IMMOBILE
                        executor.submit(mandate.consumption_routine, self.kafka_client, mandate.consumption_topic,
                                        self.rx_gps_data, LOGGER_Tx, lock)
                        # Platform Principal Axes Positioning should be enabled for GPS DataType only...
                        executor.submit(principal_axes_positioning, RealmTypes.TRANSMISSION,
                                        self.serial_comm_config, self.serial_comm_registry,
                                        self.tx_gps_data, self.rx_gps_data,
                                        LOGGER_Tx, lock)
        except Exception as e:
            LOGGER_Tx.critical(
                'Exception caught while starting the GPS data extraction thread at the TxRealm Python Controller [{}]: '
                '{}'.format(LOGGER_Tx.name, traceback.format_tb(e.__traceback__)))

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        The termination sequence: a standard dunder method
        """
        KafkaConnectionFactory.get_factory().deregister(self.kafka_client)


class TxController(Controller):
    """
    The Singleton Tx controller class (v21.06)
    """

    # The singleton controller object
    __controller = None

    # The default unique identifier (UID) of this XXRealm Python Controller
    __uid = 'MIDGARD_v21.06'

    # The realm-type of this XXRealm Python Controller
    __realm = RealmTypes.TRANSMISSION

    # The mobility of the realm this XXRealm Python Controller is associated with
    __mobility = Mobility.IMMOBILE

    # The serial communication interface between the uC and this XXRealm Python Controller
    __serial_comm_config = SerialCommConfig(id='Bifrost_Tx', is_wireless=False, port='COM8',
                                            timeout=1000.0, sleep_duration=0.1)

    # The configuration instance for the Apache Kafka publish/subscribe framework among XXRealm Python Controllers
    __kafka_config = KafkaConfig(client_id='Heimdall_Tx', group_id='Heimdall_Tx', broker_id=0)

    @staticmethod
    def get_controller():
        """
        Get the one and only instance of this XXRealm Python Controller

        Returns: The one and only instance of this XXRealm Python Controller
        """
        if TxController.__controller is None:
            TxController()
        return TxController.__controller

    def __init__(self):
        """
        The initialization sequence for this XXRealm Python Controller

        Raises:
            XXRealmPythonControllerInstantiationError: This exception is raised when instantiation of the Singleton
                                                       XXRealm Python Controller instance FAILS.
            ConnectionError: Connection error.
            Exception: Common base class for all non-exit exceptions.
        """
        try:
            if TxController.__controller is not None:
                raise XXRealmPythonControllerInstantiationError('Only one instance of the TxRealm Python Controller is '
                                                                'allowed per platform | This class is a Singleton')
            else:
                TxController.__controller = self
                # Core members of the Python Controller
                self._uid = os.getenv(CONTROLLER_UID_ENVIRONMENT_VARIABLE, default=TxController.__uid)
                self._realm = TxController.__realm
                self._mobility = TxController.__mobility
                self._serial_comm_config = TxController.__serial_comm_config
                self._kafka_config = TxController.__kafka_config
                self._setup_handler = None
                self._registration_key = None
                LOGGER_Tx.info('Starting the TxRealm Python Controller with UID: {}'.format(self.uid))
                background_server = None
                with rpyc.connect(os.getenv(REALMS_HOSTNAME_ENVIRONMENT_VARIABLE, default='155.98.37.207'),
                                  int(os.getenv(REALMS_PORT_ENVIRONMENT_VARIABLE, default='18871')),
                                  config={'allow_all_attrs': True}) as realms_connection:
                    background_server = rpyc.BgServingThread(realms_connection)
                    data_type_associations = self.data_type_associations()
                    # Register this XXRealm Python Controller with Centralized Realms
                    self.registration_key = realms_connection.root.register(self, data_type_associations)
                    # Post-registration, start the setup handler
                    self.setup_handler.setup(self.mandates(realms_connection, data_type_associations))
        except XXRealmPythonControllerInstantiationError as xxrpcie:
            LOGGER_Tx.critical(
                'XXRealmPythonControllerInstantiationError caught while initializing the Centralized Realms Python '
                'Controller | {}'.format(xxrpcie))
        except ConnectionError as ce:
            LOGGER_Tx.critical('A ConnectionError exception was caught while trying to connect to the Centralized '
                               'Realms Python Controller at {}: {}'.format(LOGGER_Tx.name,
                                                                           traceback.print_tb(ce.__traceback__)))
        except Exception as e:
            LOGGER_Tx.critical('An exception was caught while either trying to register this TxRealm Python Controller '
                               '[{}] with the Centralized Realms Python Controller OR while setting up subsequent '
                               'operations in the constituent TxSetupHandler: {}'.format(LOGGER_Tx.name,
                                                                                         traceback.print_tb(
                                                                                             e.__traceback__)))
        finally:
            if background_server is not None:
                background_server.stop()

    @staticmethod
    def data_type_associations() -> Dict[DataTypes, MOM_ROUTINE_PAIR]:
        """
        The data type associations (these are global and cannot be overwritten during registration)

        Returns: The data_type-publish/subscribe routine associations
        """

        """
        TODO (v21.09): Extend the routine associations to all 4 Kafka API Implementations & all 4 DataTypes
        """
        return {DataTypes.GPS: MOM_ROUTINE_PAIR(publish_routine=gps_fetch_publish, subscribe_routine=gps_subscribe),
                DataTypes.COMM: MOM_ROUTINE_PAIR(publish_routine=comm_fetch_publish, subscribe_routine=comm_subscribe),
                DataTypes.CONTROL: MOM_ROUTINE_PAIR(publish_routine=ctrl_fetch_publish,
                                                    subscribe_routine=ctrl_subscribe)}

    def mandates(self, realms_connection, data_type_associations) -> List[ControllerMandate]:
        """
        Develop and return the ControllerMandates collection for all DataTypes in Project Odin

        Args:
            realms_connection: The rpyc.Connection object to the Centralized Realms Python Controller
            data_type_associations: The datatype associations collection that establishes a one-to-one relationship
                                    between a DataType enumeration member and its mom_routine_pair of production and
                                    consumption utilities

        Returns: A collection of populated ControllerMandates for all DataTypes in Project Odin
        """
        mandates = list()
        for data_type, mom_routine_pair in data_type_associations.items():
            production_topic = realms_connection.root.get_topic(self, data_type, KafkaAPIs.PRODUCER)
            consumption_topic = realms_connection.root.get_topic(self, data_type, KafkaAPIs.CONSUMER)
            mandates.append(ControllerMandate(data_type=data_type, production_routine=mom_routine_pair.publish_routine,
                                              consumption_routine=mom_routine_pair.subscribe_routine,
                                              production_topic=production_topic, consumption_topic=consumption_topic))
        return mandates

    @property
    def uid(self) -> str:
        """
        The UID getter method

        Returns: The unique identifier (UID) of this XXRealm Python Controller
        """
        if self._uid is None:
            self._uid = TxController.__uid
        return self._uid

    @uid.setter
    def uid(self, __uid) -> None:
        """
        The UID setter method

        Args:
            __uid: The string identifier to be set as this XXRealm Python Controller's UID
        """
        self._uid = __uid

    @property
    def realm(self) -> RealmTypes:
        """
        The realm-type getter method

        Returns: A RealmTypes enumeration member
        """
        if self._realm is None:
            self._realm = TxController.__realm
        return self._realm

    @realm.setter
    def realm(self, __realm: RealmTypes) -> None:
        """
        The realm-type setter method

        Args:
            __realm: The realm-type associated with this XXRealm Python Controller
        """
        self._realm = __realm

    @property
    def mobility(self) -> Mobility:
        """
        The mobility getter method

        Returns: A Mobility enumeration member
        """
        if self._mobility is None:
            self._mobility = TxController.__mobility
        return self._mobility

    @mobility.setter
    def mobility(self, __mobility: Mobility) -> None:
        """
        The mobility setter method

        Args:
            __mobility: The mobility associated with this XXRealm Python Controller
        """
        self._mobility = __mobility

    @property
    def registration_key(self) -> str:
        """
        The registration key getter method

        Returns: The registration key of this XXRealm Python Controller post-registration with the Centralized Realms
                 Python Controller
        """

        """
        TODO (v21.09): Raise an exception (RealmControllerNotRegisteredError) if registration_key is queried for an
                       un-registered controller -- outside of __init__, 
        """
        return self._registration_key

    @registration_key.setter
    def registration_key(self, __registration_key):
        """
        The registration key setter method

        Args:
            __registration_key: The string identifier to be set as this XXRealm Python Controller's registration key
        """
        self._registration_key = __registration_key

    @property
    def serial_comm_config(self) -> SerialCommConfig:
        """
        The serial communication interface getter method

        Returns: SerialCommConfig
        """
        if self._serial_comm_config is None:
            self._serial_comm_config = TxController.__serial_comm_config
        return self._serial_comm_config

    @serial_comm_config.setter
    def serial_comm_config(self, __serial_comm_config: SerialCommConfig) -> None:
        """
        The serial communication interface setter method

        Args:
            __serial_comm_config: The uC-XXRealm Python Controller serial communication interface configuration instance
        """
        self._serial_comm_config = __serial_comm_config

    @property
    def kafka_config(self) -> KafkaConfig:
        """
        The Kafka client configuration getter method

        Returns: KafkaConfig
        """
        if self._kafka_config is None:
            self._kafka_config = TxController.__kafka_config
        return self._kafka_config

    @kafka_config.setter
    def kafka_config(self, __kafka_config: KafkaConfig) -> None:
        """
        The Kafka client configuration setter method

        Args:
            __kafka_config: The KafkaClient configuration (KafkaConfig) associated with this XXRealm Python Controller
        """
        self._kafka_config = __kafka_config

    @property
    def setup_handler(self) -> SetupHandler:
        """
        The setup handler getter method

        Returns: SetupHandler
        """
        if self._setup_handler is None:
            self._setup_handler = TxSetupHandler(TxController.__mobility, self._serial_comm_config,
                                                 self._kafka_config)
        return self._setup_handler

    @setup_handler.setter
    def setup_handler(self, __setup_handler: SetupHandler) -> None:
        """
        The setup handler setter method

        Args:
            __setup_handler: The setup handler instance for this XXRealm Python Controller
        """
        self._setup_handler = __setup_handler


# Run Trigger
if __name__ == '__main__':
    TxController()
