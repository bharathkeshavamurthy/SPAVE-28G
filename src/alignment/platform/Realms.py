"""
This script details the operations at the Centralized "Realms" Python Controller that handles:

    a. The registration and de-registration of XXRealm Python Controllers--along with their operational access rights
    authentication and subscription filtration tasks; and

    b. The control operations related to the global frameworks employed in Project Odin, i.e., Zookeeper startup,
    Kafka server startup, Kafka topics creation, Zookeeper status check, Kafka server status check, etc.

DESIGN NOTES:

    a. For optimal deployment, this Centralized Realms Python Controller is restricted to run only on Linux;

    b. Install Kafka and Zookeeper--with CMAK, if administrative monitoring is needed; and

    b. Make sure the required environment variables are set on this Linux platform--namely, PATH, REALMS_HOSTNAME, and
    REALMS_PORT.

Author: Bharath Keshavamurthy <bkeshava@purdue.edu>
Organization: School of Electrical & Computer Engineering, Purdue University, West Lafayette, IN.
Copyright (c) 2021. All Rights Reserved.
"""

# The imports
import os
import rpyc
import glob
import ntplib
import asyncio
import platform
import subprocess
from Utilities import *
import logging.handlers
from ntplib import NTPException
from jproperties import Properties
from rpyc.utils.server import ThreadedServer

# Ignore DeprecationWarning for async.coroutine decorator
warnings.filterwarnings(action='ignore', category=DeprecationWarning, module=__name__)

# The global basic logging configuration for this script
logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT, datefmt=LOGGING_DATE_TIME_FORMAT)

# The global logger object and its handlers for this script [StreamHandler, RotatingFileHandler]
LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.StreamHandler())
LOGGER.addHandler(logging.handlers.RotatingFileHandler(''.join([GENERAL_LOGS_DIRECTORY, 'Realms.log']),
                                                       maxBytes=20000000, backupCount=20))


class Realms(rpyc.Service):
    """
    A Singleton class encapsulating the centralized control operations for Project Odin
    """

    __realms = None

    @staticmethod
    def get_realms():
        """
        Instance access method for this Singleton

        Returns: The ONE and ONLY instance of this Realms Controller
        """
        if Realms.__realms is None:
            Realms()
        return Realms.__realms

    def __init__(self):
        """
        The initialization sequence for this Singleton

        Raises:
            ThreadedServerNotConfiguredError: This exception is raised when either the hostname or the port is not
                                              configured as environment variables (or system properties) on the
                                              Centralized Realms Python Controller's platform.
            RealmsInstantiationError: This exception is raised when the instantiation of the Singleton Realms instance
                                      FAILS.
            RealmsStartupPipelineExecutionError: This exception is raised when an error has occurred during the
                                                 execution of the startup pipeline in the Centralized Realms Python
                                                 Controller. The error could be due to the following sequenced reasons
                                                 (note the persistence of error):
                                                     a. Unsupported platform encountered: The Centralized Realms Python
                                                     Controller can only be run on Linux;
                                                     b. A valid, single properties file could not be found for either
                                                     Zookeeper or the Kafka server;
                                                     c. Zookeeper startup failed;
                                                     d. Kafka server startup failed; or
                                                     e. Kafka topic creation failed.
            Exception: Common base class for all non-exit exceptions.
        """
        try:
            # Instance instantiation handling
            if Realms.__realms is not None:
                raise RealmsInstantiationError('Only one instance of the Centralized Realms Python Controller is '
                                               'allowed i.e., this class is a Singleton.')
            else:
                Realms.__realms = self
                # XXRealm Python Controller registry and DataType-specific publish/subscribe routine associations
                self.registry = dict()
                self.routine_associations = dict()
                # A singleton NTPClient enforced throughout Project Odin via this singleton Centralized Controller
                self.ntplib_client = ntplib.NTPClient()
                # A look-up table for Kafka topic names and their corresponding configurations
                self.kafka_topic_lookup = dict()
                for topic in KafkaTopics:
                    self.kafka_topic_lookup[topic.value.name] = topic
                # Get the hostname and port properties for this Centralized Realms Python Controller's threaded server
                #   and its expositions & callbacks
                threaded_server_details = REALMS_THREADED_SERVER_DETAILS(
                    hostname=os.getenv(REALMS_HOSTNAME_ENVIRONMENT_VARIABLE, default='<ip_address>'),
                    port=int(os.getenv(REALMS_PORT_ENVIRONMENT_VARIABLE, default='<port>')))
                if threaded_server_details.hostname == '' or threaded_server_details.port < 1024:
                    raise ThreadedServerNotConfiguredError('Either the hostname or the port for the rpyc.utils.server '
                                                           'ThreadedServer has not been set on the platform.')
                # Startup pipeline execution
                startup_pipeline = connect([self.__platform_check, self.__mom_configuration_check,
                                            self.__zookeeper_start, self.__kafka_server_start,
                                            self.__kafka_topic_create_all])
                startup_pipeline_output = asyncio.get_event_loop().run_until_complete(startup_pipeline(
                    MOM_PROPERTIES(zookeeper_properties='<zookeeper_properties_file_location>',
                                   kafka_server_properties='<kafka_server_properties_file_location>')))
                for error_message in startup_pipeline_output.error_message:
                    if error_message is not None and error_message != '':
                        raise RealmsStartupPipelineExecutionError('Error during the execution of the startup '
                                                                  'pipeline | {}'.format(error_message))
                # RPyC remote expositions and callback responses
                # Start the rpyc.utils.server.ThreadedServer with default hostname:port configuration
                ThreadedServer(self, hostname=threaded_server_details.hostname,
                               port=threaded_server_details.port,
                               protocol_config={'allow_public_attrs': True}).start()
        except RealmsInstantiationError as rie:
            LOGGER.critical(
                'RealmsInstantiationError caught while initializing the Centralized Realms Python Controller | '
                '{}'.format(rie))
        except ThreadedServerNotConfiguredError as tsnce:
            LOGGER.critical('ThreadedServerNotConfiguredError caught while initializing the Centralized Realms Python '
                            'Controller - Set it manually OR Re-install Realms via Docker--making sure that the Docker '
                            'CLI tasks take care of this environment setup | {}'.format(tsnce))
        except RealmsStartupPipelineExecutionError as rspee:
            LOGGER.critical('RealmsStartupPipelineExecutionError caught while executing the startup pipeline of the '
                            'Centralized Realms Python Controller | {}'.format(rspee))
        except Exception as e:
            LOGGER.critical('An exception was caught during the initialization of the Centralized Realms Python '
                            'Controller | {}'.format(traceback.print_tb(e.__traceback__)))

    # noinspection PyDeprecation
    @staticmethod
    @asyncio.coroutine
    def __platform_check(mom_properties: MOM_PROPERTIES) -> PIPELINE_INTERNAL_CAPSULE:
        """
        A platform check routine to ensure optimal deployment of this Centralized Realms Python Controller

        Args:
            mom_properties: The Message Oriented Middleware (MOM) framework properties essential for optimal operation
                            of this Centralized Python Controller

        Returns: An internal data capsule specific to this startup pipeline that is fed into the next stage OR posted
                 as the output of the encapsulating pipeline
        """
        go_ahead = platform.system() == 'Linux'
        error_message = ['Unsupported platform: {}'.format(platform.system()) if not go_ahead else '']
        return PIPELINE_INTERNAL_CAPSULE(go_ahead=go_ahead, next_stage_config=None,
                                         next_stage_input=mom_properties, error_message=error_message)

    # noinspection PyDeprecation
    @staticmethod
    @asyncio.coroutine
    def __mom_configuration_check(data_capsule: PIPELINE_INTERNAL_CAPSULE) -> PIPELINE_INTERNAL_CAPSULE:
        """
        A Message Oriented Middleware (MOM) framework configuration check routine to ensure the optimal startup of the
        publish/subscribe APIs that are essential to Project Odin

        Args:
            data_capsule: An internal data capsule specific to this startup pipeline that is fed into this routine from
                          the previous stage

        Returns: An internal data capsule specific to this startup pipeline that is fed into the next stage OR posted
                 as the output of the encapsulating pipeline
        """
        go_ahead = False
        error_message = data_capsule.error_message
        if data_capsule.go_ahead:
            zookeeper_properties = glob.glob(data_capsule.next_stage_input.zookeeper_properties)
            kafka_server_properties = glob.glob(data_capsule.next_stage_input.kafka_server_properties)
            go_ahead = len(zookeeper_properties) == 1 and len(kafka_server_properties) == 1
            error_message.append('A single and valid properties file could not be found in the provided path for '
                                 'either Zookeeper or the Kafka server: Found Zookeeper properties: {} | '
                                 'Found Kafka server properties: {}'.format(zookeeper_properties,
                                                                            kafka_server_properties)
                                 if not go_ahead else '')
        return PIPELINE_INTERNAL_CAPSULE(go_ahead=go_ahead, next_stage_config=None,
                                         next_stage_input=data_capsule.next_stage_input,
                                         error_message=error_message)

    # noinspection PyDeprecation
    @staticmethod
    @asyncio.coroutine
    def __zookeeper_start(data_capsule: PIPELINE_INTERNAL_CAPSULE) -> PIPELINE_INTERNAL_CAPSULE:
        """
        Zookeeper startup routine using the Linux terminal

        Args:
            data_capsule: An internal data capsule specific to this startup pipeline that is fed into this routine from
                          the previous stage

        Returns: An internal data capsule specific to this startup pipeline that is fed into the next stage OR posted
                 as the output of the encapsulating pipeline
        """
        go_ahead = False
        zookeeper_config_full = None
        error_message = data_capsule.error_message
        if data_capsule.go_ahead:
            zk_subprocess = subprocess.Popen('zookeeper-server-start.sh {} >> {} &'.format(
                data_capsule.next_stage_input.zookeeper_properties, ZOOKEEPER_CONSOLE_LOGS_DIRECTORY),
                stdout=subprocess.PIPE, shell=True)
            zk_subprocess.communicate()
            zookeeper_start_stderr = zk_subprocess.returncode
            go_ahead = zookeeper_start_stderr == 0
            error_message.append('Zookeeper startup failed | Sub-process returned error code = [{}]}'.format(
                zookeeper_start_stderr) if not go_ahead else '')
            zookeeper_config_full = Properties()
            with open(data_capsule.next_stage_input.zookeeper_properties, 'rb') as zookeeper_config_file:
                zookeeper_config_full.load(zookeeper_config_file)
            # Wait for the server to start...
            # Hard-coded sleep duration because this is an internal "dynamic installation" call
            time.sleep(20.0)
        return PIPELINE_INTERNAL_CAPSULE(go_ahead=go_ahead, next_stage_config=zookeeper_config_full,
                                         next_stage_input=data_capsule.next_stage_input.kafka_server_properties,
                                         error_message=error_message)

    # noinspection PyDeprecation
    @staticmethod
    @asyncio.coroutine
    def __kafka_server_start(data_capsule: PIPELINE_INTERNAL_CAPSULE) -> PIPELINE_INTERNAL_CAPSULE:
        """
        Kafka startup routine using the Linux terminal

        Args:
            data_capsule: An internal data capsule specific to this startup pipeline that is fed into this routine from
                          the previous stage

        Returns: An internal data capsule specific to this startup pipeline that is fed into the next stage OR posted
                 as the output of the encapsulating pipeline
        """
        go_ahead = False
        kafka_config_full = None
        error_message = data_capsule.error_message
        if data_capsule.go_ahead:
            kafka_subprocess = subprocess.Popen('kafka-server-start.sh {} >> {} &'.format(
                data_capsule.next_stage_input, KAFKA_CONSOLE_LOGS_DIRECTORY),
                stdout=subprocess.PIPE, shell=True)
            kafka_subprocess.communicate()
            kafka_server_start_stderr = kafka_subprocess.returncode
            go_ahead = kafka_server_start_stderr == 0
            error_message.append('Kafka server startup failed | Sub-process returned error code = [{}]'.format(
                kafka_server_start_stderr) if not go_ahead else '')
            kafka_config_full = Properties()
            with open(data_capsule.next_stage_input, 'rb') as kafka_config_file:
                kafka_config_full.load(kafka_config_file)
            # Wait for the server to start...
            # Hard-coded sleep duration because this is an internal "dynamic installation" call
            time.sleep(20.0)
        return PIPELINE_INTERNAL_CAPSULE(go_ahead=go_ahead, next_stage_config=data_capsule.next_stage_config,
                                         next_stage_input=kafka_config_full, error_message=error_message)

    # noinspection PyDeprecation
    @staticmethod
    @asyncio.coroutine
    def __kafka_topic_create_all(data_capsule: PIPELINE_INTERNAL_CAPSULE) -> PIPELINE_INTERNAL_CAPSULE:
        """
        Creating all the default Kafka topics essential for optimal operations of the various modules in Project Odin

        DESIGN NOTE: The code does not allow on-the-fly creation of Kafka topics by any arbitrary module in Project
                     Odin. This is done to avoid the chaos of resource expositions, reservations, and semaphores.

        Args:
            data_capsule: An internal data capsule specific to this startup pipeline that is fed into this routine from
                          the previous stage

        Returns: An internal data capsule specific to this startup pipeline that is fed into the next stage OR posted
                 as the output of the encapsulating pipeline
        """
        go_ahead = False
        topic_in_error = None
        kafka_topic_creation_stderr = ''
        error_message = data_capsule.error_message
        if data_capsule.go_ahead:
            for topic in KafkaTopics:
                kafka_topic_creation_subprocess = subprocess.Popen(
                    'kafka-topics.sh --zookeeper {} --create --topic {} --partitions {} --replication-factor {}'.format(
                        data_capsule.next_stage_input.get('zookeeper.connect').data, topic.value.name,
                        str(topic.value.partitions), str(topic.value.replication_factor)),
                    stdout=subprocess.PIPE, shell=True)
                kafka_topic_creation_subprocess.communicate()
                kafka_topic_creation_stderr = kafka_topic_creation_subprocess.returncode
                go_ahead = kafka_topic_creation_stderr == 0
                if not go_ahead:
                    topic_in_error = topic.value.name
                    break
                # Hard-coded sleep duration because this is an internal "dynamic installation" call
                time.sleep(20.0)
            error_message.append('Error while creating Kafka topic {} | Sub-process returned error code = [{}]'.format(
                topic_in_error, kafka_topic_creation_stderr) if not go_ahead else '')
        return PIPELINE_INTERNAL_CAPSULE(go_ahead=go_ahead, next_stage_config=data_capsule.next_stage_config,
                                         next_stage_input=data_capsule.next_stage_input, error_message=error_message)

    def exposed_register(self, controller: Controller, data_type_associations: Dict) -> str:
        """
        Register a controller implementation instance with this Centralized Realms Python Controller

        DESIGN NOTE: The scalability of this code is unlimited horizontally within each realm, while there is no need
                     for vertical scalability because the number of realms will be a very small number (max 5).

        Args:
            controller: The controller implementation instance to be registered
            data_type_associations:  The data type associations in Project Odin (these are global and cannot be
                                     overwritten during registration)

        Returns: The registration key of the specified controller

        Raises:
            InvalidControllerConfiguration: This exception is raised when invalid configurations are received for the
                                            XXRealm Python Controller that is to be registered.
        """
        # Global Application-Scoped one-shot {datatype: mom_routine} associations
        # Blockchain inspiration: P2P networking - Peer-based global ledger update on Kafka API routines for data_types
        #   in Project Odin
        for k, v in data_type_associations.items():
            if isinstance(k, DataTypes) and isinstance(v, MOM_ROUTINE_PAIR) and \
                    k not in self.routine_associations.keys():
                self.routine_associations[k] = v
        # XXRealm Python Controller registration
        if controller.realm is None or controller.uid is None:
            raise InvalidControllerConfiguration('Invalid XXRealm Python Controller configurations received: '
                                                 'Controller Realm: {} | Controller UID: {}'.format(controller.realm,
                                                                                                    controller.uid))
        registration_key = ''.join([controller.realm.value.upper(), REALM_KEY,
                                    REGISTRATION_KEY_SEPARATOR, controller.uid])
        self.registry[registration_key] = controller
        return registration_key

    def exposed_controllers_count(self) -> int:
        """
        Get the number of registered XXRealm Python Controllers in this deployment of Project Odin

        Returns: The number of registered XXRealm Python Controllers in this deployment of Project Odin
        """
        return len(self.registry)

    def exposed_get_topic(self, controller: Controller, data_type: DataTypes, api: KafkaAPIs) -> KafkaTopics:
        """
        Get the Kafka topic for the XXRealm Python Controller associated with the provided registration_key

        Args:
            controller: The registered controller impl requesting a Kafka topic for its publishes/subscriptions
            data_type: The type of data being published/consumed by this registered referenced XXRealm Python Controller
            api: The Kafka API member used to determine the API-specific topic corresponding to this controller and the
                 requested datatype

        Returns: The Kafka topic for the registered referenced XXRealm Python Controller's publishes/subscriptions of
                 the specified data_type

        Raises:
            XXRealmPythonControllerNotRegisteredError: This exception is raised when accesses are made to core methods
                                                       in an unregistered XXRealm Python Controller.
            NotImplementedError: Method or function hasn't been implemented yet.
        """

        """
        TODO (v21.09): Implement this routine for the Kafka Stream and Connector APIs | Publisher Filtration at Consumer
        """
        if controller.registration_key is None:
            raise RealmControllerNotRegisteredError('Unable to get a Kafka topic for controller impl {} '
                                                    'because it has not been registered with the Centralized '
                                                    'Realms Python Controller.'.format(controller.__repr__()))
        if api.value != KafkaAPIs.PRODUCER.value and api.value != KafkaAPIs.CONSUMER.value:
            raise NotImplementedError('Kafka topic retrieval has not yet been implemented for APIs other than the '
                                      'producer and consumer interfaces')
        realm_value = (lambda: counterpart_realm(controller.realm).value,
                       lambda: controller.realm.value)[api.value == KafkaAPIs.PRODUCER.value]()
        return self.kafka_topic_lookup.get(''.join([PROJECT_NAME.upper(), TOPIC_NAME_SEPARATOR, realm_value.upper(),
                                                    TOPIC_NAME_SEPARATOR, data_type.value.upper(), TOPIC_NAME_SEPARATOR,
                                                    EVENTS_KEY.upper()]), KafkaTopics.ODIN_GRAVEYARD)

    def exposed_get_association(self, data_type: DataTypes) -> Any:
        """
        Get the Kafka API routine association for the specified data_type

        Args:
            data_type: The DataType enumeration member corresponding to which a Kafka API routine in Project Odin is
                       to be returned

        Returns: The Kafka API routine in Project Odin corresponding to the provided DataType enumeration member
        """
        if data_type not in self.routine_associations:
            raise InvalidDataAssociationQueryError('A routine association has not been established for datatype = [{}] '
                                                   'at the Centralized Realms Python Controller'.format(data_type))
        return self.routine_associations[data_type]

    def exposed_get_timestamp(self) -> str:
        """
        Get the NTPLib synchronized UTC timestamp from a singleton NTPClient for uniform logging throughout Project Odin

        Returns: A formatted UTC date-time string queried from a synchronized pool of servers via NTP

        Raises:
            NTPException: Exception raised by NTPLib when a client tries to query the given pool for "tx_time"
        """
        while 1:
            try:
                timestamp = datetime.utcfromtimestamp(self.ntplib_client.request(NTPLIB_QUERY_ADDRESS).tx_time)
                return str(timestamp)
            except NTPException as ntpe:
                LOGGER.warning('NTPException caught while querying UTC time via NTP - {}'.format(
                    traceback.print_tb(ntpe.__traceback__)))


# Run Trigger
if __name__ == '__main__':
    Realms()
