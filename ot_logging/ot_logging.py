import logging
from datetime import datetime
import ot_db_manager
from types import SimpleNamespace
import time
import pandas as pd
import weakref


class ot_logging(logging.Logger):
    def __init__(self, process_name, args = SimpleNamespace(), lvl = logging.DEBUG):
        self.process_name = process_name
        self.lvl = lvl


        # NOTE:     args is the flags passed into a given program; if the provided arg has a value, we can use it to describe the process in more detail.
        true_attributes = [attr for attr, value in vars(args).items() if value is True]
        self.description = str(true_attributes)[1:-1].replace(' ', '').replace('-', '').replace('\'', '')

        self.filename = '/var/log/ot_logs/' + process_name + '_' + self.description + str(datetime.now().strftime('%Y-%m-%d_%H-%M')) +'.log'

            # need to auto-resolve user from the given environment
        super().__init__(self.filename, lvl)


        self._finalizer = weakref.finalize(self, self._on_delete)

        

        self.log_to_db = False

        self.listening = None
        
        self.sql_db = None

        self.process_name = process_name
        
        logging.basicConfig(format='%(process)s|%(asctime)s.%(msecs)03d|'+ self.process_name +'|%(lineno)d|%(levelname)s|%(message)s', datefmt='%Y-%m-%d %H:%M:%S',
                            filename=self.filename, level=lvl, filemode="w")

        # self.suds_client = logging.getLogger('suds.client').setLevel(logging.CRITICAL)
        # self.suds_transport = logging.getLogger('suds.transport').setLevel(logging.CRITICAL)
        # self.suds_resolver = logging.getLogger('suds.resolver').setLevel(logging.CRITICAL)

        self.set_execution_level()
    
    def getLogger(self):
        return logging.getLogger(f"{self.process_name}")

    def addLoggingLevel(self, levelName, levelNum, methodName=None):
        """
        Comprehensively adds a new logging level to the `logging` module and the
        currently configured logging class.

        `levelName` becomes an attribute of the `logging` module with the value
        `levelNum`. `methodName` becomes a convenience method for both `logging`
        itself and the class returned by `logging.getLoggerClass()` (usually just
        `logging.Logger`). If `methodName` is not specified, `levelName.lower()` is
        used.

        To avoid accidental clobberings of existing attributes, this method will
        raise an `AttributeError` if the level name is already an attribute of the
        `logging` module or if the method name is already present 

        Example
        -------
        >>> addLoggingLevel('TRACE', logging.DEBUG - 5)
        >>> logging.getLogger(__name__).setLevel("TRACE")
        >>> logging.getLogger(__name__).trace('that worked')
        >>> logging.trace('so did this')
        >>> logging.TRACE
        5

        """
        if not methodName:
            methodName = levelName.lower()

        if hasattr(logging, levelName):
            raise AttributeError('{} already defined in logging module'.format(levelName))
        if hasattr(logging, methodName):
            raise AttributeError('{} already defined in logging module'.format(methodName))
        if hasattr(logging.getLoggerClass(), methodName):
            raise AttributeError('{} already defined in logger class'.format(methodName))

        # This method was inspired by the answers to Stack Overflow post
        # http://stackoverflow.com/q/2183233/2988730, especially
        # http://stackoverflow.com/a/13638084/2988730
        # def logForLevel(self, message, *args, **kwargs):
        #     if self.isEnabledFor(levelNum):
        #         self._log(levelNum, message, args, **kwargs)
        # def logToRoot(message, *args, **kwargs):
        #     logging.log(levelNum, message, *args, **kwargs)

        logging.addLevelName(levelNum, levelName)
        setattr(logging, levelName, levelNum)
        setattr(logging.getLoggerClass(), methodName, self.execution)
        setattr(logging, methodName, self.execution)

    def set_execution_level(self):
        self.log_to_db = True
        # self.listen()
        self.addLoggingLevel('EXECUTION', 100)

        # if self.env[0] == 'Production':
        #     self.sql_db = ot_db_manager.ot_db_manager(system = '10.100.20.68', uid = self.env[1][1]['IBM CRED']['user'], pwd = self.env[1][1]['IBM CRED']['pass'], library = self.env[1][1]['IBM CRED']['library'], table_name = 'CGLOG', logg = self)
        # else:

        #     self.sql_db = ot_db_manager.ot_db_manager(system = '10.100.20.68', uid = 'tpratt', pwd = 'tpratt23', library = 'TMPLIB', table_name = 'CGLOG', logg = self)
        self.sql_db = ot_db_manager.ot_db_manager(system = '10.0.10.96', uid = 'writer', pwd = 'LA-fcYG6SlqH', library = 'staging', table_name = 'OTLOG', logg = self)
        self.execution(True)
    
    '''
    IT'S IMPORTANT TO NOTE THAT EACH PROCESS THAT DEFINES A NEW OT_LOGGING OBJECT AUTOMATICALLY LOGS ITSELF AS STARTING
    THIS INCLUDES SUB-PROCESSES THAT REQUIRE CMLOGGING (ot_db_manager, FOR EXAMPLE) IF A CMLOGGING OBJECT IS NOT PASSED.   
    '''
    def execution(self, start:bool = True):
        message = 'START' if start else 'END'
        logging.log(100, message)

        # WE HAVE A TIME.SLEEP(.1) HERE SO THE LOG-LISTENER HAS TIME TO PICK UP THIS STATEMENT
        #     BEFORE THE TRASH COLLECTOR COMES THROUGH TO WASH AWAY OUR SINS. (since the last statement of a program
        #         is often the final "execution" statment, it appears objects are being deconstructed more quickly than the log listner is able to handle.)
        self.store_message('EXECUTION', message)
        time.sleep(.3)

    def info(self, message):
        logging.info(message)
        self.store_message('INFO', message)
    
    def warning(self, message):
        logging.warning(message)
        self.store_message('WARNING', message)
    
    def error(self, message):
        logging.error(message)
        self.store_message('ERROR', message)

    def critical(self, message):
        logging.critical(message)
        self.store_message('CRITICAL', message)

    def store_message(self, type, message):

        # only push logs that contain information regarding critical events
        if logging.getLevelName(type) < logging.WARNING:
            return

        # define sql object
        sql_df = pd.DataFrame([self.process_name, type, self.description, 'U_EVOL', message]).T
        
        sql_df.columns = ['PROGRAM', 'TYPE', 'DESCRIPTION', 'USER', 'DATA_DUMP']

        sql_df.name = 'OTLOG'

        # if the sql_db object exists, we push to it.
        if(self.sql_db):
            self.sql_db.push_df_to_db(sql_df, check_t_1 = False)

    '''
    def listen(self):
        logging.info('Begin Listening to Log File')
        self.listening = threading.Thread(target=self.listener)
        self.listening.daemon = True
        self.listening.start()

    ##### parallel process to listen to the current processe's log file  
    # DEV NOTES: process to push logs asynchroniously   
    def listener(self):
        """[summary]
        Seperate process designed to listen to the log file and aggregate important events.
        """
        lines = self.log_listen(
            open(self.filename, "r"))
        

        for line in lines:

            line = line.split('|')

            # if the current line is not complete:
            if not len(line) == 6:
                continue
            # only push logs that contain information regarding critical events
            if logging.getLevelName(line[4]) < logging.WARNING:
                continue


            line.insert(5, '?')
            line.insert(6, 'TPRATT')

            sql_df = pd.DataFrame(line).T
            
            sql_df.columns = ['ID', 'Timestamp', 'Program', 'Line Number', 'Type', 'Description', 'User', 'Data_dump']
            sql_df = sql_df.drop(columns=['ID','Timestamp','Line Number'])
            sql_df.name = 'CGLOG'

            # if the sql_db object exists, we push to it.
            if(self.sql_db):
                print(line)
                self.sql_db.push_df_to_db(sql_df)


    def log_listen(self, thefile):
        """[summary]

        Args:
            thefile (FILE): Python File object representing the current state of the log file.

        Yields:
            String: Last line of the log file.
        """        
        thefile.seek(0, 2)
        while True:
            line = thefile.readline()
            if not line:
                time.sleep(0.05)
                continue
            yield line        
    '''

    def _on_delete(self):
        print('The garbage eating moster is on its way to destroy the file. i guess it\'s time to force close our connection..')
        self.listening = None
        



# if __name__ == '__main__':
#     cml = ot_logging('')