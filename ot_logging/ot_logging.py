import logging
from datetime import datetime
import ot_db_manager
from types import SimpleNamespace
import time
import pandas as pd
import weakref
import threading
import queue


class OtLogging(logging.Logger):
    def __init__(self, process_name: str, args: SimpleNamespace = SimpleNamespace(), lvl: int = logging.DEBUG):
        self.message_queue = queue.Queue()
        self.process_name = process_name
        self.lvl = lvl

        # NOTE: args are the flags passed into a given program; if the provided arg has a value, we can use it to describe the process in more detail.
        true_attributes = [attr for attr, value in vars(args).items() if value is True]
        self.description = ','.join(true_attributes)

        self.filename = f'/var/log/ot_logs/{process_name}_{self.description}_{datetime.now().strftime("%Y-%m-%d_%H")}.log'

        super().__init__(self.process_name, lvl)

        self.set_execution_level()

        self._finalizer = weakref.finalize(self, self._on_delete)

        self.log_to_db = False
        self.listening = None
        self.process_name = process_name

        logging.basicConfig(
            format=f'%(process)s|%(asctime)s.%(msecs)03d|{self.process_name}|%(lineno)d|%(levelname)s|%(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            filename=self.filename,
            level=lvl,
            filemode="w"
        )

        
        self.queue_thread = threading.Thread(target=self._process_queue, name='log_queue_thread', daemon=True)
        self.queue_thread.start()

        

    def get_logger(self) -> logging.Logger:
        return logging.getLogger(self.process_name)

    def add_logging_level(self, level_name: str, level_num: int, method_name: str = None):
        """
        Add a new logging level to the logging module and the logger class.

        Args:
            level_name (str): Name of the new logging level.
            level_num (int): Numeric value of the logging level.
            method_name (str, optional): Name of the method to add. Defaults to level_name.lower().
        """
        if not method_name:
            method_name = level_name.lower()

        if hasattr(logging, level_name):
            raise AttributeError(f'{level_name} already defined in logging module')
        if hasattr(logging, method_name):
            raise AttributeError(f'{method_name} already defined in logging module')
        if hasattr(logging.getLoggerClass(), method_name):
            raise AttributeError(f'{method_name} already defined in logger class')

        logging.addLevelName(level_num, level_name)
        setattr(logging, level_name, level_num)
        setattr(logging.getLoggerClass(), method_name, self.execution)
        setattr(logging, method_name, self.execution)

    def set_execution_level(self):
        self.log_to_db = True
        self.add_logging_level('EXECUTION', 100)

        self.sql_db = ot_db_manager.ot_db_manager(
            system='100.98.39.58',
            uid='writer',
            pwd='LA-fcYg6SlqH',
            library='STAGING',
            table_name='OTLOG',
            logg=self
        )
        self.execution(True)

    def execution(self, start: bool = True):
        message = 'START' if start else 'END'
        logging.log(100, message)

        # Add execution message to the queue
        self.enqueue_message('EXECUTION', message)

    def info(self, message: str):
        logging.info(message)
        self.enqueue_message('INFO', message)

    def warning(self, message: str):
        logging.warning(message)
        self.enqueue_message('WARNING', message)

    def error(self, message: str):
        logging.error(message)
        self.enqueue_message('ERROR', message)

    def critical(self, message: str):
        logging.critical(message)
        self.enqueue_message('CRITICAL', message)

    def enqueue_message(self, log_type: str, message: str):
        """
        Enqueue a log message to be stored in the database.

        Args:
            log_type (str): The severity level of the log.
            message (str): The log message.
        """

        if logging.getLevelName(log_type) < logging.INFO:
            return

        log_entry = {
            'PROGRAM': self.process_name,
            'TYPE': log_type,
            'DESCRIPTION': self.description,
            'USER': 'U_EVOL',
            'DATA_DUMP': message.encode('utf-8')
        }
        self.message_queue.put(log_entry)

    def _process_queue(self):
        """
        Process log messages from the queue and store them in the database.
        """
        while True:
            try:
                log_entry = self.message_queue.get()
                if log_entry and self.sql_db:
                    df = pd.DataFrame([log_entry])
                    df.name = 'OTLOG'
                    self.sql_db.push_df_to_db(df, check_t_1=False)
                elif self.sql_db:
                    time.sleep(.25)
                else:
                    self.error(f"No database connection for log message: {log_entry}")
                self.message_queue.task_done()
                
            except Exception as e:
                self.warning(f"Failed to process log entry: {e}")

    def _on_delete(self):
        print("The garbage eating monster is on its way to destroy the file. It's time to force close our connection.")
        self.listening = None
        # Ensure all messages are processed before deletion
        self.message_queue.join()