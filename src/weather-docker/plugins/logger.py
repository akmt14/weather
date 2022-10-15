from datetime import datetime

class log_record:
    
    _path = "../data/logs/"

    """
    _summary_ : records outcome of DAG run. Implemented to facilitate issue due to 

    Methods:
        success() - On successful json download
        fail() - On failure to either read, fetch or download reports 
        write_log() - Store in logs file
    """

    def __init__(self, _folder=None, _file=None, _error=None, _log=None):

        self._folder = _folder
        self._file = _file
        self._error = _error
        self._log = _log

    def success(self):

        curr_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        level_name = "INFO"
        self._log = '{}|{}|{}|{}\n'.format(curr_dt, level_name, self._folder, self._file)

        return self._log

    def fail(self):

        curr_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        level_name = "ERROR"
        error = self._error
        self._log = '{}|{}|{}|{}|{}|{}\n'.format(curr_dt, level_name, error, self._folder, self._file)
        
        return self._log

    @staticmethod
    def write_log(msg):

        with open (log_record._path + '02_daily_logs', mode='a', encoding='utf-8') as f:
            f.write(msg)