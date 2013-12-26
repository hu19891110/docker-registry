import time
from oss.oss_api import *
from oss.oss_xml_handler import *
from . import Storage

import cache

logger = logging.getLogger(__name__)

class OSSStorage(Storage):

    def __init__(self, config):
        self._config = config
        self._root_path = self._config.storage_path
        self._oss = OssAPI(self._config.oss_access_key, self._config.oss_secret_key)

    def _init_path(self, path=None):
        path = os.path.join(self._root_path, path) if path else self._root_path
        logger.debug(path)
        if path and path[0] == '/':
            return path[1:]
        return path

    def makeKey(self, path):
        return

    def is_exists(self, path):
        pass

    def get_contents_as_string(self, path):
        pass

    def set_contents_from_string(self, path, string_data):
        headers = {}
        if isinstance(string_data, unicode):
            string_data = string_data.encode("utf-8")
        fp = StringIO.StringIO(string_data)
        res = self.oss.put_object_from_fp(self._config.oss_bucket, path, 
                                          fp, '\n', headers)
        fp.close()
        if (res.status/100) == 2:
            print("put object from fp ok")
        else:
            print("put object from fp error")
        print(res)
        return res

    #@cache.put
    def get_content(self, path):
        path = self._init_path(path)
        #if not self.is_exists(path):
        print(path)
        raise IOError('No such key: \'{0}\''.format(path))
        return self.get_contents_as_string(path)

    def put_content(self, path, content):
        path = self._init_path(path)
        self.set_contents_from_string(path, content)

    def stream_read(self, path):
        logger.debug("stream_read")

    def stream_write(self, path, fp):
        logger.debug("stream_write")

    def list_directory(self, path=None):
        logger.debug("list_directory")

    def exists(self, path):
        logger.debug("exists")

    #@cache.remove
    def remove(self, path):
        logger.debug("remove")

    def get_size(self, path):
        logger.debug("get_size")
