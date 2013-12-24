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

    def _init_path(self, path=None):
        path = os.path.join(self._root_path, path) if path else self._root_path
        logger.debug(path)
        if path and path[0] == '/':
            return path[1:]
        return path

    def makeKey(self, path):
        return

    @cache.put
    def get_content(self, path):
        print("in get content", path)
        path = self._init_path(path)
        print("++++++++++")
        print(path)


    def put_content(self, path, content):
        logger.debug("put_content")

    def stream_read(self, path):
        logger.debug("stream_read")

    def stream_write(self, path, fp):
        logger.debug("stream_write")

    def list_directory(self, path=None):
        logger.debug("list_directory")

    def exists(self, path):
        logger.debug("exists")

    @cache.remove
    def remove(self, path):
        logger.debug("remove")

    def get_size(self, path):
        logger.debug("get_size")
