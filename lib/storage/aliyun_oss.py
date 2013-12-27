import time
from oss.oss_api import *
from oss.oss_xml_handler import *
from . import Storage, temp_store_handler
from lib import checksums

import cache

logger = logging.getLogger(__name__)

class OSSStorage(Storage):

    def __init__(self, config):
        self._config = config
        self._root_path = self._config.storage_path
        self._oss = OssAPI(self._config.oss_host, 
                           self._config.oss_access_key, 
                           self._config.oss_secret_key)

    def _init_path(self, path=None):
        path = os.path.join(self._root_path, path) if path else self._root_path
        logger.debug(path)
        if path and path[0] == '/':
            return path[1:]
        return path

    def makeKey(self, path):
        return

    def exists(self, path, external=True):
        if external:
            path = self._init_path(path)
        headers = {}
        try:
            res = self._oss.head_object(self._config.oss_bucket, path, headers)
        except Exception as e:
            print(e)
        # print("==========")
        # print("exists")
        # print(path)
        # print(res.status)
        # print("==========")
        if (res.status/100) == 2:
            return True
        else:
            # print(res.read())
            return False

    def get_contents_as_string(self, path):
        res = self._oss.get_object(self._config.oss_bucket, path)
        if (res.status/100) == 2:
            #fp = StringIO.StringIO()
            # print("==========")
            # print("get_content_as_string")
            # print(path)
            # print(res.read())
            # print("==========")
            return res.read()

    def set_contents_from_string(self, path, string_data):
        if isinstance(string_data, unicode):
            string_data = string_data.encode("utf-8")
        print(string_data)
        csums = []
        tmp, store_hndlr = temp_store_handler()
        h, sum_hndlr = checksums.simple_checksum_handler(string_data)
        csums.append('sha256:{0}'.format(h.hexdigest()))
        tmp.seek(0)
        csums.append(checksums.compute_tarsum(tmp, string_data))
        tmp.close()
        print("set_content_from_string")
        print(csums)

        fp = StringIO.StringIO(string_data)
        res = self._oss.put_object_from_fp(self._config.oss_bucket, path, fp)
        fp.close()
        if (res.status/100) != 2:
            logger.error(res.read())
        return res

    #@cache.get
    def get_content(self, path):
        # print("==========")
        # print("get_content")
        # print(path)
        # print("==========")
        path = self._init_path(path)
        if not self.exists(path, False):
            # print("not exists")
            # print(path)
            raise IOError('No such key: \'{0}\''.format(path))
        return self.get_contents_as_string(path)

    #@cache.put
    def put_content(self, path, content):
        path = self._init_path(path)
        self.set_contents_from_string(path, content)

    def stream_read(self, path):
        logger.debug("stream_read")

    def stream_write(self, path, fp):
        logger.debug("stream_write")

    def list_directory(self, path=None):
        logger.debug("list_directory")

    #@cache.remove
    def remove(self, path):
        logger.debug("remove")

    def get_size(self, path):
        logger.debug("get_size")
