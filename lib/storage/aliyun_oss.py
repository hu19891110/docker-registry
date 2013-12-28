import time
from oss.oss_api import *
from oss.oss_xml_handler import *
from . import Storage
from lib import checksums
import cStringIO as StringIO

import cache

logger = logging.getLogger(__name__)

class OSSStorage(Storage):

    def __init__(self, config):
        self._config = config
        self._root_path = self._config.storage_path
        self._oss = OssAPI(self._config.oss_host, 
                           self._config.oss_access_key, 
                           self._config.oss_secret_key)

        self.RecvBufferSize = 1024*1024*10

    def _init_path(self, path=None):
        path = os.path.join(self._root_path, path) if path else self._root_path
        logger.debug(path)
        if path and path[0] == '/':
            return path[1:]
        return path

    def exists(self, path, pack=True):
        if pack:
            path = self._init_path(path)
        res = self._oss.head_object(self._config.oss_bucket, path)
        if (res.status/100) == 2:
            return True
        else:
            return False

    def get_contents_as_string(self, path):
        res = self._oss.get_object(self._config.oss_bucket, path)
        if (res.status/100) == 2:
            fp = StringIO.StringIO()
            data = ''
            while True:
                data = res.read(self.RecvBufferSize)
                if data:
                    fp.write(data)
                else:
                    break;
            return fp.getvalue()

    def set_contents_from_string(self, path, string_data):
        if isinstance(string_data, unicode):
            string_data = string_data.encode("utf-8")

        fp = StringIO.StringIO(string_data)
        res = self._oss.put_object_from_fp(self._config.oss_bucket, path, fp)
        fp.close()
        if (res.status/100) != 2:
            logger.error(res.read())
        return res

    #@cache.get
    def get_content(self, path):
        path = self._init_path(path)
        if not self.exists(path, False):
            raise IOError('No such key: \'{0}\''.format(path))
        return self.get_contents_as_string(path)

    #@cache.put
    def put_content(self, path, content):
        path = self._init_path(path)
        self.set_contents_from_string(path, content)

    def stream_read(self, path):
        logger.debug("stream_read")

    def stream_write(self, path, fp):
        buffer_size = 5 * 1024 * 1024
        if self.buffer_size > buffer_size:
            buffer_size = self.buffer_size
        path = self._init_path(path)

        #Init multipart upload
        res = self._oss.init_multi_upload(self._config.oss_bucket, path)
        if res.status == 200:
            body = res.read()
            h = GetInitUploadIdXml(body)
            upload_id = h.upload_id

            if len(upload_id) == 0:
                logger.error("Init upload failed!")
            else:
                num_part = 1
                while True:
                    try:
                        buf = fp.read(buffer_size)
                        if buf:
                            params = {}
                            params['partNumber'] = str(num_part)
                            params['uploadId'] = upload_id
                            io = StringIO.StringIO(buf)
                            self._oss.put_object_from_fp(bucket=self._config.oss_bucket,
                                                         object=path,
                                                         fp=io,
                                                         params=params)
                            num_part += 1
                            io.close()
                        else:
                            break;
                    except IOError:
                        break
                part_msg_xml = get_part_xml(self._oss,
                                            self._config.oss_bucket, 
                                            path,
                                            upload_id)
                res = self._oss.complete_upload(self._config.oss_bucket,
                                                path,
                                                upload_id,
                                                part_msg_xml)
                if res.status == 200:
                    logger.debug("Upload successful")
                else:
                    logger.debug("Upload failed")

    def list_directory(self, path=None):
        path = self._init_path(path)
        if not path.endswith('/'):
            path += '/'
        ln = 0
        if self._root_path != '/':
            ln = len(self._root_path)
        exists = False

        delimiter = "/"
        res = self._oss.get_bucket(self._config.oss_bucket, delimiter=delimiter)
        if res.status == 200:
            body = res.read()
            h = GetBucketXml(body)
            (file_list, common_list) = h.list()
            for c in common_list:
                print(c)

            for key_name in file_list:
                print(key_name)
                if key_name.endswith('/'):
                    yield name[ln:-1]
                else:
                    yield name[ln:]


    #@cache.remove
    def remove(self, path):
        logger.debug("remove")
        path = self._init_path(path)
        if self.exists(path, False):
            print(path)
            self._oss.delete_object(self._config.oss_bucket, path)

    def get_size(self, path):
        logger.debug("get_size")
