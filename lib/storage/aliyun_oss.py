import time
from oss.oss_api import *
from oss.oss_xml_handler import *
from . import Storage

import cache

logger = logging.getLogger(__name__)

class OSSStorage(Storage):

	def __init__(self, config):
		pass

	def makeKey(self, path):
		return

	@cache.put
	def get_content(self, path):
		logger.debug("get_content")

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
