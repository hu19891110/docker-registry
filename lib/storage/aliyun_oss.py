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
		pass

	def put_content(self, path, content):
		pass

	def stream_read(self, path):
		pass

	def stream_write(self, path, fp):
		pass

	def list_directory(self, path=None):
		pass

	def exists(self, path):
		pass

	@cache.remove
	def remove(self, path):
		pass

	def get_size(self, path):
		pass
