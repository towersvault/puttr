#!/usr/bin/python3

from os import remove, makedirs, rmdir
from os.path import dirname, abspath, isfile, join, isdir, getsize
from functools import reduce
from time import sleep
import logging, json, requests, glob, shutil, datetime, zlib


CLIENT_DATA = {
	'version': '1.0',
	'default_chunk_size': 256 * 1024,
	'debug_mode': True,
	'logging_id': 'PuttrClient'
}

settings = None
downloader = None
file_handler = None


class SettingsHandler:
	class ServerSettings:
		def __init__(self):
			self.host = ''
			self.auth_key = ''
		
	class LocalSettings:
		def __init__(self):
			self.temp_dir = ''
			self.storage_dir = ''
	
	def __init__(self):
		SETTINGS_DIR = dirname(abspath(__file__))
		FILENAME = 'settings.ptr'

		self.server = SettingsHandler.ServerSettings()
		self.local = SettingsHandler.LocalSettings()

		self.__settings = {
			'server': {
				'host': '',
				'auth_key': ''
			},
			'local': {
				'temp_dir': '',
				'storage_dir': ''
			}
		}

		if not isfile(join(SETTINGS_DIR, FILENAME)):
			logging.error('Settings file not found. Complete recreated file.')
			with open(join(SETTINGS_DIR, FILENAME), 'w') as fout:
				json.dump(self.__settings, fout)
			exit()
		else:
			with open(join(SETTINGS_DIR, FILENAME), 'r') as fin:
				self.__settings = json.load(fin)
		
		# Initialize Server Settings
		self.server.host = self.__settings['server']['host']
		self.server.auth_key = self.__settings['server']['auth_key']

		# Initialize Local Settings
		self.local.temp_dir = '/'.join([x for x in self.__settings['local']['temp_dir'].split('/') if x])
		self.local.storage_dir = '/'.join([x for x in self.__settings['local']['storage_dir'].split('/') if x])


class FileHandler:
	def get_local_files(self):
		files = {}

		try:
			for f_glob in glob.glob(settings.local.storage_dir + '/*'):
				if isdir(f_glob):
					tag = str(f_glob).split('/')[-1]

					for _file in glob.glob(f_glob + '/*.*'):
						files[str(_file).split('/')[-1]] = {
							'filename': str(_file).split('/')[-1],
							'tag': tag,
							'size': getsize(_file)
						}
				elif isfile(f_glob):
					files[str(f_glob).split('/')[-1]] = {
						'filename': str(f_glob).split('/')[-1],
						'tag': 'Untagged',
						'size': getsize(f_glob)
					}
		except BaseException as e:
			logging.error(str(e))
		
		return files

	def get_server_files(self):
		r = requests.get('%s/API/%s/sync' % (
			settings.server.host,
			settings.server.auth_key
		))
		if r.status_code == 200:
			return r.json()
		return None
	
	def __move_local(self, files):
		for _file in files:
			try:
				old_folder = files[_file]['tag']
				new_folder = files[_file]['new_tag']

				if not isdir(join(settings.local.storage_dir, new_folder)):
					makedirs(join(settings.local.storage_dir, new_folder))
				shutil.move(
					join(join(settings.local.storage_dir, old_folder), _file),
					join(join(settings.local.storage_dir, new_folder), _file)
				)

				logging.info('%s MOVED from %s/ to %s/' % (
					_file,
					old_folder,
					new_folder
				))
			except BaseException as e:
				logging.error(str(e))
			
			try:
				# The code below will only delete the dir if it's empty.
				# If not, it's going to throw an exception.
				# That should explain why the except clause is getting passed.
				rmdir(join(settings.local.storage_dir, old_folder))

				logging.info('%s/ EMPTY, folder deleted' % old_folder)
			except:
				pass

	def __delete_local(self, files):
		for _file in files:
			try:
				remove(join(join(settings.local.storage_dir, files[_file]['tag']), _file))

				logging.info('%s DELETED from %s/' % (_file, files[_file]['tag']))
			except BaseException as e:
				logging.exception(str(e))
			
			try:
				rmdir(join(settings.local.storage_dir, files[_file]['tag']))

				logging.info('%s/ EMPTY, folder deleted' % old_folder)
			except:
				pass
			

	def sync(self):
		if requests.get('%s/API/%s/ping' % (settings.server.host, settings.server.auth_key)).status_code == 200:
			local = self.get_local_files()
			server = self.get_server_files()
			if server:
				local_delete = {}
				local_move = {}
				to_download = {}

				server_sync = server['cloud']
				local_sync = server['local']

				for l in local:
					if local_sync.__contains__(l):
						if local_sync[l]['delete_file'] == 1:
							local_delete[l] = local[l]
						elif local_sync[l]['tag'] != local[l]['tag']:
							local_move[l] = local[l]
							local_move[l]['new_tag'] = local_sync[l]['tag']
				
				for s in server_sync:
					if not local.__contains__(s):
						to_download[s] = server_sync[s]
				
				self.__move_local(local_move)
				self.__delete_local(local_delete)
				
				# Download files
				for td in to_download:
					downloader.download_file(to_download[td])

			# TODO: Improve error handling here

			local = self.get_local_files()
			r = requests.post('%s/API/%s/sync' % (
				settings.server.host,
				settings.server.auth_key
			), json=local)
			logging.info('Files SYNCED, status_code=%d' % r.status_code)


class DownloadHandler:
	def __get_download_url(self, putio_id):
		r = requests.get('%s/API/%s/downloads/url/%s' % (
			settings.server.host,
			settings.server.auth_key,
			str(putio_id)
		))
		if r.status_code == 200:
			return r.json()['url']
		return ''
	
	def __notify_download_complete(self, putio_id):
		r = requests.get('%s/API/%s/downloads/completed/%s' % (
			settings.server.host,
			settings.server.auth_key,
			str(putio_id)
		))
		return r.status_code == 200

	def __calculate_crc32(self, storage_dir, filename):
		try:
			prev = 0
			for el in open(join(storage_dir, filename), 'rb'):
				prev = zlib.crc32(el, prev)
			return str(('%X' % (prev & 0xFFFFFFFF)))
		except BaseException as e:
			logging.exception(str(e))
		return ''

	def __download_file(self, url, filename):
		open(join(settings.local.temp_dir, filename), 'w').close()
		_download_size = 10**100
		_percentage = 0
		_retries = 0
		_chunk_size = 32768

		with (getsize(join(settings.local.temp_dir, filename)) < _download_size and _retries < 30):
			try:
				_local_size = getsize(join(settings.local.temp_dir, filename))
				if _local_size > 0:
					_local_size = _local_size - _chunk_size
				if _download_size != 10**100:
					headers = {
						'Range': 'bytes=%s-%s' % (str(_local_size), str(_download_size))
					}
					logging.info('%s HEADERS set to (bytes=%s-%s)' % (
						filename,
						str(_local_size),
						str(_download_size)
					))
				else:
					headers = {}
				
				_file_stream = requests.get(url=url, stream=True, headers=headers, timeout=60*5)
				if _download_size == 10**100:
					_download_size = int(_file_stream.headers.get('Content-Length'))
				
				with open(join(settings.local.temp_dir, filename), 'rb+') as fin:
					for chunk in _file_stream.iter_content(chunk_size=_chunk_size):
						if chunk:
							fin.seek(_local_size)
							fin.write(chunk)
							fin.flush()
							_local_size += _chunk_size
							if _percentage != int((_local_size / _download_size) * 100):
								_percentage = int((_local_size / _download_size) * 100)
								logging.info('%s PROGRESS %s %% (%s of %s)' % (
									filename,
									str(_percentage),
									str(_local_size),
									str(_download_size)
								))
			except BaseException as e:
				logging.error('%s download FAILED, %s' % str(e))
			
			try:
				_file_stream.close()
			except:
				pass
			
			sleep(2)
			_retries += 1
	
	def download_file(self, file_data):
		# TODO: This is one of those "don't touch this code!"
		# It needs to get improved and cleaned up

		try:
			_filename = file_data['filename']
			_crc32 = file_data['crc32']
			_folder = file_data['tag']
			_putio_id = file_data['putio_id']
			_download_url = self.__get_download_url(_putio_id)
			_started = datetime.datetime.now()

			self.__download_file(_download_url, _filename)

			logging.info('%s download COMPLETE within %s' % (
				str(_filename),
				str(datetime.datetime.now() - _started)
			))

			if isfile(join(settings.local.temp_dir, _filename)):
				_local_crc32 = self.__calculate_crc32(settings.local.temp_dir, _filename)
				if str(_crc32).upper() == str(_local_crc32).upper():
					logging.info('%x PASSED integrity check, crc32=%s' % (
						str(_filename),
						str(_local_crc32).upper()
					))

					self.__notify_download_complete(_putio_id)

					if not isdir(join(settings.local.storage_dir, _folder)):
						makedirs(join(settings.local.storage_dir, _folder))
					shutil.move(join(settings.local.temp_dir, _filename), join(join(settings.local.storage_dir, _folder), _filename))
				else:
					logging.error('%s FAILED integrity check, local=%s server=%s' % (
						str(_filename),
						str(_local_crc32).upper(),
						str(_crc32).upper()
					))
					remove(join(settings.local.temp_dir, _filename))
		except BaseException as e:
			logging.exception(str(e))


if __name__ == '__main__':
	settings = SettingsHandler()
	downloader = DownloadHandler()
	file_handler = FileHandler()