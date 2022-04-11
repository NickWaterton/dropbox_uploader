#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Dropbox app for API v2.
NOTE: all dates/times are UTC
'''

import os, argparse, time, json, sys, time
from datetime import datetime, timedelta, timezone
import pytz
import logging
from logging.handlers import RotatingFileHandler
import threading
import signal
import dropbox
from dropbox import DropboxOAuth2FlowNoRedirect
from dropbox.exceptions import ApiError, AuthError
from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver
from watchdog.events import LoggingEventHandler
from watchdog.events import FileSystemEventHandler

from dropbox_content_hasher import DropboxContentHasher

__version__ = "1.0.0"

APP = 'NicksPythonUploader'
#Put your app key here
APP_KEY = 'qsyrwq1sch9u145'

utc=pytz.UTC

class MyHandler(FileSystemEventHandler):

    def __init__(self, dbu, log=None, arg=None):
        if log is not None:
            self.log = log
        else:
            self.log = logger.getlogger('Main.'+self.__class__.__name__)
            
        self.arg = arg
        self.threads = {}
        self.lock = threading.Lock()
        self.dbu = dbu

    def process(self, event):
        """
        event.event_type 
            'modified' | 'created' | 'moved' | 'deleted'
        event.is_directory
            True | False
        event.src_path
            path/to/observed/file
        event.dest_path
            if file is renamed, this is what it was renamed to
        """
        with self.lock:
            #ignore directory events
            if event.is_directory:
                self.log.debug("directory: {}, event: {}".format(event.src_path, event.event_type))
            else:
                path = event.src_path
                if event.event_type == 'moved':
                    if self.threads.pop(event.src_path, None) is not None:
                        self.log.debug('Removed thread (moved): {}'.format(event.src_path))
                    path = event.dest_path

                # the file will be processed here
                if path not in self.threads.keys():
                    self.log.info("file: {}, event: {}".format(path, event.event_type))
                    self.threads[path] = threading.Thread(target=self.wait_for_file_to_arrive, args=(path, event.event_type == 'moved'), daemon=True).start()

    def on_modified(self, event):
        #self.process(event)
        pass

    def on_created(self, event):
        self.process(event)
        
    def on_moved(self, event):
        self.process(event)
        
    def wait_for_file_to_arrive(self, file, immediate=False):
        try:
            self.log.debug("Waiting for: file: {}, immediate: {}".format(file, immediate))
            if immediate or self.file_has_arrived(file):
                self.log.info("file: {}, has finished updating".format(file))
                #upload file to dropbox
                if os.path.exists(file):
                    self.log.info('Uploading: file: {}'.format(file))
                    self.dbu.upload_files(file, self.arg.upload_path, self.arg.overwrite)
                else:
                    self.log.warning('Not Uploading File: {}, file does not exist'.format(file))
            else:
                self.log.warning("file: {}, problem, skipping".format(file))
        except Exception as e:
            self.log.exception(e)
        if self.threads.pop(file, None) is not None:
            self.log.debug('Removed thread: {}'.format(file))
                
    def file_has_arrived(self, file):
        size2 = -1
        count = 0
        while count < 5:
            try:
                if not os.path.exists(file):
                    self.log.info("File does not exist: File: {}, size {}".format(file, human_size(size)))
                    return False
                if file not in self.threads:
                    self.log.debug("Thread does not exist: File: {}, size {}".format(file, human_size(size)))
                    return False
                self.log.debug("Getting File Size: File: {}".format(file))
                size = os.path.getsize(file)
                if size == size2 and size != 0:
                    self.log.info("Arrived: File: {}, size {}".format(file, human_size(size)))
                    return True
                else:
                    size2 = size
                    self.log.debug("Checking: File: {}, size {}".format(file, human_size(size)))
                    count = count + 1 if size == 0 else 0
                    time.sleep(60)
            except FileNotFoundError:
                self.log.warning('File: {} no longer exists'.format(file))
                return False
            except OSError as e:
                self.log.warning("warn: {}".format(e))
                return False
        self.log.warning("Timeout, File: {}, size {} after {} minutes".format(file, human_size(size), count))
        return False

class DropBoxUpload:
    def __init__(self, timeout=100, chunk=50, log=None):
        self.timeout = timeout
        #max chunk size 150MB in 4MB increments
        self.chunk = min(max(chunk, 1) * 4096 * 1024, 150 * 4096 * 1024)//4
        self.log = log if log is not None else logging.getLogger('Main.'+self.__class__.__name__)
        #dont calculate hash size for files bigger than this as it takes too long...
        self.max_file_size_for_hash = 1024*100
        self.lock = threading.Lock()
        self.load_tokens()
        self.load_inProgress()
        
    def load_tokens(self, filename='config.json'):
        try:
            with open(filename, 'r') as f:
                self.tokens = json.load(f)
                with dropbox.Dropbox(oauth2_refresh_token=self.tokens['refresh_token'], app_key=APP_KEY) as dbx:
                    dbx.users_get_current_account()
                    self.log.info("Successfully connected to Dropbox")
        except Exception as e:
            self.authorize()
            
    def load_inProgress(self, filename='files_in_progress.json'):
        try:
            batch_files = {}
            with open(filename, 'r') as f:
                self.files = json.load(f)
            for k, v in self.files.copy().items():
                self.files[k]['uploading'] = False
                if datetime.fromisoformat(v.get('date', datetime.now().isoformat())) < datetime.now() - timedelta(days = 5):
                    self.log.info('Removing expired File Upload: {}'.format(k))
                    self.files.pop(k, None)
                else:
                    if os.path.isfile(k) and os.path.getsize(k) == v.get('filesize',-1):
                        if not self.files[k].get('batch', False):
                            self.log.info('File Upload interrupted: {}, {}%, to {}'.format(k, round(100*v['position']/v['filesize'], 2), v['destination']))
                            threading.Thread(target=self.upload_file, args=(k, v['destination'], True, None, True), daemon=True).start()
                        else:
                            batch_files[k] = v
                    else:
                        self.log.info('Removing invalid File Upload: {}'.format(k))
                        self.files.pop(k, None)
                        
            if len(batch_files.keys()) > 0:
                self.start_batch_upload(batch_files, upload_path=None, overwrite=True, recursive=False, older_than=None)

        except Exception as e:
            #self.log.exception(e)
            self.files = {}

    def save_inProgress(self, filename='files_in_progress.json'):
        try:
            with open(filename, 'w') as f:
                json.dump(self.files, f, indent=4)
        except Exception as e:
            self.log.exception(e)
            
    def save_tokens(self, filename='config.json'):
        try:
            with open(filename, 'w') as f:
                json.dump(self.tokens, f, indent=4)
        except Exception as e:
            self.log.exception(e)
            
    def authorize(self):
        self.tokens = None
        auth_flow = DropboxOAuth2FlowNoRedirect(APP_KEY, use_pkce=True, token_access_type='offline')
        authorize_url = auth_flow.start()
        self.log.info("1. Go to: " + authorize_url)
        self.log.info("2. Click \"Allow\" (you might have to log in first).")
        self.log.info("3. Copy the authorization code.")
        auth_code = input("Enter the authorization code here: ").strip()

        try:
            oauth_result = auth_flow.finish(auth_code)
        except Exception as e:
            self.log.exception(e)
            sys.exit(1)

        with dropbox.Dropbox(oauth2_refresh_token=oauth_result.refresh_token, app_key=APP_KEY) as dbx:
            dbx.users_get_current_account()
            self.log.info("Successfully set up client!")
            
            # View the details of the oauth result
            self.log.info(f'Access Token  = {oauth_result.access_token}')
            self.log.info(f'Account ID    = {oauth_result.account_id}')
            self.log.info(f'Refresh Token = {oauth_result.refresh_token}')
            self.log.info(f'Expiration    = {oauth_result.expires_at}')
            self.log.info(f'Scope         = {oauth_result.scope}')
            self.tokens =   {   'app_key'       : APP_KEY,
                                'access_token'  : oauth_result.access_token,
                                'account_id'    : oauth_result.account_id,
                                'refresh_token' : oauth_result.refresh_token,
                            }

            # Store this to use over and over whenever an access token expires
            self.save_tokens()
            
    def show_progress(self, time_elapsed, uploaded_percent=None, filename=''):
        if filename:
            filename+= ': '
        if uploaded_percent is not None:
            self.log.info('{}Uploaded {:.2f}%'.format(filename, uploaded_percent).ljust(15) + ' --- {:.0f}m {:.0f}s'.format(time_elapsed//60,time_elapsed%60).rjust(15))
        else:
            self.log.info('{}Uploaded {:.2f}%'.format(filename, 100).ljust(15) + ' --- {:.0f}m {:.0f}s'.format(time_elapsed//60,time_elapsed%60).rjust(15))
        
    def get_client_info(self):
        with dropbox.Dropbox(oauth2_refresh_token=self.tokens['refresh_token'], app_key=APP_KEY) as dbx:
            full_account = dbx.users_get_current_account()
            self.log.info('linked account:\nName: {}\nE-Mail: {}'.format(full_account.name.display_name, full_account.email))
            
    def list_files(self, path, recursive=False):
        try:
            self.print_files(self.file_list(path, recursive))
        except ApiError as e:
            pass
                
    def file_list(self, path, recursive=False):
        try:
            files_list = []
            with dropbox.Dropbox(oauth2_refresh_token=self.tokens['refresh_token'], app_key=APP_KEY) as dbx:
                files = dbx.files_list_folder(path)
                files_list.extend(files.entries)
                while files.has_more:
                    files = dbx.files_list_folder_continue(files.cursor)
                    files_list.extend(files.entries)
                if recursive:
                    for file in files_list:
                        if isinstance(file, dropbox.files.FolderMetadata):
                            files_list.extend(self.file_list(file.path_lower))
        except ApiError as e:
            pass
        return files_list
                
    def get_file_metadata(self, path):
        '''
        FileMetadata(client_modified=datetime.datetime(2022, 3, 24, 14, 24, 28), content_hash='d21e4db582bb4a0de329a9c238c34063a61d0c3bd7d39bce43af57b67d5d4a3a', export_info=NOT_SET, file_lock_info=NOT_SET, has_explicit_shared_members=NOT_SET, id='id:cAa78Yx2AucAAAAAAAAsXQ', is_downloadable=True, media_info=NOT_SET, name='vzdump-qemu-122-2022_03_11-01_00_02.log', parent_shared_folder_id=NOT_SET, path_display='/Backups/dump/vzdump-qemu-122-2022_03_11-01_00_02.log', path_lower='/backups/dump/vzdump-qemu-122-2022_03_11-01_00_02.log', property_groups=NOT_SET, rev='5daf79a957e1213d61aab', server_modified=datetime.datetime(2022, 3, 24, 14, 24, 28), sharing_info=NOT_SET, size=7331, symlink_info=NOT_SET)
        '''
        try:
            with dropbox.Dropbox(oauth2_refresh_token=self.tokens['refresh_token'], app_key=APP_KEY) as dbx:
                file = dbx.files_alpha_get_metadata(path)
                self.log.debug(file)
                return file
        except ApiError as e:
            if e.error.is_path():
                if e.error.get_path().is_not_found():
                    return None
            self.log.exception(e)
        return None
        
    def get_file_hash(self, path):
        hasher = DropboxContentHasher()
        with open(path, 'rb') as f:
            while True:
                chunk = f.read(1024*50)  # or whatever chunk size you want
                if len(chunk) == 0:
                    break
                hasher.update(chunk)
        return hasher.hexdigest()
        
    def get_modified_time(self, path):
        mtime = os.path.getmtime(path)
        return datetime(*time.gmtime(mtime)[:6])
        
    def duplicate_file(self, file_path, dest_path):
        file = self.get_file_metadata(dest_path)
        if file is not None:
            if isinstance(file, dropbox.files.FolderMetadata):
                return False
            if file.size <= self.max_file_size_for_hash:
                hash = self.get_file_hash(file_path)
                if hash == file.content_hash:
                    return True
            else:
                #mtime = self.get_modified_time(file_path)
                #self.log.info('comparing: mtime: {} with client_modified: {}'.format(mtime, file.client_modified))
                #if file.size == os.path.getsize(file_path) and file.client_modified == mtime:
                if file.size == os.path.getsize(file_path):
                   return True 
        return False
        
    def get_free_space(self):
        try:
            with dropbox.Dropbox(oauth2_refresh_token=self.tokens['refresh_token'], app_key=APP_KEY) as dbx:
                space = dbx.users_get_space_usage()
                if space.allocation.is_individual():
                    allocated = space.allocation.get_individual().allocated
                elif space.is_team():
                    self.log.warning('This is a team space allocation - not individual')
                    allocated = space.allocation.get_team().allocated
                elif space.allocation.is_other():
                    return float("inf")
                self.log.debug('Dropbox Space: allocated: {}, used: {}, free: {}({}%)'.format(human_size(allocated), human_size(space.used), human_size(allocated - space.used), 100*(allocated - space.used)//allocated))
                return allocated - space.used
        except ApiError as e:
            self.log.exception(e)
        return float("inf")
        
    def check_directory(self, path):
        try:
            with dropbox.Dropbox(oauth2_refresh_token=self.tokens['refresh_token'], app_key=APP_KEY) as dbx:
                folder = dbx.files_get_metadata(path)
                self.log.debug(folder)
                if isinstance(folder, dropbox.files.FolderMetadata):
                    return folder
        except ApiError as e:
            if e.error.is_path():
                if e.error.get_path().is_not_found():
                    return None
            self.log.exception(e)
        return None
        
    def create_directory(self, path):
        if path == '/':
            self.log.error('cannot create root folder')
            return
        try:
            with dropbox.Dropbox(oauth2_refresh_token=self.tokens['refresh_token'], app_key=APP_KEY) as dbx:
                self.log.info('Creating directory: {}'.format(path))
                folder = dbx.files_create_folder_v2(path)
                self.log.debug(folder)
                return folder
        except ApiError as e:
            self.log.exception(e)
        return None
                
    def print_files(self, files):
        for file in files:
            if isinstance(file, dropbox.files.FolderMetadata):
                self.log.info("Folder: {}".format(file.path_display))
            elif isinstance(file, dropbox.files.FileMetadata):
                self.log.info("mod(UTC): {}, {:10} {}".format(file.client_modified.isoformat(), human_size(file.size), file.path_display))
            else:
                self.log.info("Unknown: {}".format(file.path_display))
                
    def delete_folder(self, path):
        if path == '/':
            self.log.error('cannot delete all files/folders in {}'.format(path))
            return
        file = self.get_file_metadata(path)
        if isinstance(file, dropbox.files.FolderMetadata):
            self.delete_file(path)
        else:
            self.log.warning('{} is not a folder'.format(path))
                
    def delete_files(self, path, older_than=datetime.now(timezone.utc)):
        '''
        delete file or all files in folder
        '''
        if path == '/':
            self.log.error('cannot delete all files/folders in {}'.format(path))
            return
        file = self.get_file_metadata(path)
        if isinstance(file, dropbox.files.FileMetadata):
            if utc.localize(file.client_modified) < older_than:
                self.delete_file(file.path_lower)
                #self.log.info('Would Delete: {}, modified: {}, older_than: {} old: {}'.format(file.path_lower, utc.localize(file.client_modified), older_than, utc.localize(file.client_modified) < older_than))
        elif isinstance(file, dropbox.files.FolderMetadata):
            for file in self.file_list(path):
                self.delete_files(file.path_lower, older_than)
                
    def delete_file(self, path):
        if path == '/':
            self.log.error('cannot delete all files/folders in {}'.format(path))
            return False
        try:
            with dropbox.Dropbox(oauth2_refresh_token=self.tokens['refresh_token'], app_key=APP_KEY) as dbx:
                dbx.files_delete_v2(path)
                self.log.info('Deleted: {}'.format(path))
                return True
        except ApiError as e:
            self.log.exception(e)
        return False
                
    def upload_files(self, path, upload_path, overwrite=False, recursive=False, uploadEntryList=None, depth=0):
        if os.path.isfile(path):
            self.upload_file(path, upload_path, overwrite, uploadEntryList, destinationIsFile=False)
        elif os.path.isdir(path):
            if not recursive and depth >= 1:
                return
            depth+=1
            for file in os.listdir(path):
                self.upload_files(os.path.join(path, file), upload_path, overwrite, recursive, uploadEntryList, depth) 

    def upload_file(self, file_path, upload_path, overwrite=True, uploadEntryList=None, destinationIsFile=False):
        
        if self.files.get(file_path, {}).get('uploading', False):
            self.log.info('Already uploading: {}'.format(file_path))
            return

        try:
            with dropbox.Dropbox(oauth2_refresh_token=self.tokens['refresh_token'], app_key=APP_KEY) as dbx:
                file_size = os.path.getsize(file_path)
                file_basename = os.path.basename(file_path)
                dest_path = upload_path if destinationIsFile else join_path(upload_path, file_basename)
                if not overwrite and self.duplicate_file(file_path, dest_path):
                    self.log.debug("File {} already exists in the destination folder - not overwriting".format(file_basename))
                    return
                        
                since = time.time()
                if not file_path in self.files.keys():
                    self.files[file_path] = {}
                self.files[file_path]['uploading'] = True
                self.files[file_path]['batch'] = uploadEntryList is not None
                
                self.log.info('Uploading file: {} => {}'.format(file_path, dest_path))
                with open(file_path, 'rb') as f:
                    if file_size <= self.chunk and uploadEntryList is None:
                        dbx.files_upload(f.read(), dest_path, client_modified=self.get_modified_time(file_path))
                        self.show_progress(time.time() - since, filename=file_basename)
                        self.files.pop(file_path, None)
                    else:
                        if self.files[file_path].get('session', None) is None:
                            upload_session_start_result = dbx.files_upload_session_start(f.read(self.chunk))
                            self.files[file_path]['session'] = upload_session_start_result.session_id
                            self.files[file_path]['position'] = f.tell()
                            self.files[file_path]['date'] = datetime.now().isoformat()
                            self.files[file_path]['filesize'] = file_size
                            self.files[file_path]['destination'] = dest_path
                        else:
                            f.seek(self.files[file_path].get('position', f.tell()))
                            startdate = self.files[file_path].get('date', datetime.now().isoformat())
                            self.log.info('Resuming upload of: {} at {}% started on: {}'.format(file_basename, round(100*f.tell()/file_size, 2), startdate))
                        cursor = dropbox.files.UploadSessionCursor(session_id=self.files[file_path]['session'],offset=f.tell())
                        dest_path = self.files[file_path].get('destination', dest_path)
                        commit = dropbox.files.CommitInfo(path=dest_path, client_modified=self.get_modified_time(file_path))
                        self.files[file_path]['position'] = f.tell()                           
                        if file_size != f.tell():
                            self.show_progress(time.time() - since, 100*f.tell()/file_size, filename=file_basename)
                        self.save_inProgress()
                        while f.tell() <= file_size:
                            if ((file_size - f.tell()) <= self.chunk):
                                if uploadEntryList is None:
                                    dbx.files_upload_session_finish(f.read(self.chunk), cursor, commit)
                                    self.files.pop(file_path, None)
                                else:
                                    try:
                                        dbx.files_upload_session_append_v2(f.read(self.chunk), cursor, close=True)
                                        cursor.offset = f.tell()
                                    except ApiError as e:
                                        if e.error.is_closed():
                                            self.log.warning('{}, session closed'.format(file_path))
                                        else:
                                            self.log.exception(e)
                                    self.files[file_path]['position'] = f.tell()
                                    uploadEntryList.append(dropbox.files.UploadSessionFinishArg(cursor=cursor, commit=commit))
                                self.show_progress(time.time() - since, filename=file_basename)
                                self.log.info('{} {}'.format(file_path, 'Upload Completed' if uploadEntryList is None else 'Pending Completion'))
                                break
                            else:
                                try:
                                    dbx.files_upload_session_append_v2(f.read(self.chunk), cursor)
                                    cursor.offset = f.tell()
                                except ApiError as e:
                                    if e.error.is_incorrect_offset():
                                        correct_offset = e.error.get_incorrect_offset().correct_offset # these methods don't read so naturally
                                        self.log.warning('{} incorrect offset: {} (corrected {})'.format(file_basename, f.tell(), correct_offset))
                                        cursor.offset = correct_offset
                                        f.seek(correct_offset)
                                    else:
                                        raise
                                        
                                self.files[file_path]['position'] = f.tell()
                                self.show_progress(time.time() - since, 100*f.tell()/file_size, filename=file_basename)
                            self.save_inProgress()
                self.save_inProgress()
        except ApiError as e:
            self.log.exception(e)
            
    def get_file_list(self, dir_path, upload_path, overwrite=True, recursive=False, older_than=None, file_dict={}):
        if os.path.isdir(dir_path):
            file_list = os.listdir(dir_path)
            for file in file_list:
                file_path = join_path(dir_path, file)
                dest_path = join_path(upload_path, file)
                if os.path.isfile(file_path) and (older_than is None or utc.localize(self.get_modified_time(file_path)) >= older_than):
                    if not overwrite and self.duplicate_file(file_path, dest_path):
                        continue
                    file_dict[file_path] = {'destination': dest_path}
                    self.log.debug('Added {} => {} to file_dict'.format(file_path, file_dict[file_path]['destination']))
                if os.path.isdir(file_path) and recursive:
                    self.get_file_list(file_path, dest_path, overwrite, recursive, older_than, file_dict)
        return file_dict
            
    def start_batch_upload(self, dir_path, upload_path, overwrite=True, recursive=False, older_than=None):
        threading.Thread(target=self.batch_upload, args=(dir_path, upload_path, overwrite, recursive, older_than), daemon=True).start()
            
    def batch_upload(self, dir_path, upload_path, overwrite=True, recursive=False, older_than=None):
        '''
        Batch upload, starts multiple files upload at once
        '''
        if not isinstance(dir_path, dict) and not os.path.isdir(dir_path):
            self.log.error('Batch upload path must be a directory or dictionary: {}'.format(dir_path))
            return
        with self.lock: #only one batch upload at a time allowed
            try:
                with dropbox.Dropbox(oauth2_refresh_token=self.tokens['refresh_token'], app_key=APP_KEY) as dbx:
                    uploadEntryList = []
                    threads = []
                        
                    if isinstance(dir_path, str):
                        if upload_path is None:
                            self.log.error('Upload path must be specified')
                            return
                        dir_path = self.get_file_list(dir_path, upload_path, overwrite, recursive, older_than)
   
                    for file_path in dir_path.keys():
                        if self.files.get(file_path, {}).get('uploading', False):
                            self.log.info('Already uploading: {}'.format(file_path))
                            continue

                        upload_path = dir_path[file_path].get('destination', None)
                        if upload_path is not None:
                            threads.append(threading.Thread(target=self.upload_file, args=(file_path, upload_path, True, uploadEntryList, True), daemon=True).start())

                    count = 0
                    while len(uploadEntryList) != len(threads):
                        time.sleep(10)
                        count += 1
                        if count % 6 == 0:
                            self.log.info('uploadEntryList: {}, threads: {}'.format(len(uploadEntryList), len(threads)))
                    batch_upload = dbx.files_upload_session_finish_batch_v2(uploadEntryList)
                    self.check_status(batch_upload)
                        
            except ApiError as e:
                self.log.exception(e)
        
    def check_status(self, job_id):
        try:
            with dropbox.Dropbox(oauth2_refresh_token=self.tokens['refresh_token'], app_key=APP_KEY) as dbx:
                if isinstance(job_id, dropbox.files.UploadSessionFinishBatchLaunch):
                    if job_id.is_complete():
                        self.log.info('Upload Compete')
                        for entry in job_id.get_complete().entries:
                            if entry.is_success():
                                self.log.info('Upload Success: {}'.format(entry.get_success().name))
                                self.remove_files_dict_entry(entry.get_success().path_display)
                            elif entry.is_failure():
                                self.log.info('Upload Failure: {}'.format(entry.get_failure()))
                    elif job_id.is_other():
                        self.log.info('(other) Upload in Progress: {}'.format(job_id))
                    else:
                        self.log.info('Upload in Progress: {}'.format(job_id))
                elif isinstance(job_id, dropbox.files.UploadSessionFinishBatchResult):
                    # returned by files_upload_session_finish_batch_v2
                    for entry in job_id.entries:
                        if entry.is_success():
                            self.log.info('Upload Success: {}'.format(entry.get_success().name))
                            self.remove_files_dict_entry(entry.get_success().path_display)
                        elif entry.is_failure():
                            self.log.info('Upload Failure: {}'.format(entry.get_failure()))
                        else:
                            self.log.info('Upload in Progress: {}'.format(entry))
                elif isinstance(job_id, str):
                    result = dbx.files_upload_session_finish_batch_check(job_id)
                    if result.is_complete():
                        for entry in result.get_complete().entries:
                            if entry.is_success():
                                self.log.info('Upload Success: {}'.format(entry.get_success().name))
                                self.remove_files_dict_entry(entry.get_success().path_display)
                            elif entry.is_failure():
                                self.log.info('Upload Failure: {}'.format(entry.get_failure()))
                            else:
                                self.log.info('Upload in Progress: {}'.format(entry))
                    else:
                        self.log.info('Upload in Progress: {}, {}'.format(job_id, result))
                
        except ApiError as e:
            self.log.exception(e)
        return None
        
    def remove_files_dict_entry(self, path):
        for k,v in self.files.copy().items():
            if k == path or v.get('destination', '') == path:
                self.log.info('removing entry: {}'.format(k))
                self.files.pop(k, None)
        self.log.info('Saving Progress')
        self.save_inProgress()
                            
    def download_files(self, path, download_path, overwrite=False, recursive=False):
        file = self.get_file_metadata(download_path)
        if isinstance(file, dropbox.files.FileMetadata):
            if not overwrite:
                file_path = join_path(path, os.path.basename(file.path_display))
                if self.duplicate_file(file_path, download_path):
                    self.log.info("File {} already exists in the destination folder - not overwriting".format(file_path))
                    return
            self.download_file(path, download_path)
        elif isinstance(file, dropbox.files.FolderMetadata):
            self.log.info('Downloading Folder: {} to {}'.format(download_path, path))
            for download_file in self.file_list(download_path, recursive):
                self.download_files(join_path(path, download_path), download_file.path_display, overwrite)
                            
    def download_file(self, path, download_path):
        try:
            with dropbox.Dropbox(oauth2_refresh_token=self.tokens['refresh_token'], app_key=APP_KEY) as dbx:
                if not os.path.isdir(path):
                    os.makedirs(path)
                self.log.info('Downloading File: {} to {}'.format(download_path, path))
                filename = os.path.basename(download_path)
                dbx.files_download_to_file(join_path(path, filename), download_path)
        except ApiError as e:
            self.log.exception(e)
    
def join_path(basepath, path):
    return os.path.join(basepath, *path.split(os.path.sep))

def is_on_mount(path):
    return False if path == os.path.dirname(path) else True if os.path.ismount(path) else is_on_mount(os.path.dirname(path))

def human_size(size_bytes):
    """
    format a size in bytes into a 'human' file size, e.g. bytes, KB, MB, GB, TB, PB
    Note that bytes/KB will be reported in whole numbers but MB and above will have greater precision
    e.g. 1 byte, 43 bytes, 443 KB, 4.3 MB, 4.43 GB, etc
    """
    if size_bytes == 1:
        # because I really hate unnecessary plurals
        return "1 byte"

    suffixes_table = [('bytes',0),('KB',0),('MB',1),('GB',2),('TB',2), ('PB',2)]

    num = float(size_bytes)
    for suffix, precision in suffixes_table:
        if num < 1024.0:
            break
        num /= 1024.0

    if precision == 0:
        formatted_size = f'{int(num)}'
    else:
        formatted_size = str(round(num, ndigits=precision))

    return f'{formatted_size} {suffix}'
    
def delete_files(dbu, arg):
    if arg.upload_path is None:
        log.error('--upload_path is required')
        sys.exit(1)
    older_than = datetime.now(timezone.utc) - timedelta(days=arg.older_than)
    log.info('Deleting dropbox folders/files in {} older than {}'.format(arg.upload_path, older_than.isoformat()))
    threading.Thread(target=dbu.delete_files, args=(arg.upload_path, older_than), daemon=True).start()
                        
def setup_logger(logger_name, log_file, level=logging.DEBUG, console=False):
    try:
        l = logging.getLogger(logger_name)
        if logger_name ==__name__:
            formatter = logging.Formatter('[%(levelname)1.1s %(asctime)s] %(threadName)10.10s: %(message)s')
        else:
            formatter = logging.Formatter('%(message)s')
        fileHandler = logging.handlers.RotatingFileHandler(log_file, mode='a', maxBytes=2000000, backupCount=5)
        fileHandler.setFormatter(formatter)
        if console == True:
          streamHandler = logging.StreamHandler()

        l.setLevel(level)
        l.addHandler(fileHandler)
        if console == True:
          streamHandler.setFormatter(formatter)
          l.addHandler(streamHandler)
    except IOError as e:
        if e[0] == 13: #errno Permission denied
            print("Error: %s: You probably don't have permission to write to the log file/directory - try sudo" % e)
        else:
            print("Log Error: %s" % e)
        sys.exit(1)
        
def exit_handler(signum, frame):
    log.info(f'{signal.strsignal(signum)} received. Exiting....')
    exit(0)

def main():
    parser = argparse.ArgumentParser(description='Upload/Download files to/from dropbox')
    parser.add_argument('action', type=str, choices=['client', 'list', 'info', 'upload', 'delete', 'delete_folder', 'monitor', 'download'], default=None, help='action to take')
    parser.add_argument('-f','--file_path', type=str, default=None, help='path to file to upload')
    parser.add_argument('-u','--upload_path', type=str, default=None, help='path in dropbox')
    parser.add_argument('-t','--timeout', type=int, default=100)
    parser.add_argument('-c','--chunk', type=int, default=50, help='chunk size in MB')
    parser.add_argument('-o','--overwrite', action='store_true', help='Overwrite Exisitng Files', default=False)
    parser.add_argument('-O', '--older_than', type=int, help='Delete files older than x days', default=0)
    parser.add_argument('-r','--recursive', action='store_true', help='Recurse into subdirectories', default=False)
    parser.add_argument('-l','--log', action='store',type=str, default="/home/nick/Scripts/dropbox.log", help='path/name of log file (default: /home/nick/Scripts/dropbox.log)')
    parser.add_argument('-D','--debug', action='store_true', help='debug mode', default=False)
    parser.add_argument('--version', action='version', version="%(prog)s ("+__version__+")")
    arg = parser.parse_args()
    
    #-------------- Main --------------

    if arg.debug:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO

    #setup logging
    global log
    setup_logger(__name__, arg.log,level=log_level,console=True)

    log = logging.getLogger(__name__)

    #------------ Main ------------------

    log.info("*******************")
    log.info("* Program Started *")
    log.info("*******************")
    
    log.info("Dropbox-Uploader Version: %s" % __version__)
    
    log.info("Python Version: %s" % sys.version.replace('\n',''))
    
    log.debug("DEBUG mode on")
    
    #register interrupt handler
    signal.signal(signal.SIGINT, exit_handler)
    
    if arg.upload_path is not None and not arg.upload_path.startswith('/'):
        arg.upload_path = '/' + arg.upload_path
    
    dbu = DropBoxUpload(timeout=arg.timeout, chunk=arg.chunk, log=log)
    log.info('Action: {}'.format(arg.action))
    if arg.action == 'client':
        dbu.get_client_info()
    elif arg.action == 'list':
        if arg.upload_path is None:
            log.error('--upload_path is required')
            sys.exit(1)
        dbu.list_files(arg.upload_path, arg.recursive)
    elif arg.action == 'info':
        if arg.upload_path is None:
            log.error('--upload_path is required')
            sys.exit(1)
        dbu.get_file_metadata(arg.upload_path)
    elif arg.action == 'upload':
        if arg.file_path is None or arg.upload_path is None:
            log.error('--file_path and  --upload_path are required')
            sys.exit(1)
        dbu.upload_files(arg.file_path, arg.upload_path, arg.overwrite, arg.recursive)
    elif arg.action == 'download':
        if arg.file_path is None or arg.upload_path is None:
            log.error('--file_path and  --upload_path are required')
            sys.exit(1)
        dbu.download_files(arg.file_path, arg.upload_path, arg.overwrite, arg.recursive)
    elif arg.action == 'delete':
        if arg.upload_path is None:
            log.error('--upload_path is required')
            sys.exit(1)
        older_than = datetime.now(timezone.utc) - timedelta(days=arg.older_than)
        log.info('Deleting dropbox folders/files in {} older than {}'.format(arg.upload_path, older_than.isoformat()))
        dbu.delete_files(arg.upload_path, older_than)
    elif arg.action == 'delete_folder':
        if arg.upload_path is None:
            log.error('--upload_path is required')
            sys.exit(1)
        dbu.delete_folder(arg.upload_path)
    elif arg.action == 'monitor':
        if arg.file_path is None or arg.upload_path is None:
            log.error('--file_path and  --upload_path are required')
            sys.exit(1)
        if not os.path.isdir(arg.file_path):
            self.log.error('--file_path: {} Must be a directory'.format(arg.file_path))
            sys.exit(1)
            
        min_free_space = 500 * 1024 * 1024 * 1024
            
        time.sleep(len(dbu.files)*2)    #wait for interrupted files to resume uploading
        older_than = datetime.now(timezone.utc) - timedelta(days=arg.older_than)
        log.info('Uploading files in {} to {} newer than {}'.format(arg.file_path, arg.upload_path, older_than.isoformat()))
        upload = dbu.start_batch_upload(arg.file_path, arg.upload_path, arg.overwrite, arg.recursive, older_than=older_than)
            
        log.info("monitoring directory: {} for changes".format(arg.file_path))

        if is_on_mount(arg.file_path):
            # have to poll a mounted file system - poll every 10 seconds
            observer = PollingObserver(10)
        else:
            observer = Observer()
        Handler = MyHandler(dbu, log, arg)
        observer.schedule(Handler, arg.file_path, recursive=arg.recursive)
        observer.start()
        try:
            while True:
                space = dbu.get_free_space()
                log.info("Monitoring {} files, Dropbox free space: {}".format(len(Handler.threads), human_size(space)))
                if (space < min_free_space):
                    self.log.warning('Free space is {}, less than minnimum allowed: {}'.format(human_size(space), human_size(min_free_space)))
                    if arg.older_than > 0:
                        dbu.delete_files(dbu, arg)
                time.sleep(60)
        except KeyboardInterrupt:
            observer.stop()
        observer.join()
        
    while threading.active_count() > 0:    #wait for resumed uploads to complete
        time.sleep(10)

if __name__ == "__main__":
    main()
