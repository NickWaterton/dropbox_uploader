# dropbox_uploader
Uploads files to Dropbox uses Python 3.8 minnimum

## Requirements

Need to install:
* dropbox python api
* watchdog
```
pip install dropbox
pip install watchdog
```
Uses linux signals module

## Authentication

Need to create a dropbox app via the Dropbox developer page

See https://developers.dropbox.com/oauth-guide

When you have an AppKey edit dropbox_upload.py and edit `APP_KEY = ''` to add your App Key.  
OR append the AppKey to the command line as follows:

Run the program `./dropbox_upload.py client -a <AppKey>` which will then give instructions on how to authorize the program for Dropbox access.

The generated credentials are saved in the file `config.json` so you only have to perform the authorization once. Never share this file!

if you need to re-authenticate for some reason, delete `config.json` and re-run the program as above. 

## Usage

```
nick@Backup-Public:~/Scripts/dropbox_upload$ ./dropbox_upload.py -h
usage: dropbox_upload.py [-h] [-f FILE_PATH] [-u UPLOAD_PATH] [-t TIMEOUT] [-c CHUNK] [-m MIN_FREE_SPACE] [-o] [-O OLDER_THAN] [-r]
                         [-l LOG] [-D] [-a APP_KEY] [--version]
                         {client,list,info,upload,delete,delete_folder,monitor,download}

Upload/Download files to/from dropbox

positional arguments:
  {client,list,info,upload,delete,delete_folder,monitor,download}
                        action to take (default: None)

optional arguments:
  -h, --help            show this help message and exit
  -f FILE_PATH, --file_path FILE_PATH
                        path to file to upload (default: None)
  -u UPLOAD_PATH, --upload_path UPLOAD_PATH
                        path in dropbox (default: None)
  -t TIMEOUT, --timeout TIMEOUT
                        upload timeout (seconds) (default: 100)
  -c CHUNK, --chunk CHUNK
                        chunk size in MB (default: 50)
  -m MIN_FREE_SPACE, --min_free_space MIN_FREE_SPACE
                        minnimum free space for upload when monitoring as % of total (default: 25)
  -o, --overwrite       Overwrite Exisitng Files (default: False)
  -O OLDER_THAN, --older_than OLDER_THAN
                        Delete files older than x days (0=disabled) (default: 0)
  -r, --recursive       Recurse into subdirectories (default: False)
  -l LOG, --log LOG     path/name of log file (default: /home/nick/Scripts/dropbox.log)
  -D, --debug           debug mode (default: False)
  -a APP_KEY, --app_key APP_KEY
                        your APP_KEY from Dropbox (only needed for first time authentication) (default: None)
  --version             show program's version number and exit
```
