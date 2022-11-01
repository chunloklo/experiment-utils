import ZODB, ZODB.FileStorage
import os
from filelock import FileLock 
from BTrees import OOBTree
import transaction
from ZODB.blob import Blob
import gc
import pickle

zodb_connection = None

DB_DATA_KEY = 'data'
DB_CONFIGS_KEY = 'configs'
DB_FOLDER = 'results_dbs'

def db_exists(db_folder):
    return os.path.isfile(f'{db_folder}/data.fs')

def open_db(db_folder, create_if_not_exist=True):
    if not db_exists(db_folder) and not create_if_not_exist:
        raise ValueError(f'Database at {db_folder} does not yet exist and open_db has been told not to create a database if it does not exit yet. Erroring out')

    os.makedirs(db_folder, exist_ok=True)
    lock_name = f'{db_folder}/access.lock'
    lock = FileLock(lock_name)
    lock.acquire()

    blob_dir = f'{db_folder}/blobs'

    storage = ZODB.FileStorage.FileStorage(f'{db_folder}/data.fs', blob_dir=blob_dir)
    db = ZODB.DB(storage)
    connection = db.open()
    root = connection.root()

    return db, connection, root, lock

def close_db(db, connection, lock):
    connection.close()
    db.close()
    lock.release()
    # Running garbage collection when the database is closed since opening the database require significant memory usage.
    # Otherwise repeated save calls build up a ton of memory usage.
    gc.collect()

def pack_db(db_folder):
    db, connection, root, lock = open_db(db_folder, False)
    db.pack()
    db.pack()
    close_db(db, connection, lock)
    
def is_blob(record):
    return isinstance(record, Blob)

def convert_if_blob(record):
    """Converts the record into data if it is saved as blob. Otherwise, save it as usual
    """
    if is_blob(record):
        f = record.open()
        return pickle.load(f)
    else:
        return record

def zodb_op_save(root, config_id, data, key, use_blob=True):
    if key not in root:
        root[key] = OOBTree.OOBTree()

    if use_blob:
        binary_data = pickle.dumps(data)
        root[key][config_id] = Blob(binary_data)
    else:
        root[key][config_id] = data

    transaction.commit()

def zodb_op_load(root, config_id, key):
    if key not in root:
        raise IndexError(f'{key} object is not in the database yet. Cannot retrieve any {key} for {config_id}')

    if config_id not in root[key]:
        raise IndexError(f'Cannot find config id {config_id} in {key} object')

    record = root[key][config_id]

    return convert_if_blob(record)

def zodb_op_check_exists(root, config_id, key):
    if key not in root:
        return False
        
    if config_id not in root[key]:
        return False

    return True

def save_config_and_data_zodb(config_id, config, data, db_folder):
    db, connection, root, lock = open_db(db_folder)
    zodb_op_save(root, config_id, config, DB_CONFIGS_KEY)
    zodb_op_save(root, config_id, data, DB_DATA_KEY)
    close_db(db,  connection, lock)

def load_data_config_from_id(config_id, db_folder):
    global zodb_connection
    if zodb_connection is None:
        db, connection, root, lock = open_db(db_folder, create_if_not_exist=False)
    else:
        db, connection, root, lock = zodb_connection

    config = zodb_op_load(root, config_id, DB_CONFIGS_KEY)
    data = zodb_op_load(root, config_id, DB_DATA_KEY)

    if zodb_connection is None:
        close_db(db,  connection, lock)
    
    return config, data

def check_config_exists(config_id, db_folder):
    global zodb_connection
    if zodb_connection is None:
        db, connection, root, lock = open_db(db_folder, create_if_not_exist=False)
    else:
        db, connection, root, lock = zodb_connection

    config_exists = zodb_op_check_exists(root, config_id, DB_CONFIGS_KEY)

    if zodb_connection is None:
        close_db(db,  connection, lock)
    
    return config_exists
    
class BatchDBAccess():
    """Used to open DB for batch retrieval access.
    Example: with BatchDBAccess(): # Retrieve data
    """
    def __init__(self, db_folder):
        self.db_folder = db_folder
    def __enter__(self):
        global zodb_connection
        zodb_connection = open_db(self.db_folder, create_if_not_exist=False)
    def __exit__(self, exc_type, exc_value, exc_traceback):
        global zodb_connection
        # db, connection, and lock
        close_db(zodb_connection[0], zodb_connection[1], zodb_connection[3])
        zodb_connection = None