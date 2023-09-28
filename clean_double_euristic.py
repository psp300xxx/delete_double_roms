import os
import sys
import hashlib
import logging
import tqdm
import tempfile
import json
from typing import List

FORMAT = '%(asctime)s %(message)s'
logging.basicConfig(format=FORMAT)
HANDLER = logging.FileHandler("./delete.log")
LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(HANDLER)
LOGGER.setLevel(logging.DEBUG)

def compute_hashes(path: str) -> List[str]:
    BUFFSIZE = 32000
    results: List[int] = list()
    with open(path) as f:
        is_completed = False
        while not is_completed:
            data = f.read(BUFFSIZE)
            if not data:
                is_completed = True
                continue
            current_hash = hashlib.sha256()
            current_hash.update(data.encode("utf-8"))
            digest = current_hash.hexdigest()
            results.append(digest)
    return results

def delete_pairs(directory: str) -> int:
    files = list(os.scandir(directory))
    files_to_be_removed: List[str] = list()
    for i in tqdm.tqdm(range(len(files))):
        for j in tqdm.tqdm(range(i+1, len(files))):
            left, right = files[i],files[j]
            left_dict = None
            right_dict = None
            with open(left.path) as f:
                left_dict = json.load(f)
            with open(right.path) as f:
                right_dict = json.load(f)
            left_digests = left_dict["digests"]
            right_digests = right_dict["digests"]
            full_set = set(left_digests)
            for dig in right_digests:
                full_set.add(dig)
            left_max_size = int( len(left_digests)*1.2 )
            if len(full_set) <= left_max_size:
                LOGGER.info( "{},{} matches, second will be deleted".format(left_dict["filepath"], right_dict["filepath"])  )
                files_to_be_removed.append( right_dict["filepath"]  )
    for path in files_to_be_removed:
        os.remove(path)
    return len(files_to_be_removed)



def delete_doubles_files_from(directory: str, tmp_dir: str) -> int:
    LOGGER.info("Deleting file from directory:'{}'".format(directory))
    queue = [directory]
    while queue:
        next_dir = queue.pop()
        LOGGER.info("Visiting: '{}'".format(next_dir))
        for file in os.scandir(next_dir):
            if file.is_dir():
                queue.append(file.path)
                continue
            current_hashes: List[str] = compute_hashes(file.path)
            store_hashes(hashes=current_hashes, tmp_dir=tmp_dir, file_path=file.path,file_name=file.name)
    return delete_pairs(directory= tmp_dir)


def store_hashes( hashes: List[str], tmp_dir: str ,file_path: str, file_name: str)-> None:
    if not os.path.exists(tmp_dir):
        os.makedirs(tmp_dir)
    destination_file = os.path.join( tmp_dir, "{}_{}.json".format(file_name, hashlib.md5(file_path.encode("utf-8")).hexdigest()) )
    dict = {
        "filepath": file_path,
        "digests": hashes
    }
    with open(destination_file, "w") as f:
        json.dump( dict, f )

def create_temp_dir() -> str:
    with tempfile.TemporaryDirectory() as tmp_dir:
        if not os.path.exists(tmp_dir):
            os.makedirs(tmp_dir)
        else:
            for file in os.scandir(tmp_dir):
                os.remove(file.path)
        return tmp_dir
    return None


def main(root_dir: str):
    temp_directory = create_temp_dir()
    if temp_directory is None:
        LOGGER.error("Unable to create temp dir")
        exit(1)
    LOGGER.info( "Tmp dir created at: '{}'".format(temp_directory) )
    removed_files_number = delete_doubles_files_from(directory=root_dir, tmp_dir=temp_directory)
    LOGGER.info("I deleted '{}' files".format(removed_files_number))

if __name__ == "__main__":
    main( sys.argv[1] )