import sys
import os
import logging
# import tempfile
import wget
import subprocess
import copy
import mediacloud.api
from dotenv import load_dotenv

# set up basic logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s | %(levelname)s | %(name)s | %(message)s')
logger = logging.getLogger(__name__)
logger.info("------------------------------------------------------------------------")

# load in config variables
load_dotenv()
MC_API_KEY = os.getenv('MC_API_KEY')
mc = mediacloud.api.AdminMediaCloud(MC_API_KEY)
ELASTIC_SEARCH_HOST = os.getenv('ELASTIC_SEARCH_HOST')


def file_size_mb(file_path):
    return os.stat(file_path).st_size / 1000000


def get_files_dir():
    # temp_dir = tempfile.mkdtemp(prefix="{}-{}".format(topics_id, snapshots_id))
    base_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_dir, 'downloads')


def download_snapshot_files(topics_id: int, snapshots_id: int, download_dir: str, force: bool = False):
    logger.info("Downloading files for topic {}, snapshot {}".format(topics_id, snapshots_id))
    # figure out what files to download
    snapshot_files = mc.topicSnapshotFiles(topics_id, snapshots_id=snapshots_id)['snapshot_files']
    # save them to temporary dir
    logger.info("  will download {} files to {}".format(len(snapshot_files), download_dir))
    results = []
    for file in snapshot_files:
        logger.info("  Downloading {}".format(file['name']))
        logger.debug("    from {}".format(file['url']))
        file_name = "{}-{}-{}-{}".format(topics_id, snapshots_id, file['snapshot_files_id'], file['name'])
        gzipped_file_name = "{}.gz".format(file_name)
        zip_file_path = os.path.join(download_dir, gzipped_file_name)
        json_file_path = os.path.join(download_dir, "{}.json".format(file_name))
        if force or not os.path.exists(json_file_path):
            # download it if we don't have an expanded copy, or a zipped copy
            if force or not os.path.exists(zip_file_path):
                wget.download(file['url'], zip_file_path)
            zip_file_size_mb = file_size_mb(zip_file_path)
            logger.debug("    saved {:0.2f} MB".format(zip_file_size_mb))
            # and we should unzip it and rename it
            subprocess.check_output(["gunzip", zip_file_path])
            tmp_file_path = os.path.join(download_dir, file_name)
            os.rename(tmp_file_path, json_file_path)
        raw_size_mb = file_size_mb(json_file_path)
        logger.debug("    unzipped size {:0.2f} MB".format(raw_size_mb))
        # return the paths that might be useful for processing
        info = copy.deepcopy(file)
        info['zip_file_path'] = zip_file_path
        info['file_path'] = json_file_path
        info['topics_id'] = topics_id
        info['snapshots_id'] = snapshots_id
        results.append(info)
    logger.info("  done downloading snapshot files")
    return results


def upload_to_kibana(ndjson_file_path: str, es_host: str, index_name: str, mappings_file_path: str):
    logger.info("  Upload {}".format(ndjson_file_path))
    logger.debug("    into index {} on {}".format(index_name, es_host))
    logger.debug("    using mapping {}".format(mappings_file_path))
    logger.debug("    file size: {:0.2f} MB".format(file_size_mb(ndjson_file_path)))
    cmd = ["elasticsearch_loader",
           "--es-host={}".format(es_host),
           "--index={}".format(index_name),
           "--index-settings-file={}".format(mappings_file_path),
           "json",
           ndjson_file_path,
           "--json-lines"
           ]
    logger.debug(" ".join(cmd))
    #subprocess.check_output(cmd)


if __name__ == "__main__":
    # parse out command line args
    if len(sys.argv) is not 3:
        logger.error("You must pass in a topics_id and snapshots_id")
        sys.exit()
    topic = int(sys.argv[1])
    snapshot = int(sys.argv[2])
    mappings_file_path = os.path.join('mappings', 'mediacloud-topic-post-mappings.json')
    # we first need to get the very large dump files with all the content
    file_info = download_snapshot_files(topic, snapshot, get_files_dir())
    # now try to upload the files to kibana
    logger.info("Ready to upload {} files".format(len(file_info)))
    for f in file_info:
        index_name = 'topic-{}-snapshot-{}'.format(f['topics_id'], f['snapshots_id'])
        upload_to_kibana(f['file_path'], ELASTIC_SEARCH_HOST, index_name, mappings_file_path)
