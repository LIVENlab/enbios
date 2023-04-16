"""
  - NIS -> Nextcloud. Export a case study to a Nextcloud folder:
    - Link a Nextcloud folder to a NIS case study.
    - Dublin Core. "convert_generator_to_dublin_core"
    - MSM JSON. The NIS format for the representation of sequences of commands. "convert_generator_to_json_generator"
  - Nextcloud -> NIS. ---

* BASIC EXAMPLE *

>> pip install webdavclient

UPLOAD:

import webdav.client as wc
options = {
    "webdav_hostname": "https://nextcloud.data.magic-nexus.eu/remote.php/dav/files/NIS_agent/",
    "webdav_login": "NIS_agent",
    "webdav_password": "***"
}
client = wc.Client(options)
client.upload_sync(remote_path="/NIS_exports/r.xlsx", local_path="/home/rnebot/GoogleDrive/AA_MAGIC/nis-backend/backend_tests/z_input_files/reproduce_million_rows.xlsx")

------------

DOWNLOAD:

options = {
    "webdav_hostname": "https://nextcloud.data.magic-nexus.eu/",
    "webdav_login": "NIS_agent",
    "webdav_password": "***"
}

client = wc.Client(options)
client.download_sync(remote_path="/remote.php/webdav/NIS_beta/CS_format_examples/08_caso_energia_eu_new_commands.xlsx", local_path="/home/rnebot/r3.xlsx")

"""
from nexinfosys.models.musiasem_methodology_support import CaseStudy


def upload_case_study(folder_name: str, cs: CaseStudy):
    """
    When exporting, save to Nextcloud. How? -> .xlsx file, ¿output variables also?, readme.txt with the person. The
    folder name would be the case study code plus a timestamp?
        Launch asynchronous? -> Potentially slow
        Create directory
            Submit .xlsx file
            Submit README.MD file
            Submit outputs?

    :param folder_name:
    :param cs:
    :return:
    """
    pass
