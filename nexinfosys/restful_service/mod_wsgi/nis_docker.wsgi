import os
import sys

code_path = '/app/'

print("--- Starting Magic NIS ---")
if "MAGIC_NIS_SERVICE_CONFIG_FILE" in os.environ and os.environ["MAGIC_NIS_SERVICE_CONFIG_FILE"]:
    conf_file_name = os.environ["MAGIC_NIS_SERVICE_CONFIG_FILE"]
    print("Configuration file name from Environment: " + conf_file_name)
else:
    raise Exception("A configuration file path must be specified in environment variable MAGIC_NIS_SERVICE_CONFIG_FILE"
#    conf_file_name = 'nis_docker_naples.conf'
#    print("Literal configuration file name")

if code_path not in sys.path:
    sys.path.insert(0, code_path)
print(sys.path)

from nexinfosys import cfg_file_env_var
os.environ[cfg_file_env_var] = code_path + "backend/restful_service/" + conf_file_name
print("Resulting config file name: "+os.environ[cfg_file_env_var])

# TODO Disable if deployed in Docker container. The Docker container should be immutable, so no changes to source code
# TODO expected. If any, do a Stop -> Start cycle.

# import backend.restful_service.mod_wsgi.monitor as monitor
# monitor.start(interval=1.0)
# monitor.track(os.path.join(os.path.dirname(__file__), 'site.cf'))

import backend.restful_service.service_main
from backend.restful_service import app as application

print("--- Magic NIS started ---")

