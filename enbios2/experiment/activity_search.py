# try full text search like
# https://charlesleifer.com/blog/using-sqlite-full-text-search-with-python/
from pathlib import Path
from typing import Optional

import bw2data
from bw2data.backends import ActivityDataset
from playhouse.sqlite_ext import *
from tqdm import tqdm

from enbios2.ecoinvent.spatial import geo_code2name, get_ecoinvent_geo_data
from enbios2.experiment.databases import DBTypes, create_db
from enbios2.experiment.db_models import BW_Activity, FTS_BW_ActivitySimple
from enbios2.generic.enbios2_logging import get_logger
from enbios2.generic.files import ReadDataPath

logger = get_logger(__file__)

def build_fts(bw_project: str, db_path: Path, ecoinvent_geo_xml_path: Optional[str] = None):
    logger.info(f"Building FTS for {bw_project} to {db_path}")
    create_db(db_path, db_type=DBTypes.ActivityFTS)
    bw2data.projects.set_current(bw_project)
    activities = ActivityDataset.select()

    # content_fields = ["name", "product", "comment"]

    geo_code2_name = {}
    if ecoinvent_geo_xml_path:
        geo_code2_name = geo_code2name(get_ecoinvent_geo_data(ReadDataPath(ecoinvent_geo_xml_path)))

    for act in tqdm(activities):
        db_a = BW_Activity.create(code=act.code, database=act.database, name=act.name, location=act.location,
                                  location_name=geo_code2_name.get(act.location, ""),
                                  product=act.product if act.product else "",
                                  type=act.type, comment=act.data.get("comment", ""),
                                  synonyms="##".join(act.data.get("synonyms", "")),
                                  classification=act.data.get("classification", ""),
                                  unit=act.data.get("unit", ""),
                                  reference_product=act.data.get("reference_product", ""))

        # co = (getattr(db_a, field, "") for field in content_fields)
        # co_s = "\n".join(f for f in co if f)
        FTS_BW_ActivitySimple.create(docid=db_a.id,
                                     name=db_a.name,
                                     comment=db_a.comment,
                                     location_name=db_a.location_name,
                                     product=db_a.product,
                                     synonyms=db_a.synonyms)


def _parse_match_info(buf):
    bufsize = len(buf)  # Length in bytes.
    return [struct.unpack('@I', buf[i:i + 4])[0] for i in range(0, bufsize, 4)]


def rank(raw_match_info):
    # handle match_info called w/default args 'pcx' - based on the example rank
    # function http://sqlite.org/fts3.html#appendix_a
    match_info = _parse_match_info(raw_match_info)
    score = 0.0
    p, c = match_info[:2]
    for phrase_num in range(p):
        phrase_info_idx = 2 + (phrase_num * c * 3)
        for col_num in range(c):
            col_idx = phrase_info_idx + (col_num * 3)
            x1, x2 = match_info[col_idx:col_idx + 2]
            if x1 > 0:
                score += float(x1) / x2
    return -score


def search(text: str):
    query = (BW_Activity
             .select(BW_Activity.name)
             .join(FTS_BW_ActivitySimple, on=(BW_Activity.id == FTS_BW_ActivitySimple.docid))
             .where(FTS_BW_ActivitySimple.match(text)).order_by(FTS_BW_ActivitySimple.rank()).limit(20)
             .dicts())
    for row_dict in query:
        print(row_dict)


build_fts("ecoi_dbs",
          Path('fts_db/cutoff391.db'),
          "ecoinvent/ecoinvent 3.9.1_cutoff_ecoSpold02/MasterData/Geographies.xml")
