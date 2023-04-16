"""
COMEXT database

This database is Eurostat, but uses an organization differentiated from the rest of datasets.

An extract of mail messages between Eurostat's support and MAGIC development is at the end of the file (Spanish)

"""

import os
import re
import tempfile
from io import StringIO, BytesIO
from abc import abstractmethod
import zipfile
from typing import List
import pandas as pd
import numpy as np
import requests

from nexinfosys.common.helper import translate_case
from nexinfosys.ie_imports.data_source_manager import IDataSourceManager, get_dataset_structure, \
    filter_dataset_into_dataframe
from nexinfosys.ie_imports.data_sources.eurostat_bulk import Eurostat
from nexinfosys.models.statistical_datasets import DataSource, Database, Dataset, Dimension


dataset_csv = """
Code,File,Label
DS-066341,epannsold-r2.zip,"Sold production, exports and imports by PRODCOM list (EUROPROMS Nace r2)"
DS-066342,epanntotal-r2.zip,"Total production by PRODCOM list (EUROPROMS NACE r2)"
DS-043408,epannsold.zip,"Sold production, exports and imports by PRODCOM list (EUROPROMS Nace r1)"
DS-043409,epanntotal.zip,"Total production by PRODCOM list (EUROPROMS NACE r1)"
"""

# Map of correspondence of field names, DS-066341 -> DS-043408
# Also valid for correspondence of field names between DS-066342 and DS-043409
map_r2_r1_fields = {
    "DECL": "DECLARANT",
    "PRCCODE": "PRCCODE",
    "PERIOD": "PERIOD",
    "EXPQNT": "EXP_QUANTITY",
    "EXPVAL": "EXP_VALUE",
    "IMPQNT": "IMP_QUANTITY",
    "IMPVAL": "IMP_VALUE",
    "PRODQNT": "PROD_QUANTITY",
    "PVALBASE": "PROD_VALUE_BASE",
    "PQNTFLAG": "PROD_QUANTITY_FLAG",
    "PQNTBASE": "PROD_QUANTITY_BASE",
    "PRODVAL": "PROD_VALUE_EUR",
    "PVALFLAG": "PROD_VALUE_FLAG",
    "QNTUNIT": "UNIT"
}

dimension_descriptions = {
    "DECL": "Country",
    "PRCCODE": "NACE code",
    "PERIOD": "Year",
    "EXPQNT": "Exported quantity",
    "EXPVAL": "Exported price",
    "IMPQNT": "Imported quantity",
    "IMPVAL": "Imported price",
    "PRODQNT": "Sold production quantity",
    "PRODVAL": "Sold production price",
    "PVALBASE": "Sold production price BASE",
    "PVALFLAG": "Sold production price FLAG",
    "PQNTFLAG": "Sold production quantity FLAG",
    "PQNTBASE": "Sold production quantity BASE",
    "QNTUNIT": "Unit for quantities"
}


class COMEXT(IDataSourceManager):
    def __init__(self):
        self._eurostat = Eurostat()  # Used to obtain datasets Metadata
        self._bulk_download_url = "https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?" \
                                  "sort=1&file=comext%2FCOMEXT_OTHER_DATA%2FEUROPROMS%2F"
        self._datasets = pd.read_csv(StringIO(dataset_csv))
        self._datasets["idx"] = self._datasets["Code"]
        self._datasets.set_index("idx", inplace=True)

    def get_name(self) -> str:
        """ Source name """
        return self.get_datasource().name

    def get_datasource(self) -> DataSource:
        """ Data source """
        src = DataSource()
        src.name = "COMEXT"
        src.description = "Eurostat's COMEXT"
        return src

    def get_databases(self) -> List[Database]:
        """ List of databases in the data source """
        db = Database()
        db.code = ""
        db.description = "Comext"
        return [db]

    def get_datasets(self, database=None) -> list:
        """ List of datasets in a database, or in all the datasource (if database==None)
            Return a list of tuples (database, dataset)
        """
        return [(r["Code"], r["Label"], r["Code"]) for i, r in self._datasets.iterrows()]

    def get_dataset_structure(self, database, dataset) -> Dataset:
        """ Obtain the structure of a dataset: concepts, dimensions, attributes and measures """
        ds_tmp = self._eurostat.get_dataset_structure(None, dataset)
        dims = {d.code: d for d in ds_tmp.dimensions}

        ds = Dataset()
        ds.code = ds_tmp.code
        ds.description = ds_tmp.description
        ds.database = ds_tmp.database

        # Dimensions are: DECLARANT (Country) and PRCCODE (NACE + others Code), PERIOD (Time)
        declarant = dims["DECL" if dataset.startswith("DS-066") else map_r2_r1_fields["DECL"]]
        dd = Dimension()
        dd.code = "DECL" if dataset.startswith("DS-066") else map_r2_r1_fields["DECL"]
        dd.description = dimension_descriptions["DECL"]
        dd.attributes = None
        dd.is_time = None
        dd.is_measure = None
        dd.dataset = ds
        dd.code_list = declarant.code_list.clone()
        # dd.code_list.dimension = dd

        prccode = dims["PRCCODE"]
        dd = Dimension()
        dd.code = "PRCCODE" if dataset.startswith("DS-066") else map_r2_r1_fields["PRCCODE"]
        dd.description = dimension_descriptions["PRCCODE"]
        dd.attributes = None
        dd.is_time = None
        dd.is_measure = None
        dd.dataset = ds
        dd.code_list = prccode.code_list.clone()
        # dd.code_list.dimension = dd

        dd = Dimension()
        dd.code = "PERIOD" if dataset.startswith("DS-066") else map_r2_r1_fields["PERIOD"]
        dd.description = dimension_descriptions["PERIOD"]
        dd.attributes = None
        dd.is_time = True
        dd.is_measure = None
        dd.dataset = ds

        if dataset in ("DS-066341", "DS-043408"):
            measures = ["PRODQNT", "EXPQNT", "IMPQNT", "PQNTBASE", "PRODVAL", "EXPVAL", "IMPVAL", "PVALBASE"]
            attributes = ["QNTUNIT", "PVALFLAG", "PQNTFLAG"]
        else:
            measures = ["PRODQNT", "PQNTBASE"]
            attributes = ["PQNTFLAG", "QNTUNIT"]

        for m in measures:
            dd = Dimension()
            dd.code = m if dataset.startswith("DS-066") else map_r2_r1_fields[m]
            dd.description = dimension_descriptions[m]
            dd.attributes = None
            dd.is_time = None
            dd.is_measure = True
            dd.dataset = ds
        # TODO Attributes

        return ds

    def etl_full_database(self, database=None, update=False):
        """ If bulk download is supported, refresh full database """
        pass

    def etl_dataset(self, dataset, update=False):
        """ If bulk download is supported, refresh full dataset """
        file_base = self._datasets.loc[dataset, "File"]
        url = self._bulk_download_url + file_base
        zip_name = tempfile.gettempdir() + "/" + file_base
        if os.path.isfile(zip_name):
            if not update:
                return zip_name
            else:
                os.remove(zip_name)

        r = requests.get(url, stream=True)
        # http://stackoverflow.com/questions/15352668/download-and-decompress-gzipped-file-in-memory
        with open(zip_name, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)

        return zip_name

    def get_dataset_filtered(self, dataset: str, dataset_params: list) -> Dataset:
        """ Obtains the dataset with its structure plus the filtered values
            The values can be in a pd.DataFrame or in JSONStat compact format
            After this, new dimensions can be joined, aggregations need to be performed
        """
        def multi_replace(text, rep):
            rep = dict((re.escape(k), v) for k, v in rep.items())
            pattern = re.compile("|".join(rep.keys()))
            return pattern.sub(lambda m: rep[re.escape(m.group(0))], text)

        # Read dataset structure
        ds = self.get_dataset_structure(None, dataset)

        # Read full dataset into a Dataframe
        dataframe_fn = tempfile.gettempdir() + "/" + dataset + ".bin2"
        df = None
        if os.path.isfile(dataframe_fn):
            df = pd.read_parquet(dataframe_fn)
            # df = pd.read_msgpack(dataframe_fn)

        if df is None:
            zip_name = self.etl_dataset(dataset, update=False)
            with zipfile.ZipFile(zip_name, "r") as arch:
                # Read file and
                st = arch.read(arch.filelist[0].filename)
                # pattern = re.compile("(\\:)|( [b-fnpruzscde]+)")
                # st = pattern.sub(lambda m: "NaN" if m.group(0) == ":" else "", gz.read().decode("utf-8"))

                fc = BytesIO(st)
            # os.remove(zip_name)  # Remove, because the dataset is stored as PARQUET dataframe format
            df = pd.read_csv(fc, dtype={"PRCCODE": "str",
                                        "DECL" if dataset.startswith("DS-066") else map_r2_r1_fields["DECL"]: "str"
                                        }
                             )

            # Get the list of dimensions and of measures
            dimensions = []
            measures = []
            for d in ds.dimensions:
                if d.is_measure:
                    measures.append(d.code)
                else:
                    dimensions.append(d.code)

            # Convert measures to numeric
            to_delete = []
            for cn in df.columns:
                if cn in measures:
                    df[cn] = df[cn].astype(np.float)
                elif cn not in dimensions:
                    to_delete.append(cn)
                elif cn == "PERIOD":
                    # Remove the tail "52" (meaning "year")
                    df[cn] = (df[cn]//100).astype("str")

            # Delete unused columns
            for cn in to_delete:
                del df[cn]

            # Set index on the dimension columns
            df.set_index(dimensions, inplace=True)
            # Save df
            df.to_parquet(dataframe_fn)
            # df.to_msgpack(dataframe_fn)

        # Change dataframe index names to match the case of the names in the metadata
        # metadata_names_dict = {dim.code.lower(): dim.code for dim in ds.dimensions}
        # dataframe_new_names = [metadata_names_dict.get(name.lower(), name) for name in df.index.names]
        dataframe_new_names = translate_case(df.index.names, [dim.code for dim in ds.dimensions])
        df.index.names = dataframe_new_names

        # Filter it using generic Pandas filtering capabilities
        def obtain_periods_to_filter(filter_dict):
            start = None
            if "StartPeriod" in filter_dict:
                start = filter_dict["StartPeriod"]
                del filter_dict["StartPeriod"]
                if isinstance(start, list): start = start[0]
            if "EndPeriod" in filter_dict:
                endd = filter_dict["EndPeriod"]
                del filter_dict["EndPeriod"]
                if isinstance(endd, list): endd = endd[0]
            else:
                if start:
                    endd = start
            if start:
                # Assume year, convert to integer, generate range, then back to string
                start = int(start)
                endd = int(endd)
                filter_dict["PERIOD"] = [str(a) for a in range(start, endd + 1)]

        if dataset_params:
            obtain_periods_to_filter(dataset_params)
            ds.data = filter_dataset_into_dataframe(df, dataset_params, dataset, eurostat_postprocessing=False)
        else:
            ds.data = df

        return ds

    def get_refresh_policy(self):  # Refresh frequency for list of databases, list of datasets, and dataset
        pass


if __name__ == '__main__':
    e = COMEXT()
    e.get_dataset_filtered("DS-066341", None)


"""
QQQQ
Buenas tardes,

Me gustaría saber si existe la posibilidad de descargar por completo (bulk) desde alguna URL los datsets PRODCOM,
además sin requerir interacción humana.

Ahora mismo más de 6000 de los datasets de Eurostat se pueden descargar perfectamente de esta manera.
Sin embargo no he encontrado los de PRODCOM, y era simplemente saber si es que no es posible o si la URL
de descarga es distinta.

Muchas gracias.
---------------------------------------------------------------------------------------------------------------------
AAAA
Gracias por su interés en la página web de Eurostat, www.ec.europa.eu/eurostat, y por plantearnos su consulta telefónica.

He encontrado la opción de descarga para PRODCOM. Le adjunto a continuación el enlace a esos archivos y la ruta seguida.
El único problema es que los datos son de finales de 2017. Hemos preguntado al equipo responsable y, como le acabo de
comentar por teléfono, nos han comunicado que están trabajando en las actualizaciones y que esperan subir nuevos datos
para finales de la próxima semana, en principio.

Aquí tiene el enlace a los datos de PRODCOM:

http://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?sort=1&dir=comext%2FCOMEXT_OTHER_DATA%2FEUROPROMS

Ruta: Bulk Download Listing > comext > COMEXT OTHER DATA > Europroms > seleccione a continuación el archivo o archivos
que más le interesen.

Esperamos que esta información le sea de utilidad.
---------------------------------------------------------------------------------------------------------------------
QQQQ
La información que me pasas ya me sirve bastante.

Para completar el desarrollo que tengo que hacer, necesitaría saber también cómo consultar los metadatos. Con los demás
datasets de Eurostat existe una URL donde se obtiene una información en formato SDMX. Me pasa algo parecido a lo
anterior, y es que no sé qué URL habría para consultar los datasets de COMEXT. Por ejemplo para los demás datasets de
Eurostat esta URL es:

http://ec.europa.eu/eurostat/SDMX/diss-web/rest/dataflow/ESTAT/all/latest

¿Hay alguna equivalente para COMEXT?
---------------------------------------------------------------------------------------------------------------------
AAAA
Buenos días de nuevo:

Me alegro mucho de que le haya servido. Me comunican que para COMEXT y PRODCOM no disponemos de un archivo en formato
SDMX como los otros que ha encontrado. Los metadatos sí que los tenemos en la página de Eurostat para las tablas de
PRODCOM:

http://ec.europa.eu/eurostat/web/prodcom/data/database

En el símbolo con la M al lado del nombre de la carpeta encontrará la información. Le paso el enlace también:

http://ec.europa.eu/eurostat/cache/metadata/en/prom_esms.htm

Espero que, teniendo la información, pueda manejarla usted mismo para su proyecto. En cualquier caso, recuerde mirar
los otros enlaces la semana que viene, por si hubiera una actualización en este sentido.

"""
