"""
  Given a dataset:
    * generate a "<schema>.xml" record (Mondrian specific)
    * generate and execute a DDL to create a set of relational database tables (first check if they exist) and
    * load data into them

  * Also: possibility to destroy the set of tables
  * Also: ability to notify Mondrian of the availability of the cube (maybe just start or restart Mondrian instance)

  The dataset uses the dataset model: Dataset, Dimension, Codelist for metadata and pd.DataFrame for the data

  The preparation of tables in the relational database overwrites existing
  The <schema>.xml which goes into "schema" directory in the deployed Mondrian WAR

  Packages that can help:
   - Pymondrian. https://github.com/gabitoju/pymondrian
   - Pygrametl. https://github.com/chrthomsen/pygrametl
   - Pylytics

"""

