This repo contains code for an interview project. There are three directories containing data files, and two scripts.

The directory 1_source_files contains unmodified files received as part of the interview prompt.

The directory 2_modified_files contains files that were updated - either programmatically or manually - from a file in 1_source_files.

The directory 3_generated_files contains files that are newly generated files, rather than simple updates of an existing file. (They may be generated using data from existing files, but they are not replacing anything in 1_source_files.)

The two scripts are initial_transform_and_generate_join_table.py and create_db_and_sql_tables.sql. The Python file performs small data cleaning transformations, and generates a file that will be utilized later to join two disparate datasets. The SQL file creates a database, schema, and tables, as well as a view utilizing the "join" dataset generated in the Python script.