# Sparkify
Sparkify is startup company that want's someone to analyze user activity data that they collected from their music app. Currently, the data resides in a directory of JSON logs.
this is my proposed solution.

### BOM (Bill of Materials)
    - postgres database
    - python3 with psycopg2 and pandas

### Design
I have built the database using the Start schema to achieve fast aggregation and for the simplicity of the reuired queries.

The star schema design as follows:  
    -Fact table: songplays
    -Dimensions tables: songs, artists, users, time.

### Installation
Run `create_tables.py` to create the database and tables then run `etl.py`
