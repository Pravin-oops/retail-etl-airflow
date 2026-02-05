-- Run this as RETAIL_DW
-- Create a logical reference to the physical folder inside the Docker container
-- Note: /data is the path we defined in docker-compose.yml
CREATE OR REPLACE DIRECTORY source_data_dir AS '/data';
-- Verify access (This checks if Oracle can "see" the folder)
SELECT * FROM all_directories WHERE directory_name = 'SOURCE_DATA_DIR';



-- Setting read write permission to our user, run as SYSDBA
GRANT READ, WRITE ON DIRECTORY SOURCE_DATA_DIR TO RETAIL_DW;
