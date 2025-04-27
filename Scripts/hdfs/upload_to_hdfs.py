from hdfs import InsecureClient
import os
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('hdfs_uploader')



def upload_to_hdfs():
    logger.info("Uploading to HDFS")
    HDFS_URI = "http://namenode:9870"  
    LOCAL_dir = "/opt/airflow/includes/local_warehose/bronze"
    HDFS_dir = "/bronze"
    client = InsecureClient(HDFS_URI, user='hadoop')  
    list_filse = []
    for file_name in os.listdir(LOCAL_dir):
        if file_name.endswith(".json"):
            local_path = os.path.join(LOCAL_dir, file_name)
            # check if the file is fit for upload
            df = pd.read_json(local_path, lines=True)
            if 'data' in df.columns and df['data'].astype(str).str.contains('We have detected your API key as None',case=False).any():
                logger.info(f"File {file_name} contains 'We have detected' and will not be uploaded.")
                os.remove(local_path)
                continue 

            hdfs_path = os.path.join(HDFS_dir, file_name)
            client.upload(hdfs_path=hdfs_path, local_path=local_path, overwrite=True)
            list_filse.append(file_name)
            logger.info(f"Uploaded {local_path} to {hdfs_path}")
            os.remove(local_path) 
            logger.info(f"Removed local file {local_path} after upload")
    df = pd.DataFrame(list_filse, columns=['file_name'])
    path = os.path.join('/opt/airflow/Source', 'new_json_files.csv')
    os.remove(path) 
    df.to_csv(path, index=False)
    client.upload(hdfs_path='/bronze/new_json_files.csv', local_path=path, overwrite=True)
    logger.info("Uploaded list of files to HDFS")
    
    
if __name__ == "__main__":
    upload_to_hdfs()
    logger.info("Upload to HDFS completed successfully.")