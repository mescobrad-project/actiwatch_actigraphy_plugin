from mescobrad_edge.plugins.actiwatch_actigraphy_plugin.models.plugin import EmptyPlugin,\
      PluginActionResponse, PluginExchangeMetadata

class GenericPlugin(EmptyPlugin):
    def execute_sql_on_trino(self, sql, conn):
        """Generic function to execute a SQL statement"""

        # Get a cursor from the connection object
        cur = conn.cursor()

        # Execute sql statement
        cur.execute(sql)

        # Get the results from the cluster
        rows = cur.fetchall()

        # Return the results
        return rows

    def transform_input_data(self, data, source_name, workspace_id, MRN,
                             metadata_file_name):
        """Transform input data into table suitable for creating query"""

        data = data.reset_index()

        if MRN is not None:
            data["MRN"] = MRN

        if metadata_file_name is not None:
            data["metadata_file_name"] = metadata_file_name

        # Add rowid column representing id of the row in the file
        data["rowid"] = data.index + 1

        # Insert source column representing name of the source file
        data.insert(0, "source", source_name)

        # Transform table into table with 5 columns:
        # source, rowid, variable_name, variable_value, workspace_id
        data = data.melt(id_vars=["source","rowid"])
        data = data.sort_values(by=['rowid'])

        # As a variable values type string is expected
        data = data.astype({"value":"str"})

        # Add workspace id into workspace column of the table
        data.insert(4, "workspace_id", workspace_id)

        return data

    def upload_data_on_trino(self, schema_name, table_name, data, conn):
        """Create sql statement for inserting data and update
        the table with data"""

        print(data.shape[0], "rows to insert ...")

        batch_size = 5000

        # Iterate through pandas dataframe to extract each row values
        for start in range(0, data.shape[0], batch_size):
            end = start + batch_size
            batch = data.iloc[start:end]

            # Create a batch insert statement
            data_list = []
            for row in batch.itertuples(index=False):
                data_list.append(str(tuple(row)))

            data_to_insert = ", ".join(data_list)

            # Insert data into the table
            sql_statement = "INSERT INTO iceberg.{schema_name}.{table_name} VALUES {data}"\
                .format(schema_name=schema_name, table_name=table_name, data=data_to_insert)
            self.execute_sql_on_trino(sql=sql_statement, conn=conn)

            percent_start = int((start / data.shape[0]) *100)
            percent_complete = int((end / data.shape[0]) * 100) if end < data.shape[0] else 100
            if percent_complete != percent_start:
                self.print_progress_bar(percent_complete)


    def print_progress_bar(self, percent):
        import sys
        from time import sleep
        bar_length = 50

        sys.stdout.write('\r')
        sys.stdout.write("Completed: [{:{}}] {:>3}%"
                         .format('='*int(percent/(100.0/bar_length)),
                                 bar_length, int(percent)))
        sys.stdout.flush()
        sleep(0.002)


    def download_file(self, file_path: str) -> None:
        import boto3
        from botocore.client import Config
        import os
        import time

        s3_local = boto3.resource('s3',
                                  endpoint_url=self.__OBJ_STORAGE_URL_LOCAL__,
                                  aws_access_key_id=self.__OBJ_STORAGE_ACCESS_ID_LOCAL__,
                                  aws_secret_access_key=self.__OBJ_STORAGE_ACCESS_SECRET_LOCAL__,
                                  config=Config(signature_version='s3v4'),
                                  region_name=self.__OBJ_STORAGE_REGION__)

        bucket_local = s3_local.Bucket(self.__OBJ_STORAGE_BUCKET_LOCAL__)

        # Existing non annonymized data in local MinIO bucket
        obj_personal_data = bucket_local.objects.filter(Prefix="actigraphy_data_tmp/",
                                                        Delimiter="/")

        # Files for anonymization
        files_to_anonymize = [obj.key for obj in obj_personal_data]

        # Download data which need to be anonymized
        for file_name in files_to_anonymize:
            ts = round(time.time()*1000)
            basename, extension = os.path.splitext(os.path.basename(file_name))
            path_download_file = f"{file_path}{basename}_{ts}{extension}"

            s3_local.Bucket(self.__OBJ_STORAGE_BUCKET_LOCAL__
                            ).download_file(file_name,
                                            path_download_file)

            # In order to rename the original file in bucket we need to delete it and
            # upload it again
            s3_local.Object(self.__OBJ_STORAGE_BUCKET_LOCAL__,
                            "actigraphy_data_tmp/"+os.path.basename(file_name)).delete()

    def update_filename_pid_mapping(self, obj_name, personal_id, s3_local):
        import csv
        import io

        folder = "file_pid/"
        filename = "filename_pid.csv"
        file_path = f"{folder}{filename}"

        bucket_local = s3_local.Bucket(self.__OBJ_STORAGE_BUCKET_LOCAL__)
        obj_files = bucket_local.objects.filter(Prefix=folder, Delimiter="/")

        if (len(list(obj_files))) > 0:
            existing_object = s3_local.Object(self.__OBJ_STORAGE_BUCKET_LOCAL__, file_path)
            existing_data = existing_object.get()["Body"].read().decode('utf-8')
            data_to_append = [obj_name, personal_id]
            existing_rows = list(csv.reader(io.StringIO(existing_data)))
            existing_rows.append(data_to_append)

            updated_data = io.StringIO()
            csv.writer(updated_data).writerows(existing_rows)
            s3_local.Bucket(self.__OBJ_STORAGE_BUCKET_LOCAL__).upload_fileobj(
                io.BytesIO(updated_data.getvalue().encode('utf-8')), file_path)
        else:
            key_values = ['filename', 'personal_id']
            file_data = [key_values, [obj_name, personal_id]]
            updated_data = io.StringIO()
            csv.writer(updated_data).writerows(file_data)
            s3_local.Bucket(self.__OBJ_STORAGE_BUCKET_LOCAL__).upload_fileobj(
                io.BytesIO(updated_data.getvalue().encode('utf-8')), file_path)

    def upload_data_local(self, path_to_file, personal_id):
        """Upload file local with inserted PID in the filename"""

        import boto3
        from botocore.client import Config
        import os

        basename = os.path.basename(path_to_file)
        file_name = f"actigraphy_files/{basename}"

        s3_local = boto3.resource('s3',
                                  endpoint_url=self.__OBJ_STORAGE_URL_LOCAL__,
                                  aws_access_key_id=self.__OBJ_STORAGE_ACCESS_ID_LOCAL__,
                                  aws_secret_access_key=self.__OBJ_STORAGE_ACCESS_SECRET_LOCAL__,
                                  config=Config(signature_version='s3v4'),
                                  region_name=self.__OBJ_STORAGE_REGION__)

        s3_local.Bucket(self.__OBJ_STORAGE_BUCKET_LOCAL__
                        ).upload_file(path_to_file, file_name)

        # Update key value file with mapping between filename nad patient id,
        # this file is stored in the local MinIO instance
        self.update_filename_pid_mapping(file_name, personal_id, s3_local)


    def remove_tmp_actigraphy_file(self):
        import boto3
        from botocore.client import Config

        s3_local = boto3.resource('s3',
                                  endpoint_url=self.__OBJ_STORAGE_URL_LOCAL__,
                                  aws_access_key_id=self.__OBJ_STORAGE_ACCESS_ID_LOCAL__,
                                  aws_secret_access_key=self.__OBJ_STORAGE_ACCESS_SECRET_LOCAL__,
                                  config=Config(signature_version='s3v4'),
                                  region_name=self.__OBJ_STORAGE_REGION__)

        # Empty the tmp folder if the file is not processed successfully
        objs = list(s3_local.Bucket(self.__OBJ_STORAGE_BUCKET_LOCAL__
                                    ).objects.filter(Prefix="actigraphy_data_tmp/",
                                                     Delimiter="/"))
        if len(list(objs))>0:
            for obj in objs:
                s3_local.Bucket(self.__OBJ_STORAGE_BUCKET_LOCAL__
                                ).objects.filter(Prefix=obj.key).delete()

    def extract_data(self, file, delimiter):
        """Extracted Epoch-by-Epoch Data from the Actiwatch data"""

        import pyActigraphy
        import pandas as pd
        import numpy as np

        extracted_data = []

        # Read the actiwatch file
        raw = pyActigraphy.io.read_raw_rpx(file, delimiter=delimiter, drop_na=False)

        # Extract datetime and 'Activity' column
        data = raw.data.to_frame()

        extracted_data.append(data)

        # Extract all available light channels
        # Possible channels:
        # - White Light,
        # - Red Light,
        # - Green Light,
        # - Blue Light

        if raw.light is not None:
            channels = raw.light.get_channel_list()
            light_channels = raw.light.get_channels()

            # Original data for 'White Light' are during extraction transformed with
            # log10(x+1), perform inverse function to extract and preserve the original data

            if 'White Light' in channels:
                light_channels['White Light'] = \
                    light_channels['White Light'].apply(lambda x: np.power(10, x) - 1)
            extracted_data.append(round(light_channels, 2))

        # Extract 'Sleep/Wake'
        sleep_wake = raw.sleep_wake
        if sleep_wake is not None:
            extracted_data.append(sleep_wake.to_frame())

        # Extract 'Interval Status'
        interval_status = raw.interval_status
        if interval_status is not None:
            extracted_data.append(interval_status.to_frame())

        data_to_upload = [df for df in extracted_data if df is not None]

        final_actigraphy_data = pd.concat(data_to_upload, axis=1)

        return final_actigraphy_data

    def extract_rpx_header_info(self, fname):
        """Extract file header and data header"""

        header = []

        with open(fname, mode='rb') as file:
            data = file.readlines()
        for header_offset, line in enumerate(data, 1):
            if 'Epoch-by-Epoch Data' in line.decode('utf-8'):
                break
            else:
                header.append(line.decode('utf-8'))

        return header

    def extract_identity(self, header, identity='Identity', delimiter=','):
        import re

        name = ""
        for line in header:
            if identity in line:
                name = re.sub(r'[^\w\s]', '', line.split(delimiter)[1]).strip()
                break
        return name

    def extract_full_name(self, header, identity='Full Name', delimiter=','):
        import re

        name = ""
        for line in header:
            if identity in line:
                name = re.sub(r'[^\w\s]', '', line.split(delimiter)[1]).strip()
                break
        return name

    def extract_date_of_birth(self, header, date_of_birth='Date of Birth', delimiter=","):
        import re
        import pandas as pd

        birth_date = ""
        for line in header:
            if date_of_birth in line:
                date_of_birth = re.sub(r'[^\d./]+', '', line.split(delimiter)[1])
                break
        if birth_date:
            birth_date = (pd.to_datetime(date_of_birth, dayfirst=True)).strftime("%d-%m-%Y")
        return birth_date

    def generate_personal_id(self, personal_data):
        """Based on the identity, full_name and date of birth."""

        import hashlib

        personal_id = "".join(str(data) for data in personal_data)

        # Remove all whitespaces characters
        personal_id = "".join(personal_id.split())

        # Generate ID
        id = hashlib.sha256(bytes(personal_id, "utf-8")).hexdigest()
        return id

    def generate_subject_personal_id(self, file, delimiter):
        """
        Extract subject properties and based on extracted name, date of birth and
        identity generate a unique ID.
        """
        # Extract header
        header = self.extract_rpx_header_info(file)

        # From header extract full name
        full_name = self.extract_full_name(header=header, delimiter=delimiter)

        # From header extract identity
        identity = self.extract_identity(header=header, delimiter=delimiter)

        # From header extract date of birth
        date_of_birth = self.extract_date_of_birth(header=header, delimiter=delimiter)

        # Generate personal id
        if full_name == "" or date_of_birth == "" or identity == "":
            personal_data = []
        else:
            personal_data = [full_name, date_of_birth, identity]

        pid = self.generate_personal_id(personal_data)
        return pid

    def upload_metadata_file(self, metadata_file_name, metadata_content):
        """Upload metadata files to support FAIR templates"""
        from io import BytesIO
        import boto3
        from botocore.client import Config

        s3_data_lake =  boto3.resource('s3',
                                       endpoint_url=self.__OBJ_STORAGE_URL__,
                                       aws_access_key_id=self.__OBJ_STORAGE_ACCESS_ID__,
                                       aws_secret_access_key=self.__OBJ_STORAGE_ACCESS_SECRET__,
                                       config=Config(signature_version='s3v4'),
                                       region_name=self.__OBJ_STORAGE_REGION__)

        obj_name_metadata = f"metadata_files/{metadata_file_name}"
        s3_data_lake.Bucket(self.__OBJ_STORAGE_BUCKET__).upload_fileobj(
            BytesIO(metadata_content), obj_name_metadata,
            ExtraArgs={'ContentType': "text/json"})

    def action(self, input_meta: PluginExchangeMetadata = None) -> PluginActionResponse:
        """
        Extract epoch by epoch data from actiwatch actigraphy files.
        Upload extracted data into the trino table.
        """
        import os
        import shutil
        import pandas as pd
        import csv

        from trino.dbapi import connect
        from trino.auth import BasicAuthentication

        # Initialize the connection with Trino
        conn = connect(
            host=self.__TRINO_HOST__,
            port=self.__TRINO_PORT__,
            http_scheme="https",
            auth=BasicAuthentication(self.__TRINO_USER__, self.__TRINO_PASSWORD__),
            max_attempts=1,
            request_timeout=600
        )

        # Get the schema name, schema in Trino is an equivalent to a bucket in MinIO
        # Trino doesn't allow to have "-" in schema name so it needs to be replaced
        # with "_"
        schema_name = self.__OBJ_STORAGE_BUCKET__.replace("-", "_")

        # Get the table name
        table_name = self.__OBJ_STORAGE_TABLE__.replace("-", "_")

        path_to_data = "mescobrad_edge/plugins/actiwatch_actigraphy_plugin/actigraphy_files/"

        # create temporary folder for storing downloaded files
        os.makedirs(path_to_data, exist_ok=True)

        # Download data to process
        self.download_file(path_to_data)

        try:
            for file in os.listdir(path_to_data):
                path_to_file = os.path.join(path_to_data, file)
                if os.path.isfile(path_to_file):
                    # Get the delimiter type
                    with open(path_to_file, mode='r') as file:
                        data = file.read(100)

                    sniffer = csv.Sniffer()
                    delimiter = sniffer.sniff(data).delimiter

                    # Extracting subject properties to create a PID
                    print("Extracting subject properties ...")
                    data_info = input_meta.data_info
                    if all(param is not None for param in [data_info['name'],
                                                           data_info['surname'],
                                                           data_info['date_of_birth'],
                                                           data_info['unique_id']]):

                        # Make unified dates, so that different formats of date doesn't
                        # change the final id
                        data_info["date_of_birth"] = pd.to_datetime(
                            data_info["date_of_birth"], dayfirst=True)

                        data_info["date_of_birth"] = data_info["date_of_birth"].strftime(
                            "%d-%m-%Y")

                        # ID is created from the data: name, surname, date of birth and
                        # national unique ID
                        personal_data = [data_info['name'], data_info['surname'],
                                         data_info['date_of_birth'],
                                         data_info['unique_id']]

                        personal_id = self.generate_personal_id(personal_data)
                    else:
                        personal_id = self.generate_subject_personal_id(path_to_file,
                                                                        delimiter)

                    # Extract data from the uploaded actigraphy
                    print("Extracting data ...")
                    actigraphy_data = self.extract_data(path_to_file, delimiter)

                    # Insert personal id in the extracted data
                    actigraphy_data.insert(0, "PID", personal_id)

                    # Source name of the original edf file
                    source_name = os.path.basename(path_to_file)

                    # Metadata file name
                    if input_meta.data_info["metadata_json_file"] is not None:
                        metadata_file_name = os.path.splitext(source_name)[0] + ".json"
                    else:
                        metadata_file_name = None

                    # Transform data in suitable form for updating trino table
                    data_transformed = self.transform_input_data(actigraphy_data,
                                                                 source_name,
                                                                 input_meta.data_info["workspace_id"],
                                                                 input_meta.data_info["MRN"],
                                                                 metadata_file_name)

                    print("Uploading data ...")
                    self.upload_data_local(path_to_file, personal_id)
                    self.upload_data_on_trino(schema_name, table_name, data_transformed,
                                              conn)
                    # Upload metadata file also
                    if input_meta.data_info["metadata_json_file"] is not None:
                        self.upload_metadata_file(metadata_file_name,
                                                  input_meta.data_info["metadata_json_file"])

            print("Processing of the actigraphy file is finished.")

        except Exception as e:
            print("Actigraphy processing failed with error: " + str(e))

        finally:
            # Remove folder with downloaded files
            shutil.rmtree(os.path.split(path_to_file)[0])

            # Remove file in local bucket
            self.remove_tmp_actigraphy_file()

        return PluginActionResponse()
