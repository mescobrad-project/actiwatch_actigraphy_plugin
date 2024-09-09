from mescobrad_edge.plugins.actiwatch_actigraphy_plugin.models.plugin import \
    EmptyPlugin, PluginActionResponse, PluginExchangeMetadata

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

    def transform_input_data(self, data, source_name, workspace_id, pseudoMRN,
                             metadata_file_name, startdate_time, enddate_time):
        """Transform input data into table suitable for creating query"""

        data['startdate_time'] = startdate_time
        data['enddate_time'] = enddate_time


        data["pseudoMRN"] = pseudoMRN

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

        print(data.shape[0], "rows to insert into Trino table ...")

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
            sql_statement = "INSERT INTO iceberg.{schema_name}.{table_name} \
                VALUES {data}".format(schema_name=schema_name,
                                      table_name=table_name,
                                      data=data_to_insert)
            self.execute_sql_on_trino(sql=sql_statement, conn=conn)

    def download_file(self, file_path: str) -> None:
        import boto3
        from botocore.client import Config
        import os
        import time

        s3_local = boto3.resource('s3',
                                  endpoint_url=self.__OBJ_STORAGE_URL_LOCAL__,
                                  aws_access_key_id=\
                                    self.__OBJ_STORAGE_ACCESS_ID_LOCAL__,
                                  aws_secret_access_key=\
                                    self.__OBJ_STORAGE_ACCESS_SECRET_LOCAL__,
                                  config=Config(signature_version='s3v4'),
                                  region_name=self.__OBJ_STORAGE_REGION__)

        bucket_local = s3_local.Bucket(self.__OBJ_STORAGE_BUCKET_LOCAL__)

        # Existing non annonymized data in local MinIO bucket
        obj_personal_data = bucket_local.objects.filter(
            Prefix="actigraphy_data_tmp/", Delimiter="/")

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

            # In order to rename the original file in bucket we need to delete
            # it and upload it again
            s3_local.Object(self.__OBJ_STORAGE_BUCKET_LOCAL__,
                            "actigraphy_data_tmp/" + os.path.basename(
                                file_name)).delete()

    def update_filename_pid_mapping(self, obj_name, personal_id, pseudoMRN, mrn,
                                    s3_local):
        import csv
        import io

        folder = "file_pid/"
        filename = "filename_pid.csv"
        file_path = f"{folder}{filename}"

        bucket_local = s3_local.Bucket(self.__OBJ_STORAGE_BUCKET_LOCAL__)
        obj_files = bucket_local.objects.filter(Prefix=folder, Delimiter="/")

        if (len(list(obj_files))) > 0:
            existing_object = s3_local.Object(self.__OBJ_STORAGE_BUCKET_LOCAL__,
                                              file_path)
            existing_data = existing_object.get()["Body"].read().decode('utf-8')
            data_to_append = [obj_name, personal_id, pseudoMRN, mrn]
            existing_rows = list(csv.reader(io.StringIO(existing_data)))
            existing_rows.append(data_to_append)

            # Update column names
            column_names = ['filename', 'personal_id', 'pseudoMRN', 'MRN']
            if any(col_name not in existing_rows[0] for col_name in column_names):
                existing_rows[0] = column_names

            updated_data = io.StringIO()
            csv.writer(updated_data).writerows(existing_rows)
            s3_local.Bucket(self.__OBJ_STORAGE_BUCKET_LOCAL__).upload_fileobj(
                io.BytesIO(updated_data.getvalue().encode('utf-8')), file_path)
        else:
            key_values = ['filename', 'personal_id', 'pseudoMRN', 'MRN']
            file_data = [key_values, [obj_name, personal_id, pseudoMRN, mrn]]
            updated_data = io.StringIO()
            csv.writer(updated_data).writerows(file_data)
            s3_local.Bucket(self.__OBJ_STORAGE_BUCKET_LOCAL__).upload_fileobj(
                io.BytesIO(updated_data.getvalue().encode('utf-8')), file_path)

    def upload_data_local(self, path_to_file, personal_id, pseudoMRN, mrn):
        """Upload file local with inserted PID in the filename"""

        import boto3
        from botocore.client import Config
        import os

        basename = os.path.basename(path_to_file)
        file_name = f"actigraphy_files/{basename}"

        s3_local = boto3.resource('s3',
                                  endpoint_url=self.__OBJ_STORAGE_URL_LOCAL__,
                                  aws_access_key_id=\
                                    self.__OBJ_STORAGE_ACCESS_ID_LOCAL__,
                                  aws_secret_access_key=\
                                    self.__OBJ_STORAGE_ACCESS_SECRET_LOCAL__,
                                  config=Config(signature_version='s3v4'),
                                  region_name=self.__OBJ_STORAGE_REGION__)

        s3_local.Bucket(self.__OBJ_STORAGE_BUCKET_LOCAL__
                        ).upload_file(path_to_file, file_name)

        # Update key value file with mapping between filename nad patient id,
        # this file is stored in the local MinIO instance
        self.update_filename_pid_mapping(file_name, personal_id, pseudoMRN, mrn,
                                         s3_local)


    def remove_tmp_actigraphy_file(self):
        import boto3
        from botocore.client import Config

        s3_local = boto3.resource('s3',
                                  endpoint_url=self.__OBJ_STORAGE_URL_LOCAL__,
                                  aws_access_key_id=\
                                    self.__OBJ_STORAGE_ACCESS_ID_LOCAL__,
                                  aws_secret_access_key=\
                                    self.__OBJ_STORAGE_ACCESS_SECRET_LOCAL__,
                                  config=Config(signature_version='s3v4'),
                                  region_name=self.__OBJ_STORAGE_REGION__)

        # Empty the tmp folder if the file is not processed successfully
        objs = list(s3_local.Bucket(self.__OBJ_STORAGE_BUCKET_LOCAL__).\
                    objects.filter(Prefix="actigraphy_data_tmp/",
                                   Delimiter="/"))
        if len(list(objs))>0:
            for obj in objs:
                s3_local.Bucket(self.__OBJ_STORAGE_BUCKET_LOCAL__
                                ).objects.filter(Prefix=obj.key).delete()


    def generate_personal_id(self, personal_data):
        """Based on the identity, full_name and date of birth."""

        import hashlib

        personal_id = "".join(str(data) for data in personal_data)

        # Remove all whitespaces characters
        personal_id = "".join(personal_id.split())

        # Generate ID
        id = hashlib.sha256(bytes(personal_id, "utf-8")).hexdigest()
        return id

    def upload_file_on_cloud(self, file_name, file_content, type_of_file):
        """Upload metadata files to support FAIR templates"""
        from io import BytesIO
        import boto3
        from botocore.client import Config

        s3_data_lake =  boto3.resource('s3',
                                       endpoint_url=self.__OBJ_STORAGE_URL__,
                                       aws_access_key_id=\
                                        self.__OBJ_STORAGE_ACCESS_ID__,
                                       aws_secret_access_key=\
                                        self.__OBJ_STORAGE_ACCESS_SECRET__,
                                       config=Config(signature_version='s3v4'),
                                       region_name=self.__OBJ_STORAGE_REGION__)


        s3_data_lake.Bucket(self.__OBJ_STORAGE_BUCKET__).upload_fileobj(
            BytesIO(file_content), file_name,
            ExtraArgs={'ContentType': type_of_file})


    def remove_subject_properties_info(self, path_to_file, delimiter):
        """Remove all data from Subject properties section in the actiwatch
        actigraphy files"""

        # Read the file
        with open(path_to_file, mode='rb') as file:
            data = file.readlines()

        subject_properties = False
        for header_offset, line in enumerate(data, 1):
            if 'Subject Properties' in line.decode('utf-8'):
                subject_properties = True
                break

        # Read file until the next blank line
        # First, skip blank line after section title

        if subject_properties:
            for data_offset, line in enumerate(data[header_offset+1:]):
                line_clean = line.replace(b'\r\r\n', b'\r\n')
                if line_clean == b'\r\n':
                    break
                else:
                    line_clean = line_clean.decode('utf-8')
                    split_data = [item for item in line_clean.split(delimiter)]
                    key_to_replace = split_data[0]
                    new_value = None
                    modified_string = \
                        f'{key_to_replace}{delimiter}{new_value}\r\n'
                    data[header_offset+1+data_offset] = \
                        modified_string.encode('utf-8')

        return data

    def remove_personal_data_from_header_info(self, data, delimiter):
        """In case that there is personal information outside of the Subject
        properties section remove those information also"""

        key_words = ['name', 'identity', 'initials', 'street', 'address',
                     'city', 'state', 'zip', 'country', 'phone', 'gender',
                     'birth','age', 'zone', 'latitude', 'longitude', 'altitude',
                     'geolocation', 'location']

        for header_offset, line in enumerate(data):
            if 'Epoch-by-Epoch Data' in line.decode('utf-8'):
                break
            else:
                line = line.decode('utf-8')
                line_to_check = line.lower()
                for keyword in key_words:
                    if keyword in line_to_check:
                        split_data = [item for item in line.split(delimiter)]
                        key_to_replace = split_data[0]
                        new_value = None
                        modified_string = \
                            f'{key_to_replace}{delimiter}{new_value}\r\n'
                        data[header_offset] = modified_string.encode('utf-8')
                        break

        return data

    def extract_metadata_information(self, data, delimiter):
        """Extract Start date, Start Time, End date and End time from actigraphy
        file."""
        import re
        import pandas as pd

        start_time = []
        end_time = []
        for header_offset, line in enumerate(data):
            if 'Epoch-by-Epoch Data' in line.decode('utf-8'):
                break
            else:
                line = line.decode('utf-8')
                line_to_check = line.lower()
                if 'Data Collection Start Date'.lower() in line_to_check:
                    start_time.append(re.sub(r'[^\d./]+', '',
                                             line.split(delimiter)[1]))
                elif 'Data Collection Start Time'.lower() in line_to_check:
                    start_time.append(re.sub(r'[^\d.:AMP]+', '',
                                             line.split(delimiter)[1]))
                elif 'Data Collection End Date'.lower() in line_to_check:
                    end_time.append(re.sub(r'[^\d./]+', '',
                                           line.split(delimiter)[1]))
                elif 'Data Collection End Time'.lower() in line_to_check:
                    end_time.append(re.sub(r'[^\d.:AMP]+', '',
                                           line.split(delimiter)[1]))

        if start_time:
            final_start_time = pd.to_datetime(' '.join(start_time),
                                              dayfirst=True) # following pyactigraphy implementation
        else:
            final_start_time = None

        if end_time:
            final_end_time = pd.to_datetime(' '.join(end_time), dayfirst=True) # following pyactigraphy implementation
        else:
            final_end_time = None


        return final_start_time, final_end_time

    def calculate_pseudoMRN(self, mrn, workspace_id):
        import hashlib

        if mrn is None:
            pseudoMRN = None
        else:
            personalMRN = [mrn, workspace_id]
            personal_mrn = "".join(str(data) for data in personalMRN)

            # Generate ID
            pseudoMRN = hashlib.sha256(bytes(personal_mrn, "utf-8")).hexdigest()

        return pseudoMRN

    def anonymize_actigraphy_file(self, path_to_file, delimiter):
        """Remove personal information from the uploaded actiwatch actigraphy
        file"""

        data = self.remove_subject_properties_info(path_to_file, delimiter)
        data = self.remove_personal_data_from_header_info(data, delimiter)

        return data

    def action(self, input_meta: PluginExchangeMetadata = None) -> \
        PluginActionResponse:
        """
        Extract epoch by epoch data from actiwatch actigraphy files.
        Upload extracted data into the trino table.
        """
        import os
        import shutil
        import pandas as pd
        import csv
        import pyActigraphy

        from trino.dbapi import connect
        from trino.auth import BasicAuthentication

        # Initialize the connection with Trino
        conn = connect(
            host=self.__TRINO_HOST__,
            port=self.__TRINO_PORT__,
            http_scheme="https",
            auth=BasicAuthentication(self.__TRINO_USER__,
                                     self.__TRINO_PASSWORD__),
            max_attempts=1,
            request_timeout=600
        )

        # Get the schema name, schema in Trino is an equivalent to a bucket in
        # MinIO Trino doesn't allow to have "-" in schema name so it needs to be
        # replaced with "_"
        schema_name = self.__OBJ_STORAGE_BUCKET__.replace("-", "_")

        # Get the table name
        table_name = self.__OBJ_STORAGE_TABLE__.replace("-", "_")

        path_to_data = \
            "mescobrad_edge/plugins/actiwatch_actigraphy_plugin/actigraphy_files/"

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
                        data = file.read()

                    sniffer = csv.Sniffer()
                    delimiter = sniffer.sniff(data).delimiter

                    # Check if the file is compatible with pyActigraphy
                    raw = pyActigraphy.io.read_raw_rpx(path_to_file,
                                                       delimiter=delimiter,
                                                       drop_na=False)

                    # Extracting subject properties to create a PID
                    print("Extracting subject properties ...")
                    data_info = input_meta.data_info
                    if all(param is not None for param in [data_info['name'],
                                                           data_info['surname'],
                                                           data_info['date_of_birth'],
                                                           data_info['unique_id']]):

                        # Make unified dates, so that different formats of date
                        # doesn't change the final id
                        data_info["date_of_birth"] = pd.to_datetime(
                            data_info["date_of_birth"], dayfirst=True)

                        data_info["date_of_birth"] =\
                            data_info["date_of_birth"].strftime("%d-%m-%Y")

                        # ID is created from the data: name, surname, date of
                        # birth and national unique ID
                        personal_data = [data_info['name'],
                                         data_info['surname'],
                                         data_info['date_of_birth'],
                                         data_info['unique_id']]

                        personal_id = self.generate_personal_id(personal_data)
                    else:
                        personal_data = []
                        personal_id = self.generate_personal_id(personal_data)

                    # Extract data from the uploaded actigraphy
                    print("Anonymization of data ...")
                    actigraphy_data = self.anonymize_actigraphy_file(
                        path_to_file, delimiter)

                    startdate_time, enddate_time = \
                        self.extract_metadata_information(actigraphy_data,
                                                          delimiter)

                    # Insert personal id in the extracted data
                    trino_metadata = {"PID": [personal_id]}
                    trino_metadata_df = pd.DataFrame(data=trino_metadata)

                    # Source name of the original edf file
                    source_name = os.path.basename(path_to_file)

                    # Metadata file name
                    if input_meta.data_info["metadata_json_file"] is not None:
                        metadata_file_name = \
                            os.path.splitext(source_name)[0] + ".json"
                    else:
                        metadata_file_name = None

                    pseudoMRN = self.calculate_pseudoMRN(
                        input_meta.data_info["MRN"],
                        input_meta.data_info["workspace_id"])

                    # Transform data in suitable form for updating trino table
                    data_transformed = \
                        self.transform_input_data(trino_metadata_df,
                                                  source_name,
                                                  input_meta.data_info["workspace_id"],
                                                  pseudoMRN,
                                                  metadata_file_name,
                                                  startdate_time,
                                                  enddate_time)

                    print("Uploading data ...")
                    self.upload_data_local(path_to_file, personal_id, pseudoMRN,
                                           input_meta.data_info["MRN"])
                    self.upload_data_on_trino(schema_name, table_name,
                                              data_transformed, conn)
                    obj_file_name_on_cloud = f"actigraphy_files/{source_name}"
                    self.upload_file_on_cloud(obj_file_name_on_cloud,
                                              b''.join(actigraphy_data),
                                              "text/csv")
                    # Upload metadata file also
                    if input_meta.data_info["metadata_json_file"] is not None:
                        obj_name_metadata = \
                            f"metadata_files/{metadata_file_name}"
                        self.upload_file_on_cloud(obj_name_metadata,
                                                  input_meta.data_info["metadata_json_file"],
                                                  "text/json")

            print("Processing of the actigraphy file is finished.")

        except Exception as e:
            print("Actigraphy processing failed with error: " + str(e))

        finally:
            # Remove folder with downloaded files
            shutil.rmtree(os.path.split(path_to_file)[0])

            # Remove file in local bucket
            self.remove_tmp_actigraphy_file()

        return PluginActionResponse()
