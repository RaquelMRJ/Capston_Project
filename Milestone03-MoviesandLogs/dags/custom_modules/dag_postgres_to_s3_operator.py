
"""
Transfer data from Postgres into a S3 bucket.
"""
import os
import tempfile
from typing import Optional, Union

import numpy as np
import pandas as pd

from airflow.models import BaseOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults


class PostgresToS3Operator(BaseOperator):
    

    @apply_defaults
    def __init__(
            self,
            query,
            s3_bucket,
            s3_key,
            postgres_conn_id= 'postgres_default',
            aws_conn_id = 'aws_default',
            verify = None,
            pd_csv_kwargs= None,
            index = False,
            *args, **kwargs):
        super(PostgresToS3Operator, self).__init__(*args, **kwargs)
        #self.schema = schema
        self.query = query
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.postgres_conn_id = postgres_conn_id
        self.aws_conn_id = aws_conn_id
        self.verify = verify
        self.pd_csv_kwargs = pd_csv_kwargs

        if not self.pd_csv_kwargs:
            self.pd_csv_kwargs = {}

        if "index" not in self.pd_csv_kwargs:
            self.pd_csv_kwargs["index"] = index

    def _fix_int_dtypes(self, df):
        """
        Mutate DataFrame to set dtypes for int columns containing NaN values."
        """
        for col in df:
            if "float" in df[col].dtype.name and df[col].hasnans:
                # inspect values to determine if dtype of non-null values is int or float
                notna_series = df[col].dropna().values
                if np.isclose(notna_series, notna_series.astype(int)).all():
                    # set to dtype that retains integers and supports NaNs
                    df[col] = np.where(df[col].isnull(), None, df[col]).astype(pd.Int64Dtype)

    def execute(self, context):
        Postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        s3_conn = S3Hook(aws_conn_id=self.aws_conn_id, verify=self.verify)
        data_df = Postgres_hook.get_pandas_df(self.query)
        self.log.info("Data from Postgres obtained")

        self._fix_int_dtypes(data_df)
        with tempfile.NamedTemporaryFile(mode='r+', suffix='.csv') as tmp_csv:
            tmp_csv.file.write(data_df.to_csv(**self.pd_csv_kwargs))
            tmp_csv.file.seek(0)
            s3_conn.load_file(filename=tmp_csv.name,
                              key=self.s3_key,
                              bucket_name=self.s3_bucket)

        if s3_conn.check_for_key(self.s3_key, bucket_name=self.s3_bucket):
            file_location = os.path.join(self.s3_bucket, self.s3_key)
            self.log.info("File saved correctly in %s", file_location)