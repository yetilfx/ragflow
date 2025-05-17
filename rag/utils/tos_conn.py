#
#  Copyright 2025 The Paper-Digger Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import logging
import tos
from tos.exceptions import TosClientError,TosServerError
from tos import HttpMethodType
import time 
from io import BytesIO
from rag.utils import singleton
from rag import settings


@singleton
class RAGFlowTOS:
    def __init__(self):
        self.conn = None
        self.tos_config = settings.TOS
        self.access_key = self.tos_config.get('access_key', None)
        self.secret_key = self.tos_config.get('secret_key', None)
        self.endpoint_url = self.tos_config.get('endpoint_url', None)
        self.region = self.tos_config.get('region', None)
        self.bucket = self.tos_config.get('bucket', None)
        self.prefix_path = self.tos_config.get('prefix_path', "")
        self.__open__()

    @staticmethod
    def use_default_bucket(method):
        def wrapper(self, bucket, *args, **kwargs):
            # If there is a default bucket, use the default bucket
            actual_bucket = self.bucket if self.bucket else bucket
            return method(self, actual_bucket, *args, **kwargs)
        return wrapper
    
    @staticmethod
    def use_prefix_path(method):
        def wrapper(self, bucket, fnm, *args, **kwargs):
            # If the prefix path is set, use the prefix path
            fnm = f"{self.prefix_path}/{fnm}" if self.prefix_path else fnm
            return method(self, bucket, fnm, *args, **kwargs)
        return wrapper

    def __open__(self):
        try:
            if self.conn:
                self.__close__()
        except Exception:
            pass

        try:
            # Reference：https://www.volcengine.com/docs/6349/92786
            self.conn = tos.TosClientV2(
                ak=self.access_key,
                sk=self.secret_key,
                endpoint=self.endpoint_url,
                region=self.region,
                max_retry_count=0,
            )
        except TosClientError as e:
            logging.exception(f"TOS __open__:Fail to connect TOS at region {self.region} or endpoint {self.endpoint_url}")
            raise e
        except TosServerError as e:
            logging.exception(f"TOS __open__:Fail to connect TOS at region {self.region} or endpoint {self.endpoint_url}")

    def __close__(self):
        self.conn.close()
        del self.conn
        self.conn = None

    @use_default_bucket
    def bucket_exists(self, bucket):
        try:
            logging.debug(f"TOS bucket_exists:head_bucket bucketname {bucket}")
            self.conn.head_bucket(bucket=bucket)
            exists = True
        except TosClientError:
            logging.exception(f"TOS bucket_exists:head_bucket error {bucket}")
            exists = False
        except TosServerError:
            logging.exception(f"TOS bucket_exists:head_bucket error {bucket}")
            exists = False
        return exists

    def health(self):
        
        bucket = self.bucket
        fnm = "txtxtxtxt1"
        fnm, binary = f"{self.prefix_path}/{fnm}" if self.prefix_path else fnm, b"_t@@@1"
        if not self.bucket_exists(bucket):
            self.conn.create_bucket(bucket=bucket)
            logging.debug(f"create bucket {bucket} ********")

        r = self.conn.put_object(bucket=bucket, key=fnm,content=BytesIO(binary))
        return r

    def get_properties(self, bucket, key):
        return {}

    def list(self, bucket, dir, recursive=True):
        return []

    @use_prefix_path
    @use_default_bucket
    def put(self, bucket, fnm, binary):
        for _ in range(1):
            try:
                if not self.bucket_exists(bucket):
                    self.conn.create_bucket(bucket=bucket,acl=tos.ACLType.ACL_Private)
                    logging.info(f"create bucket {bucket} ********")
                r = self.conn.put_object(bucket=bucket, key=fnm,content=BytesIO(binary))

                return r
            except Exception:
                logging.exception(f"TOS put:Fail put {bucket}/{fnm}")
                self.__open__()
                time.sleep(1)

    @use_prefix_path
    @use_default_bucket
    def rm(self, bucket, fnm):
        try:
            self.conn.delete_object(bucket=bucket, key=fnm)
        except Exception:
            logging.exception(f"TOS rm:Fail rm {bucket}/{fnm}")

    @use_prefix_path
    @use_default_bucket
    def get(self, bucket, fnm):
        for _ in range(1):
            try:
                object_stream = self.conn.get_object(bucket=bucket, key=fnm)
                object_data = object_stream.read()
                return object_data
            except Exception as e:
                logging.exception(f"TOS get：fail get {bucket}/{fnm},e:{e}")
                self.__open__()
                time.sleep(1)
        return

    @use_prefix_path
    @use_default_bucket
    def obj_exist(self, bucket, fnm):
        try:
            out = self.conn.head_object(bucket=bucket, key=fnm)
            if out.status_code == 200:
                return True
        except TosServerError as e:
            logging.debug(f"TOS obj_exist：fail get {bucket}/{fnm},e:{e}")
            if e.status_code == 404:
                return False
        except Exception as e:
            logging.exception(f"TOS obj_exist：fail get {bucket}/{fnm},e:{e}")
            
        return False
        
   


    @use_prefix_path
    @use_default_bucket
    def get_presigned_url(self, bucket, fnm, expires):
        for _ in range(10):
            try:
                r = self.conn.pre_signed_url(http_method = HttpMethodType.Http_Method_Get,
                                                     bucket = bucket,key = fnm,
                                                     expires = expires)

                return r.signed_url
            except Exception:
                logging.exception(f"TOS get_presigned_url：fail get url {bucket}/{fnm}")
                self.__open__()
                time.sleep(1)
        return