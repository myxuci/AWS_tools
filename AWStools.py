import io
import os
import cv2
import sys
import copy
import json
import time
import boto3
import email
import string
import logging
import smtplib
import sagemaker
import numpy as np
import pandas as pd
from base64 import *
from contextlib import *
from tqdm import tqdm
from datetime import datetime
import matplotlib.pyplot as plt
from matplotlib import patches

### For email notification.
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate


class AWS:

    def get_current_time(timestamp_format=None):
        '''
        Get current local time in user-defined format.
        :param timestamp_format: Output format of timestamp. Type: string.
        :return: timestamp. Type: string.
        '''
        if timestamp_format is not None:
            assert isinstance(timestamp_format, str), TypeError('Timestamp_format can only be strings.')
            _timestamp_format = timestamp_format
        else:
            _timestamp_format = '%Y-%m-%d-%H-%M-%S'

        try:
            return datetime.now().strftime(_timestamp_format)
        except Exception as e:
            print('Failed in getting current time. Error: {}'.format(e))
            raise

class aws_s3(AWS):
    def __init__(self, bucket=None, prefix=None, ACCESS_KEY=None, SECRET_KEY=None, SESSION_TOKEN=None):
        self.bucket = bucket
        self.prefix = prefix

        if (ACCESS_KEY != None) & (SECRET_KEY != None):
            self.__create_s3_client_with_users_key(ACCESS_KEY=ACCESS_KEY, SECRET_KEY=SECRET_KEY, SESSION_TOKEN=SESSION_TOKEN)
            self.__create_s3_resource()
        else:
            self.__create_s3_client()
            self.__create_s3_resource()

    def __create_s3_client(self):
        self.__s3_client = boto3.client(
            's3'
        )

    def __create_s3_resource(self):
        self.__s3_resource = boto3.resource(
            's3'
        )

    def __create_s3_client_with_users_key(self, ACCESS_KEY, SECRET_KEY, SESSION_TOKEN):
        self.__s3_client = boto3.client(
            's3',
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            aws_session_token=SESSION_TOKEN
        )

    def __get_data_generator(self, filename, chunksize):
        '''
        For batch-reading data from s3.
        :return:
        '''
        try:
            tmp_key = os.path.join(self.prefix, filename).strip()
            obj = self.__s3_client.get_object(Bucket=self.bucket, Key=tmp_key)
            df_generator = pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='iso-8859-1', chunksize=chunksize, \
                                       low_memory=False)
            print('Successfully load data from S3.')
            return df_generator
        except Exception as e:
            print('Cannot load data from S3. Reason:\n{}\n'.format(e))
            raise TypeError('Please double check the original data source on AWS S3.')

    def __echo_objects(self, bucket=None, prefix=None):
        if (bucket == None) | (prefix == None):
            bucket = self.bucket
            prefix = self.prefix
        elif (bucket != None) & (prefix != None):
            bucket = bucket
            prefix = prefix

        try:
            response = self.__s3_client.list_objects(Bucket=bucket, Prefix=prefix)
            return response
        except Exception as e:
            print('Failed in getting obj list from {}'.format(bucket+'/'+prefix))

    def list_obj(self, bucket=None, prefix=None):
        response = self.__echo_objects(bucket=bucket, prefix=prefix)
        obj_list = [ele['Key'] for ele in response['Contents']]
        return obj_list

    def delete_obj(self, bucket, prefix, s3_obj_name):
        try:
            print('Try deleting the S3 obj: {}'.format(bucket + '/' + prefix + '/' + s3_obj_name))
            key = prefix + '/' + s3_obj_name
            response = self.__s3_client.delete_objects(
                Bucket=bucket,
                Delete={
                    'Objects':[
                        {
                            'Key': key
                        },
                    ]
                }
            )
            print('The S3 obj has been deleted.')
            return response
        except Exception as e:
            print('Failed in deleting the s3 obj. Reason:\n{}\n'.format(e))


    def read_s3_csv(self, filename=None, chunksize=50000):
        '''
        Reading csv file from s3.
        :param file_name:
        :return:
        '''
        try:
            int(chunksize)
        except Exception as e:
            print('Failed in reading csv file from S3. Reason:\n{}\n'.format(e))
            raise TypeError('For the chunksize, the data type has to be numeric.')

        data_generator = self.__get_data_generator(filename=filename, chunksize=int(chunksize))
        output_df = pd.DataFrame()
        _chunk_count = 1
        tic = time.time()
        while True:
            try:
                output_df = pd.concat([output_df, next(data_generator)], axis=0)
                print('Working on data chunk {}... (chunk size = {})'.format(_chunk_count, chunksize))
                _chunk_count += 1
            except:
                print('Complete. Time cost: {} seconds.'.format(int(time.time()-tic)))
                return output_df

    def read_s3_json(self, filename):
        key = self.prefix + '/' + filename
        try:
            content_obj = self.__s3_resource.Object(self.bucket, key)
            file_content = content_obj.get()['Body'].read().decode('utf-8')
            json_obj_as_dict = json.loads(file_content)
            return json_obj_as_dict
        except Exception as e:
            print('Cannot read s3 json file. Reason:\n{}\n'.format(e))

    def save_dataframe_to_s3(self, dataframe, s3_filename, bucket=None, prefix=None, index=False, header=True, \
                             chunksize=50000):
        if (bucket == None) | (prefix == None):
            bucket = self.bucket
            prefix = self.prefix
        elif (bucket != None) & (prefix != None):
            bucket = bucket
            prefix = prefix

        try:
            fobj_buffer = io.StringIO()
            dataframe.to_csv(fobj_buffer, index=index, header=header, chunksize=chunksize)
            print('Saving file obj to S3...')
            key = prefix + '/' + s3_filename
            self.__s3_resource.Object(bucket, key).put(Body=fobj_buffer.getvalue())
            print('Complete.')
        except Exception as e:
            print('Failed in saving file obj to s3. Reason:\n{}\n'.format(e))
            raise TypeError

    def upload_local_file_to_s3(self, local_filename, s3_filename, s3_bucket, s3_prefix):
        s3_key = s3_prefix + '/' + s3_filename
        try:
            print('Uploading local file of {0} to S3: {1}/{2}/{3}'.format(local_filename, s3_bucket, s3_prefix, s3_filename))
            self.__s3_resource.meta.client.upload_file(Filename=local_filename, Bucket=s3_bucket, Key=s3_key)
            print('File has been successfully uploaded.')
        except Exception as e:
            print('Failed in uploading file to S3. Reason:\n{}\n'.format(e))
            raise TypeError


    def df_to_redshift(self, dataframe, table_name, schema, redshift_keyfile, if_exists='replace', dtype=None):
        # Based on AWS Sagemaker sample notebook.
        print('Writing to Redshift...')

        try:
            print('Fetch encrypted credentials to Redshift from AWS s3...')
            redshift_credentials = self.read_s3_json(redshift_keyfile)
            print('Fetched credentials.')
            tic = time.time()
            print('Start writing data to Redshift...')
            connection_str = 'postgresql+psycopg2://' + \
                             redshift_credentials['user'] + ':' + \
                             redshift_credentials['password'] + '@' + \
                             redshift_credentials['host'] + ':' + \
                             redshift_credentials['port'] + '/' + \
                             redshift_credentials['dbname'];

            dataframe.to_sql(name=table_name, con=connection_str,
                             schema=schema, if_exists=if_exists, chunksize=None, index=False)
            print("Complete. Time cost: {} seconds.".format(np.round(time.time()-tic, 2)))
        except Exception as e:
            print('Cannot write dataframe to Redshift. Reason:\n{}\n'.format(e))

    def download_file(self, bucket, prefix, filename_on_s3, filename_local):

        if os.path.exists(filename_local):
            raise FileExistsError('Local file already exists. Please assign a new file name to avoid overwriting by mistake.')

        try:
            _tic =  time.time()
            _key_on_s3 = prefix + '/' + filename_on_s3
            self.__s3_resource.Bucket(bucket).download_file(_key_on_s3, filename_local)
            print('Complete downloading file from S3. Time cost: {}'.format(int(time.time()-_tic)))
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("The object does not exist.")
                raise
            else:
                raise

    def debug(self):
        pass

class aws_ec2(AWS):

    def __init__(self, instances_ids=None):
        self.__create_ec2_client()
        self.__instances_ids = instances_ids
        self.__response = None

    def __create_ec2_client(self):
        self.ec2_client = boto3.client('ec2')

    def __get_waiter(self):
        self.waiter = self.ec2_client.get_waiter('instance_status_ok')

    def __wait_for_status_ok(self, instances_ids):
        self.__get_waiter()
        self.waiter.wait(InstanceIds=instances_ids)

    def list_instances_ids(self):
        return self.__instances_ids

    def update_instances_ids(self, new_instances_ids):
        self.__instances_ids = new_instances_ids

    def __make_ec2_list(self, instances_ids):
        '''
        Make a single instance ID into a list.
        :param instances_ids: string. EC2 instance ID.
        :return: [instances_ids]: list. List of provided EC2 instance ID.
        '''
        if not isinstance(instances_ids, list):
            return [instances_ids]
        else:
            return instances_ids

    def __echo_response(self, instances_ids=None):
        if instances_ids != None:
            if type(instances_ids) != list:
                try:
                    instances_ids = [instances_ids]
                except:
                    raise TypeError('Please double check your input EC2 instances IDs.')
            else:
                instances_ids = instances_ids
        else:
            if type(self.__instances_ids) != list:
                try:
                    instances_ids = [self.__instances_ids]
                except:
                    raise TypeError('Please double check your input EC2 instances IDs.')
            else:
                instances_ids = self.__instances_ids

        try:
            self.__response = self.ec2_client.describe_instance_status(
                InstanceIds=instances_ids
            )
            return self.__response
        except Exception as e:
            print('Failed in getting EC2 instance status. Reason:\n{}\n'.format(e))
            raise TypeError

    def get_ec2_status(self, instances_ids=None):
        status = dict()
        aws_response = iter(self.__echo_response(instances_ids=instances_ids)['InstanceStatuses'])
        while True:
            try:
                tmp = next(aws_response)
                status[tmp['InstanceId']] = tmp['InstanceState']['Name']
            except:
                return status

    def start_ec2_instances(self, instances_ids):
        if type(instances_ids) != list:
            try:
                instances_ids = [instances_ids]
            except:
                raise TypeError('Please check your input EC2 instances IDs to turn ON.')

        try:
            response = self.ec2_client.start_instances(
                InstanceIds=instances_ids
            )
            self.__wait_for_status_ok(instances_ids)
            return response
        except Exception as e:
            print('Failed in turning ON EC2 instances of {}. Reason:\n{}\n'.format(instances_ids, e))

    def stop_ec2_instances(self, instances_ids):
        if type(instances_ids) != list:
            try:
                instances_ids = [instances_ids]
            except:
                raise TypeError('Please check your input EC2 instances IDs to turn OFF.')

        try:
            response = self.ec2_client.stop_instances(
                InstanceIds=instances_ids
            )
            return response
        except Exception as e:
            print('Failed in turning OFF EC2 instnaces of {}. Reason:\n{}\n'.format(instances_ids, e))

    def __describe_instances(self, instances_ids):
        '''
        Describe EC2 instances.
        :param instances_ids:
        :return:
        '''
        try:
            _instances_ids = self.__make_ec2_list(instances_ids)
            _describe_resp = self.ec2_client.describe_instances(InstanceIds=_instances_ids)
            return _describe_resp
        except Exception as e:
            print('Failed in describing EC2 instance IDs of {0}. Reason: {1}'.format(_instances_ids, e))
            raise

    def get_public_IP(self, instances_ids):
        '''
        Get the public IP of EC2 instances.
        :param instances_ids: list. List of EC2 instances.
        :return: ec2_public_ip: dict.
        '''
        _instances_ids = self.__make_ec2_list(instances_ids)
        _ec2_public_ip = {}

        for _id in _instances_ids:
            try:
                _resp = self.__describe_instances(instances_ids=_id)
                _ec2_public_ip[_id] = _resp['Reservations'][0]['Instances'][0]['PublicIpAddress']
            except Exception as e:
                print('Failed in getting public IP of EC2 instance: {0}. Reason: {1}'.format(_id, e))
                _ec2_public_ip[_id] = None

        return _ec2_public_ip

    def debug(self):
        return self.__response

class aws_lambda(AWS):

    def __init__(self):
        self.__create_lambda_client()

    def __create_lambda_client(self):
        self.__lambda_client = boto3.client('lambda')

    def invoke_lambda_func(self, func_names, InvocationType='Event', Payload=b'bytes'):
        if type(func_names) != list:
            try:
                func_names = [func_names]
            except Exception as e:
                print('Error:\n{}\n'.format(e))
                raise TypeError

        self.__responses = dict()

        try:
            for name in func_names:
                response = self.__lambda_client.invoke(
                    FunctionName=name,
                    InvocationType=InvocationType,
                    LogType='Tail',
                    Payload=Payload
                )
                self.__responses[name] = response
            return self.__responses
        except Exception as e:
            print('Failed in invoking lambda function of {}. Reason:\n{}\n'.format(name, e))
            raise TypeError

    def create_func(self):
        pass

class aws_sagemaker(AWS):
    '''
    Tool box of AWS SageMaker.
    '''

    def __init__(self, s3_bucket, s3_prefix, ml_model_name, execution_role=None, region='us-east-1'):
        '''
        Tool box starter. For each tool box, it should only use one machine learning model at a time, in order to reduce
        usage complexity.
        :param s3_bucket: S3 bucket name. Used for storing trained model artifacts. Type: string.
        :param s3_prefix: S3 prefix name. Used for storing trained model artifacts. Type: string.
        :param ml_model_name: Machine learning model that will be used during this session. Type: string.
        :param execution_role: Execution role for AWS SageMaker. This parameter is mandatory for the operations of
               creating training job, hosing model artifact, creating endpoint etc. Type: string.
        :param region: AWS service region for calling SageMaker services. For more details, refer to AWS official documents,
               or use shell command 'aws configure' to see default values. Type: string.
        '''
        self.__check_input_model_name(ml_model_name)
        self.__create_sagemaker_client()
        self.__s3_bucket = s3_bucket
        self.__s3_prefix = s3_prefix
        self.__bucket_path = 'https://s3-{}.amazonaws.com/{}'.format(region, s3_bucket)
        self.__runtime_client = boto3.client('runtime.sagemaker')

        try:
            self.__region = boto3.Session().region_name
        except:
            self.__region = region

        try:
            self.__role = execution_role
        except:
            self.__role = sagemaker.get_execution_role()

        self.__tag = {'Creator': 'Xiaowei.Li', 'BillingGroup':'DataServices', 'BillingSubGroup':'DataServices-SageMaker',
                      'Project':'Machine Learning'}

    def get_execution_role_(self):
        return self.__role

    def __create_sagemaker_client(self):
        '''
        Create an AWS SageMaker client by using boto3.
        :return: Null.
        '''
        self.__sm_client = boto3.Session().client('sagemaker')

    def __check_input_model_name(self, model_name):
        '''
        Pre-check if the input model name is valid, due to limitation of pre-defined machine learning model types on AWS
        SageMaker. This function is still in beta version and it can only support XGBoost for now.
        :return: Null.
        '''
        if model_name in ['xgboost']:
            self.ml_model_name = model_name
        else:
            raise NameError('Please input a valid model name.')

    def __get_aws_model_containers(self):
        '''
        Retrive containers storing pre-defined AWS SageMaker predefined models. This is in beta version and only supports
        XGBoost for now.
        :return model_containers: Model containers address according to AWS service regions. Type: dict.
        '''
        self.__model_containers = {
            'xgboost': {
                'us-west-2': '433757028032.dkr.ecr.us-west-2.amazonaws.com/xgboost:latest',
                'us-east-1': '811284229777.dkr.ecr.us-east-1.amazonaws.com/xgboost:latest',
                'us-east-2': '825641698319.dkr.ecr.us-east-2.amazonaws.com/xgboost:latest',
                'eu-west-1': '685385470294.dkr.ecr.eu-west-1.amazonaws.com/xgboost:latest'
            }
        }

    def __get_hyperparameters(self, training_data_name, validation_data_name, max_runtime=36000, **kwargs):
        '''
        This function stores the hyperparameters used for training and deploying machine learning models on AWS sagemaker.
        It's still in beta version and only supports XGBoost model. The input parameters will be commonly used for all
        AWS pre-defined machine learning models.
        :param training_data_name: Name of training data stored in a S3 bucket. Type: string.
        :param validation_data_name: Name of validation data stored in a S3 bucket. Type: string.
        :param max_runtime: Set a timeout restriction, this is the maximum time a training job can run on SageMaker. Type: int.
        :return hyperparameters: Hyper-parameters depending on machine learning models. Type: dict.
        '''
        self.__get_aws_model_img()

        self.__hyper_params = {
            'xgboost': {
                "AlgorithmSpecification": {
                    "TrainingImage": self.__container, # Debug. Attn.
                    "TrainingInputMode": "File"
                },
                "RoleArn": self.__role,
                "OutputDataConfig": {
                    "S3OutputPath": self.__bucket_path + '/' + 'MachineLearning/DecreasedSales/sagemaker_models/xgboost' #self.__s3_prefix # Need to change this part.
                },
                "ResourceConfig": {
                    "InstanceCount": 1,
                    "InstanceType": "ml.m4.xlarge",
                    "VolumeSizeInGB": 5
                },
                "HyperParameters": {
                    "base_score":"0.8",
                    "colsample_bylevel":"1",
                    "colsample_bytree":"0.7",
                    "max_depth":"7",
                    "eval_metric":"auc",
                    "eta":"0.003",
                    "gamma":"1",
                    "min_child_weight":"3",
                    "num_round":"1000",
                    "silent":"1",
                    "subsample":"0.7",
                    "objective": "binary:logistic",
                },
                "StoppingCondition": {
                    "MaxRuntimeInSeconds": max_runtime
                },
                "InputDataConfig": [
                    {
                        "ChannelName": "train",
                        "DataSource": {
                            "S3DataSource": {
                                "S3DataType": "S3Prefix",
                                #"S3Uri": bucket_path + "/"+ prefix + '/' + 'DS_rule0_train_2016_all_2017_JanToSep_noheader.csv', # + '/train/',
                                "S3Uri": self.__bucket_path + '/' + self.__s3_prefix + '/' + training_data_name,
                                "S3DataDistributionType": "FullyReplicated"
                            }
                        },
                        "ContentType": "libsvm",
                        "CompressionType": "None"
                    },
                    {
                        "ChannelName": "validation",
                        "DataSource": {
                            "S3DataSource": {
                                "S3DataType": "S3Prefix",
                                "S3Uri": self.__bucket_path + '/'+ self.__s3_prefix + '/' + validation_data_name, # + '/validation/',
                                "S3DataDistributionType": "FullyReplicated"
                            }
                        },
                        "ContentType": "libsvm",
                        "CompressionType": "None"
                    }
                ]
            }
        }

    def __get_aws_model_img(self):
        '''
        Use AWS pre-defined model containers. This is in beta version adn only XGBoost is available for now.
        :return container: Container with selected machine learning models. Type: string.
        '''
        self.__get_aws_model_containers()

        print('Retrieving AWS sagemaker pre-defined machine learning model image.')
        print('Model name: {}'.format(self.ml_model_name))

        self.__container = self.__model_containers[self.ml_model_name][self.__region]

    def __set_training_job_params(self, job_name, training_data_name, validation_data_name, put_timestamp=True):
        '''
        Set parameters for creating a training job on AWS SageMaker.
        :param job_name: Job or project name, this will be created as or partially as training job names created on AWS
               SageMaker, depending on whether or not to put time stamp. Type: string.
        :param training_data_name: Training data stored on S3. Type: string.
        :param validation_data_name: Validation data stored on S3. Type: string.
        :param put_timestamp: Whether to put time stamp with training job. If True, will use the GMT time in the format
               of '%Y-%m-%d-%H-%M-%S'. Type: bool.
        :return training_job_params: Hyper-parameter for creating training jobs on AWS SageMaker. Type: dict.
        '''
        if put_timestamp == True:
            try:
                self.__training_job_name = str(job_name) + time.strftime("%Y-%m-%d-%H-%M-%S-GMT", time.gmtime())
            except Exception as e:
                print('Failed in creating a sagemaker training job name. Reason:\n{}\n'.format(e))
                raise NameError('Please check your input training job name.')
        else:
            self.__training_job_name = str(job_name)

        self.__get_hyperparameters(training_data_name=training_data_name, validation_data_name=validation_data_name,
                                   max_runtime=36000)

        training_job_params = copy.deepcopy(self.__hyper_params[self.ml_model_name])
        training_job_params['TrainingJobName'] = self.__training_job_name
        training_job_params['OutputDataConfig']['S3OutputPath'] = self.__bucket_path + "/" + self.__s3_prefix  # + "/xgboost-single"
        training_job_params['ResourceConfig']['InstanceCount'] = 1
        training_job_params['Tags'] = [{'Key':key, 'Value':self.__tag[key]} for key in self.__tag.keys()]

        return training_job_params

    def train(self, training_job_name, training_data_name, validation_data_name):
        '''
        This is a high level function and wrapper to get configuration and hyper-parameter to create a training job on AWS
        SageMaker and observe the training job status.
        :param training_job_name: Training job name to be created on AWS SageMaker, it will be forwarded to lower level
               functions. Type: string.
        :param training_data_name: Training data stored on S3. Type: string.
        :param validation_data_name: Validation data stored on S3. Type: string.
        :return: Null.
        '''
        training_job_params = self.__set_training_job_params(job_name=training_job_name, training_data_name=training_data_name,
                                                             validation_data_name=validation_data_name, put_timestamp=True)
        try:
            print('Will create a job {0}\nto train model of {1} on AWS sagemaker...'.format(self.__training_job_name, self.ml_model_name))
            self.__sm_client.create_training_job(**training_job_params) # Debug. Attn.
            __status = 'InProgress'
            while __status == 'InProgress':
                __status = self.__sm_client.describe_training_job(TrainingJobName=self.__training_job_name)['TrainingJobStatus']
                time.sleep(60)
            if __status == 'Completed':
                print('...Complete!')
            elif __status == 'Stopped':
                print('...Training job has been stopped. Please see AWS SageMaker dashboard for more details.')
                raise TypeError
            else:
                print('...Failed in training machine learning model on AWS SageMaker.')
                raise TypeError
        except Exception as e:
            print('Failed in training a model on AWS Sagemaker. Reason:\n{}\n'.format(e))
            raise

    def host_model(self, suffix='-model'):
        '''
        This is a high level function and wrapper to host trained model on AWS SageMaker by using trained model artifact.
        :param suffix: Suffix that needs to be added to training job name, to reduce confusion and complexity.
               Type: string.
        :return host_model_response: Response of hosting model on AWS SageMaker. Type: dict.
        '''
        try:
            self.__hosted_model_name = self.__training_job_name + suffix
            info = self.__sm_client.describe_training_job(TrainingJobName=self.__training_job_name)
            __model_url = info['ModelArtifacts']['S3ModelArtifacts']
            __primary_container = {
                'Image': self.__container,
                'ModelDataUrl': __model_url
            }

            _tag = [{'Key':key, 'Value':self.__tag[key]} for key in self.__tag.keys()]
            self.__host_model_response = self.__sm_client.create_model(
                ModelName=self.__hosted_model_name,
                ExecutionRoleArn=self.__role,
                PrimaryContainer=__primary_container,
                Tags=_tag
            )
            return self.__host_model_response
        except Exception as e:
            print('Failed in hosting machine learning model on Sagemaker. Reason:\n{}\n'.format(e))
            raise

    def create_endpoint(self, endpoint_prefix='EndpointConfig-'):
        '''
        This is a high level function and wrapper to create an endpoint on AWS SageMaker for making prediction by using
        hosted machine learning model. It is in beta version and needs to adjust input variables.
        :param prefix:
        :return:
        '''
        try:
            self.__endpoint_config_name = endpoint_prefix + self.__training_job_name
            _tag = [{'Key':key, 'Value':self.__tag[key]} for key in self.__tag.keys()]
            __create_endpoint_config_response = self.__sm_client.create_endpoint_config(
                EndpointConfigName=self.__endpoint_config_name,
                ProductionVariants=[{
                    'InstanceType': 'ml.m4.xlarge',
                    'InitialVariantWeight': 1,
                    'InitialInstanceCount': 1,
                    'ModelName': self.__hosted_model_name,
                    'VariantName': 'AllTraffic'}],
                Tags=_tag
            )

            self.__endpoint_config_arn = __create_endpoint_config_response['EndpointConfigArn']
            self.__endpoint_name = 'Endpoint-' + self.__training_job_name

            _tag = [{'Key':key, 'Value':self.__tag[key]} for key in self.__tag.keys()]
            __create_endpoint_response = self.__sm_client.create_endpoint(
                EndpointName=self.__endpoint_name,
                EndpointConfigName=self.__endpoint_config_name,
                Tags=_tag
            )

            __endpoint_resp = self.__sm_client.describe_endpoint(EndpointName=self.__endpoint_name)
            __endpoint_status = __endpoint_resp['EndpointStatus']

            while __endpoint_status == 'Creating':
                time.sleep(60)
                __endpoint_resp = self.__sm_client.describe_endpoint(EndpointName=self.__endpoint_name)
                __endpoint_status = __endpoint_resp['EndpointStatus']

            if __endpoint_status == 'InService':
                print('...Endpoint has been created.')
            else:
                print('...Failed in creating endpoint.')

            return __endpoint_resp
        except Exception as e:
            print('Failed in creating the endpoint. Reason:\n{}\n'.format(e))

    def __do_predict(self, data, endpoint_name, content_type):
        '''
        Make prediction by using an endpoint on AWS SageMaker.
        :param data: Data for making prediction, only supports libsvm format for this version. Type: string.
        :param endpoint_name: Name of created endpoint on SageMaker. Type: string.
        :param content_type: Content type for input data. Type: string.
        :return preds: Result of prediction. Type: list.
        '''
        payload = '\n'.join(data)
        response = self.__runtime_client.invoke_endpoint(EndpointName=endpoint_name,
                                                         ContentType=content_type,
                                                         Body=payload)
        result = response['Body'].read().decode('ascii')
        preds = [float(num) for num in result.split(',')]
        return preds

    def __batch_predict(self, data, batch_size, endpoint_name, content_type):
        '''
        Make batch prediction by using a low level function of self.__do_prediction.
        :param data: Data for making prediction, only supports libsvm format for this version. Type: string.
        :param batch_size: Batch size of data for making prediction. Type: int.
        :param endpoint_name: Name of created endpoint on SageMaker. Type: string.
        :param content_type: Content type for input data. Type: string.
        :return arrs: Result of batch prediction. Type: tuple.
        '''
        items = len(data)
        arrs = []
        for offset in range(0, items, batch_size):
            arrs.extend(self.__do_predict(data=data[offset:min(offset + batch_size, items)],
                                          endpoint_name=endpoint_name, content_type=content_type))
            sys.stdout.write('.')
        return (arrs)

    def predict(self, pred_data_filename, endpoint_name=None, content_type='text/x-libsvm', batch_size=64):
        '''
        This is a high level function and wrapper to make batch prediction by using an endpoint on AWS SageMaker. It is
        in beta version and only XGBoost is supported for now.
        :param pred_data_filename: File name of data for making prediction. Type: string.
        :param endpoint_name: Name of endpoint for making prediction. Type: string.
        :param content_type: Content type defined for SageMaker. Type: string.
        :param batch_size: Batch data size for making prediction. Type: int.
        :return preds: Result of prediction. Type: tuple.
        '''
        if endpoint_name == None:
            endpoint_name = self.__endpoint_name

        try:
            with open(pred_data_filename, 'r') as f:
                payload = f.read().strip()
            pred_data = payload.split('\n')
            preds = self.__batch_predict(data=pred_data, batch_size=batch_size, endpoint_name=endpoint_name,
                                         content_type=content_type)
            return preds
        except Exception as e:
            print('Failed in making prediction. Reason:\n{}\n'.format(e))
            raise TypeError

    def delete_model(self, model_name=None):
        '''
        Post process the data pipeline on AWS SageMaker to remove hosted model after making batch prediction.
        :param model_name: Model name. Type: string.
        :return delete_model_resp: Response of deleting model on SageMaker. Type: dict.
        '''
        if model_name == None:
            model_name = self.__hosted_model_name

        try:
            self.__delete_model_resp = self.__sm_client.delete_model(ModelName=model_name)
            print('AWS SageMaker ML model {} has been successfully deleted.'.format(model_name))
            return self.__delete_model_resp
        except Exception as e:
            print('Failed in deleting ML model {0}. Reason:\n{1}\n'.format(model_name, e))
            return e

    def delete_endpoint(self, endpoint_name=None):
        '''
        Post process the data pipeline on AWS SageMaker to remove created endpoint after making batch prediction.
        :param endpoint_name: Endpoint name. Type: string.
        :return delete_endpoint_resp: Response of deleting endpoint on SageMaker. Type: dict.
        '''
        if endpoint_name == None:
            endpoint_name = self.__endpoint_name

        try:
            self.__delete_endpoint_resp = self.__sm_client.delete_endpoint(EndpointName=endpoint_name)
            print('AWS SageMaker endpoint {} has been successfully deleted.'.format(endpoint_name))
            return self.__delete_endpoint_resp
        except Exception as e:
            print('Failed in deleting endpoint {0}. Reason:\n{1}\n'.format(endpoint_name, e))
            return e

    def delete_endpoint_config(self, endpoint_config=None):
        '''
        Post process the data pipeline on AWS SageMaker to remove endpoint configuration after making batch prediction.
        :param endpoint_config: Configuration name of endpoint. Type: string.
        :return delete_endpoint_resp: Response of deleting an endpoint configuration. Type: dict.
        '''

        if endpoint_config == None:
            endpoint_config = self.__endpoint_config_name

        try:
            self.__delete_endpoint_config_resp = self.__sm_client.delete_endpoint_config(EndpointConfigName=endpoint_config)
            print('AWS SageMaker endpoint configuration has been successfully deleted.'.format(endpoint_config))
            return self.__delete_endpoint_config_resp
        except Exception as e:
            print('Failed in deleting endpoint configuration {0}. Reason:\n{1}\n'.format(endpoint_config, e))
            return e

    def cleaning(self):
        '''
        This is a high level function and wrapper containing functions of post-processing data pipeline on AWS
        SageMaker to remove models and endpoints/ configurations after making batch prediction.
        :return cleaning_resp: Response of the whole cleaning procedure.
        '''
        self.__cleaning_resp = dict()

        try:
            self.__cleaning_resp['delete_endpoint'] = self.delete_endpoint()
            self.__cleaning_resp['delete_endpoint_config'] = self.delete_endpoint_config()
            self.__cleaning_resp['delete_model'] = self.delete_model()
            return self.__cleaning_resp
        except Exception as e:
            print('Failed in cleaning models/ endpoints. Reason:\n{}\n'.format(e))
            return e
        
class aws_polly(AWS):

    def __init__(self):
        self.__create_polly_client()

    def __create_polly_client(self):
        try:
            self.__polly_client = boto3.client('polly')
        except:
            self.__polly_client = boto3.client('polly', region_name='us-east-1')

    def __synthesize_speech(self, text, output_format='mp3', text_format='text', voiceid='Brian'):

        try:
            __response = self.__polly_client.synthesize_speech(
                                                                    OutputFormat=output_format,
                                                                    Text=text,
                                                                    TextType=text_format,
                                                                    VoiceId=voiceid
                                                                )
            return __response
        except Exception as e:
            print('Failed in synthesizing speech. Reason:\n{}\n'.format(e))
            raise

        return __response

    # def speak_use_template(self, template, **kwargs):
    #     try:
    #         __tmp_sound_file = 'tmp.mp3'
    #
    #         __speech_template = string.Template(template)
    #         __speech_template = __speech_template.safe_substitute(kwargs)
    #
    #         response = self.__synthesize_speech(text=__speech_template)
    #         if "AudioStream" in response:
    #             with closing(response['AudioStream']) as stream:
    #                 __sound_bytes = stream.read()
    #
    #             with open(__tmp_sound_file, 'wb+') as tmp_file:
    #                 tmp_file.write(__sound_bytes)
    #
    #             mixer.init()
    #             mixer.music.load('tmp.mp3')
    #             mixer.music.play()
    #
    #             os.remove(__tmp_sound_file)
    #         else:
    #             return response
    #     except Exception as e:
    #         print('Failed in generating speech using provided template. Reason:\n{}\n'.format(e))

class aws_notifier(AWS):

    def __init__(self, **kwargs):
        self.__dict = kwargs
        self.__email_config = None

    @classmethod
    def decode_input_params(cls, **kwargs):
        __original_params = kwargs
        __decoded_params = dict()

        for ele in __original_params:
            try:
                __decoded_params[ele] = cls.base64decoding(encoded_string=__original_params[ele])
            except:
                __decoded_params[ele] = __original_params[ele]

        return __decoded_params


    @classmethod
    def base64decoding(cls, encoded_string):
        return b64decode(encoded_string).decode('utf-8')

    @classmethod
    def email(cls, **kwargs):
        __email_config = cls.decode_input_params(**kwargs)
        try:
            __msg = email.mime.text.MIMEText(__email_config['message'], 'html')
            __msg['Subject'] = __email_config['title']
            __msg['From'] = __email_config['email_from']
            __msg['To'] = __email_config['email_to']

            if type(__email_config['email_cc']) == tuple:
                __msg['Cc'] = __email_config['email_cc']
            elif type(__email_config['email_cc']) == list:
                __msg['Cc'] = ','.join(__email_config['email_cc'])
            elif type(__email_config['email_cc']) == str:
                __msg['Cc'] = __email_config['email_cc']
            else:
                raise TypeError('The input format of cc is incorrect.')

            __email_client_host_addr = __email_config['email_host']
            __email_client_port = __email_config['email_port']
            __email_client_username = __email_config['email_username']
            __email_client_password = __email_config['email_password']

            __smtp_client = smtplib.SMTP(__email_client_host_addr, __email_client_port)
            __smtp_client.starttls()
            __smtp_client.login(__email_client_username, __email_client_password)
            __smtp_client.sendmail(__msg['From'], [__msg['To'], __msg['Cc']], __msg.as_string())
            __smtp_client.quit()
            print('Email has been successfully sent out.')
        except Exception as e:
            print('Failed in sending out email as notification. Reason:\n{}\n'.format(e))
            raise

    def debug(self, **kwargs):
        try:
            params = self.decode_input_params(**kwargs)
            print(params)
        except Exception as e:
            raise

class aws_dynamodb(AWS):

    def __init__(self):
        self.__create_dynamodb_client()

    def __create_dynamodb_client(self):
        self.__dynamodb_client = boto3.client('dynamodb')

    def __create_dynamodb_resource(self):
        self.__dynamodb_resource = boto3.resource('dynamodb')

    def update_item(self, table_name):
        pass

class aws_ssm(AWS):

    def __init__(self):
        self.__create_ssm_client()

    def __create_ssm_client(self):
        self.__client = boto3.client('ssm')


class aws_rekognition(AWS):

    def __init__(self):
        self.__create_rekognition_client()

    def __create_rekognition_client(self):
        try:
            self.__aws_recognition_client = boto3.client('rekognition')
        except Exception as e:
            print('Error while creating AWS Rekognition client. Error: {}'.format(e))
            raise

    def detect_faces(self, img_path):
        assert os.path.exists(img_path), 'File {} does not exists.'.format(img_path)
        with open(img_path, 'rb') as img_file:
            _b_img = img_file.read()

        try:
            __resp = self.__aws_recognition_client.detect_faces(
                Image = {
                    'Bytes':_b_img
                }
            )
            return __resp
        except Exception as e:
            print('Error: {]'.format(e))

    def facial_detection(self, img_path, confidence_threshold=0.8, add_bbox_rot=False):
        assert os.path.exists(img_path), 'Input image path {} does not exist.'.format(img_path)
        _img_files = os.listdir(img_path)
        output = dict()
        for _file in tqdm(_img_files):
            _res = self.detect_faces(img_path + '/' + _file)
            _detected_faces = _res['FaceDetails']

            bbox_coordinates = []
            _img = cv2.imread(img_path + '/' + _file)
            for i, _face in enumerate(_detected_faces):
                if _face['Confidence'] >= confidence_threshold:
                    _bbox = _face['BoundingBox']
                    _bbox_pose = _face['Pose']
                    _bbox_coordinate = [
                        _bbox['Left']*_img.shape[1], _bbox['Top']*_img.shape[0], \
                        _bbox['Width']*_img.shape[1], _bbox['Height']*_img.shape[0]
                    ]
                    if add_bbox_rot:
                        _bbox_coordinate += [_bbox_pose['Yaw']]
                    bbox_coordinates.append(_bbox_coordinate)

            output[_file] = bbox_coordinates

        return output

    def visualize_bbox(self, img, bbox_coordinates, bbox_rot_angles=None, **kwargs):
        if bbox_rot_angles is not None:
            assert len(bbox_coordinates) == len(bbox_rot_angles), 'Provided bbox coordinates and rotation angles are not of same lengths.'

        # Create figure and axes
        fig,ax = plt.subplots(1, dpi=150)

        # Display the image
        # test_image = cv2.imread('./images/MicrosoftTeams-image.png')
        # test_image = cv2.cvtColor(test_image, cv2.COLOR_BGR2RGB)
        # res = rek.detect_faces(bytearray(test_image))
        # test_image = test_image[::-1, ::-1]
        ax.imshow(img)

        for i, ele in enumerate(bbox_coordinates):
            # Create a Rectangle patch
            x, y, width, height = ele

            if bbox_rot_angles is not None:
                _rot_angle = bbox_rot_angles[i]
            else:
                _rot_angle = 0.0

            rect = patches.Rectangle((x,y),width=width, height=height, linewidth=1,edgecolor='b', \
                                     facecolor='none', angle=_rot_angle)

            # Add the patch to the Axes
            ax.add_patch(rect)

        plt.show()
















