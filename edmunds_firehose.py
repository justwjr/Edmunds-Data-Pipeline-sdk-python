#!/usr/bin/env python

"""
    @file edmunds_firehose_kinesis.py

        set up firehose delivery stream http://docs.aws.amazon.com/firehose/latest/dev/basic-create.html#console-to
        -s3 S3 Bucket: justw149 ssh -i ~/.ssh/the_cloud.pem ubuntu@ec2-54-161-219-252.compute-1.amazonaws.com scp -i
        ~/the_cloud.pem edmunds_firehose_kinesis.py ubuntu@ec2-54-161-219-252.compute-1.amazonaws.com:/home/ubuntu/

        ubuntu:
            sudo apt install awscli
            aws configure
            pip install boto3
            nohup python edmunds_firehose_kinesis.py&


            git clone https://github.com/EdmundsAPI/sdk-python
"""

import boto3
import os
import yaml
import json
# from edmunds import edmundsStream, OAuth
from edmunds import Edmunds

import time
import pickle

# def firehose_tweets(edmunds_stream):
#     """
#     INPUT: edmunds_stream object.
#     OUTPUT: tweets in json format inserted in S3 using Kinesis.
#     """
#     iterator = edmunds_stream.statuses.sample()
#     for tweet in iterator:
#         if 'id' in tweet:
#             try:
#                 response = firehose_client.put_record(
#                     DeliveryStreamName='edmunds_firehose',
#                     Record={'Data': json.dumps(str(tweet) + '\n')})
#                 print(response)
#             except Exception:
#                 print("Did not work.")


if __name__ == '__main__':

    credentials = yaml.load(open(os.path.expanduser('~/edmunds_api_cred.yml')))
    creds = credentials['edmunds']
    api = Edmunds(creds.get('Key'), True) # use Edmunds('YOUR API KEY', True) for debug mode

    firehose_client = boto3.client('firehose', region_name='us-east-1')
#     edmunds_stream = edmundsStream(auth=OAuth(**credentials['edmunds']))

#     while True:
#     firehose_tweets(edmunds_stream)
#     all_makes = api.make_call('/api/vehicle/v2/makes')
#     all_makes['time_stamp'] = time.time()

    '''
    create list of make, model tuples
    '''
    # n = len(all_makes['makes'])
    # make_models = []
    #
    # make_models_file = open('make_models.txt', 'w')
    #
    # for x in range(n):
    #     for y in range(len(all_makes['makes'][x]['models'])):
    #         make = all_makes['makes'][x]['niceName']
    #         model = all_makes['makes'][x]['models'][y]['niceName']
    #         make_models.append((make.encode('utf-8'), model.encode('utf-8')))
    # # print>>make_models_file, (make.encode('utf-8'), model.encode('utf-8'))
    # # make_models_file.write(make_models)
    #
    # with open('make_models.txt', 'w') as f:
    #     pickle.dump(make_models, f)

    # open the processed make_models pickle

    make_models = pickle.load(open('make_models.txt', 'rb'))

    for make, model in make_models:
        s = '/api/vehicle/v2/' + make + '/' + model + '/'
        # print(s)

        model_response = api.make_call(s)
        model_response['time_stamp'] = time.time()

        # print(model_response)

        try:
            response = firehose_client.put_record(
                DeliveryStreamName='edmunds_firehose',
                Record={'Data': json.dumps(model_response) + '\n'})
            # print(response)
        except Exception as e:
                print(str(e))

        time.sleep(3600)  # 3600 seconds => 1 hour
