"""
    :file: spark_results_boto.py
    :brief: This program grabs the latest verson of the .html file from EMR
     and uploads it to S3 to update the website.
"""

import ssl
import boto
from boto.s3.connection import S3Connection
import os


def boto_upload_s3(html_file):
    conn = S3Connection(host='s3.amazonaws.com')
    if hasattr(ssl, '_create_unverified_context'):
        ssl._create_default_https_context = ssl._create_unverified_context
    website_bucket = conn.get_bucket('edmundssparksubmitresults')
    output_file = website_bucket.new_key('topVehicles.html')
    output_file.content_type = 'text/html'
    output_file.set_contents_from_string(html_file, policy='public-read')


if __name__ == '__main__':
    # grab the latest version of "topVehicles.html" from EMR
    # scp - i
    # ~ / the_cloud.pem
    # hadoop @ ec2 - 54 - 159 - 219 - 64.
    # compute - 1.
    # amazonaws.com: ~ / topVehicles.html
    # ~ / OneDrive / Edmunds - Car - Data - Pipeline - sdk - python
    os.system(
        "scp -i ~/the_cloud.pem hadoop@ec2-54-159-219-64.compute-1.amazonaws.com:~/topVehicles.html ~/OneDrive/Edmunds-Car-Data-Pipeline-sdk-python")

    top = open("topVehicles.html", "r")
    html_file = top.read()
    boto_upload_s3(html_file)

top.close()
