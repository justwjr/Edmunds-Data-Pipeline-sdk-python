import ssl
import boto
from boto.s3.connection import S3Connection

def boto_upload_s3(html_file):
    conn = S3Connection()
    if hasattr(ssl, '_create_unverified_context'):
        ssl._create_default_https_context = ssl._create_unverified_context
    website_bucket = conn.get_bucket('edmundssparksubmitresults')
    output_file = website_bucket.new_key('topVehicles.html')
    output_file.content_type = 'text/html'
    output_file.set_contents_from_string(html_file, policy='public-read')

if __name__ == '__main__':
	top = open("topVehicles.html", "r")
	html_file = top.read()
	boto_upload_s3(html_file)
top.close()
