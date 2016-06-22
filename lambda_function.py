from __future__ import print_function

__author__ = 'mpetyx'

import boto3
import zipfile
import json
from StringIO import StringIO

print('Loading function')

destination_bucket = 'your-destination-s3-bucket-here'
source_bucket = 'your-source-s3-bucket-here' 

s3_client = boto3.client('s3')
s3 = boto3.resource('s3')

def check_if_a_key_exists(key):
    results = s3_client.list_objects(Bucket=destination_bucket, Prefix=key)
    return 'Contents' in results

def zip_files(destination_folder, s3_files):

    # if not s3_client.check_file_exists(destination_bucket, destination_folder):
    # if not check_if_a_key_exists(destination_folder):
    if True:
        zip_data = StringIO()
        zip = zipfile.ZipFile(zip_data, mode='w', compression=zipfile.ZIP_DEFLATED)

        for s3_file in s3_files:
            # download s3_file & add to zip
            temp = s3_client.download_file(source_bucket, s3_file,'/tmp/'+ str(s3_file))
            zip.writestr(s3_file + '.' + s3_file.split('.')[-1],
                         open('/tmp/'+s3_file, 'rb').read())

        zip.close()
        zip_data.seek(0)

        f = open('/tmp/' + str(destination_folder) + ".zip", 'wb')
        f.write(zip_data.read())
        f.close()

        s3_client.upload_file('/tmp/' + str(destination_folder) + ".zip", destination_bucket, destination_folder + '.zip')
        # s3.Object(destination_bucket,destination_folder + '.zip' ).put(ACL='public-read')

    # s3_client.delete_object(Bucket=bucket, Key=key)
    # send the response
    # response = HttpResponse()
    # response['Content-Disposition'] = 'attachment; filename=%s.zip' %destination_folder
    # response['Content-Type'] = 'application/x-zip'

    url = s3_client.generate_presigned_url(
        'get_object',
        Params={

            'Bucket': destination_bucket,
            'Key': destination_folder+ ".zip",},
        ExpiresIn=3600,

    )

    return url


def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))

    """
    sample event
    {
      "s3_files": "000041a2-1464798396.jpg,000041a2-cddf-4389-add2-cf2a8c3ce4ea-1464798402-preview.mp3,00005407-c573-da342aada-1453768852.mp3,0001300f-bfc6eb2e2873-1463992236.pdf",
      "destination_folder": "my_new_folder",
    }
    """

    # s3_files = event['params']['querystring']['s3_files'] # A list of s3 ids like the example aboveo

    #destination_folder = event['params']['querystring']['destination_folder']
    s3_files = event['params']['querystring']['s3_files']

    # try:
    response = zip_files(destination_folder=destination_folder, s3_files=s3_files)
    # print("CONTENT TYPE: " + response['ContentType'])
    return {
        "temporary_url":response
    }

    # except Exception as e:
    #     print(e)
    #     print(
    #         'Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'
    #             # .format(
    #             # key, bucket)
    #     )
    #     raise e
