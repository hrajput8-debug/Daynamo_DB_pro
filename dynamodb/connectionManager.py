# Copyright 2014. Amazon Web Services, Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import boto3
from botocore.exceptions import ClientError

def getDynamoDBConnection(config=None, endpoint=None, port=None, local=False, use_instance_metadata=False):
    if local:
        return boto3.resource('dynamodb', endpoint_url=f"http://{endpoint}:{port}")
    else:
        return boto3.resource('dynamodb')

def createGamesTable(db):
    try:
        table = db.create_table(
            TableName='Games',
            KeySchema=[
                {'AttributeName': 'GameId', 'KeyType': 'HASH'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'GameId', 'AttributeType': 'S'}
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        table.wait_until_exists()
        print("✅ Games table created.")
        return table
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            print("ℹ️ Games table already exists.")
            return db.Table('Games')
        else:
            raise e
