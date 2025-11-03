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


rom .setupDynamoDB import getDynamoDBConnection, createGamesTable
from uuid import uuid4
from botocore.exceptions import ClientError

class ConnectionManager:

    def __init__(self, mode=None, config=None, endpoint=None, port=None, use_instance_metadata=False):
        self.db = None
        self.gamesTable = None

        if mode == "local":
            if config is not None:
                raise Exception('Cannot specify config when in local mode')
            if endpoint is None:
                endpoint = "http://localhost"
            if port is None:
                port = 8000
            self.db = getDynamoDBConnection(endpoint=f"{endpoint}:{port}", local=True)

        elif mode == "service":
            self.db = getDynamoDBConnection(
                config=config,
                endpoint=endpoint,
                use_instance_metadata=use_instance_metadata
            )
        else:
            raise Exception("Invalid arguments, please refer to usage.")

        self.setupGamesTable()

    def setupGamesTable(self):
        try:
            # Try getting existing table
            self.gamesTable = self.db.Table("Games")
            # Force metadata load → ensures table exists
            self.gamesTable.load()
            print("✅ Games table loaded successfully.")
        except ClientError as e:
            if e.response['Error']['Code'] == "ResourceNotFoundException":
                print("⚠️ Games table not found → Creating new one…")
                self.gamesTable = createGamesTable(self.db)
            else:
                raise e

    def getGamesTable(self):
        if self.gamesTable is None:
            self.setupGamesTable()
        return self.gamesTable

    def createGamesEntry(self, hostId, opponentId, statusDate, extraData=None):
        if extraData is None:
            extraData = {}

        item = {
            "GameId": str(uuid4()),
            "HostId": hostId,
            "OpponentId": opponentId,
            "StatusDate": statusDate,
            **extraData
        }

        table = self.getGamesTable()
        table.put_item(Item=item)
        return item
