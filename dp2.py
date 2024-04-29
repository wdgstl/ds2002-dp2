from pymongo import MongoClient, errors
from bson.json_util import dumps
import os
import json

#Credentials
MONGOPASS = os.getenv('MONGOPASS')
uri = "mongodb+srv://cluster0.pnxzwgz.mongodb.net/"
client = MongoClient(uri, username='nmagee', password=MONGOPASS, connectTimeoutMS=200, retryWrites=True)

#specify a database
db = client.spq4pq

#create new collection
dp2coll = db["dp2coll"]

#Traverse Directory and Import Files
directory = "/workspace/ds2002-dp2/data"
try:
    for filename in os.listdir(directory):
        try:
            with open(os.path.join(directory, filename)) as f:
                try:
                    file_data = json.load(f)
                except Exception as e:
                    print(f"Error {e} when loading {f}\n")
                if isinstance(file_data, list):
                        for file in file_data:
                            try:
                                dp2coll.insert_one(file)
                            except Exception as e:
                                print(f"Error {e} when importing into Mongo\n")
                else:
                    try:
                        dp2coll.insert_one(file_data)
                    except Exception as e:
                        print(e)
        except Exception as e:
            print(f"Error: {e}\n")
        finally:
             if 'f' in locals():  # Check if 'file' variable exists (was opened)
                f.close()  # Close the file if it was opened
except  FileNotFoundError as e:
    print(f"Error: Directory {directory} not found.\n")
    

count = dp2coll.count_documents({})
print(count, "documents")