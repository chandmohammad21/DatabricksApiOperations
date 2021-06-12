import requests
import base64

#Funtion to create/update/delete a databricks cluster
def databricksDBFSOps(domain: str, token: str, ops: str, jsonObj: str):
  endpoint = ''
  if ops == 'create':
    endpoint = 'https://{0}/api/2.0/dbfs/{1}'.format(domain, ops)
  elif ops == 'delete':
    endpoint = 'https://{0}/api/2.0/dbfs/{1}'.format(domain, ops)
  elif ops == 'add':
    endpoint = 'https://{0}/api/2.0/dbfs/{1}'.format(domain, ops)
  elif ops == 'close':
    endpoint = 'https://{0}/api/2.0/dbfs/{1}'.format(domain, ops)    
  else:
    raise Exception(f"Sorry, Unexpected Databricks Operation: {ops}")

  return requests.post(
    endpoint,
    headers={'Authorization': 'Bearer %s' % TOKEN},
    json=jsonObj
  )

if __name__ == "__main__":
    #Use your own domain/endpoint/instance and token/bearer/pat
    DOMAIN = ''
    TOKEN = ''
    jsonObj = ''
    fileName = 'deequ-1.2.2-spark-3.0.jar'

    #Secret scope create starts
    jsonObj = {
        "path": "/FileStore/jars/%s" % fileName,
        "overwrite": True
    }

    #reating stream handle to initiate the file transfer
    streamHandle = databricksDBFSOps(DOMAIN, TOKEN, 'create', jsonObj)

    if streamHandle.status_code == 200:
        print(streamHandle.json()['handle'])
    else:
        print("Error creating DBFS File: %s: %s" % (streamHandle.json()["error_code"], streamHandle.json()["message"]))

    # with open('files\deequ-1.2.2-spark-3.0.jar', 'rb') as f:
    #     while True:
    #         # A block can be at most 1MB
    #         block = f.read(1 << 20)
    #         if not block:
    #             break
    #         data = base64.standard_b64encode(block)
    #         #data = base64.b64encode(block, 'ascii')


    with open("files\deequ-1.2.2-spark-3.0.jar", "rb") as f:
        while True:
            block = f.read(1 << 20)
            if not block:
                break
            base64_bytes = base64.standard_b64encode(block)
            base64_string = base64_bytes.decode("ascii")
            
            jsonObj = {
                "handle": streamHandle.json()['handle'],
                "data": base64_string
            }

            addBlock = databricksDBFSOps(DOMAIN, TOKEN, 'add', jsonObj)

            if addBlock.status_code == 200:
                print('Blocks added successfully')
            else:
                print("Error adding clocks DBFS : %s: %s" % (addBlock.json()["error_code"], addBlock.json()["message"]))


    #close handle once done
    jsonObj = {
        "handle": streamHandle.json()['handle']
    }

    closeStrean = databricksDBFSOps(DOMAIN, TOKEN, 'close', jsonObj)

    if closeStrean.status_code == 200:
        print('Stream handle closed successfully')
    else:
        print("Error closing handle DBFS : %s: %s" % (closeStrean.json()["error_code"], closeStrean.json()["message"]))

    #delete
    jsonObj = { "path": "/FileStore/jars/%s" % fileName }
    deleteFile = databricksDBFSOps(DOMAIN, TOKEN, 'delete', jsonObj)

