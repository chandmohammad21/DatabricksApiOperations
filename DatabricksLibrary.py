import requests

#Funtion to create/update/delete a databricks cluster
def databricksDBFSOps(domain: str, token: str, ops: str, jsonObj: str):
  endpoint = ''
  if ops == 'install':
    endpoint = 'https://{0}/api/2.0/libraries/{1}'.format(domain, ops)
  elif ops == 'uninstall':
    endpoint = 'https://{0}/api/2.0/libraries/{1}'.format(domain, ops)
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
    fileName = 'dbfs:/FileStore/jars/deequ-1.2.2-spark-3.0.jar'

    #Install library starts
    jsonObj = {
        "cluster_id": "10201-my-cluster",
        "libraries": [
            {
            "jar": fileName
            },
            {
            "maven": {
                "coordinates": "org.jsoup:jsoup:1.7.2",
                "exclusions": ["slf4j:slf4j"]
            }
            }
        ]
    }

    response = databricksDBFSOps(DOMAIN, TOKEN, 'install', jsonObj)

    if response.status_code == 200:
        print(response.json())
    else:
        print("Error installing library: %s: %s" % (response.json()["error_code"], response.json()["message"]))
    #Install library ends

    #Uninstall library starts
    jsonObj = {
    "cluster_id": "10201-my-cluster",
    "libraries": [
        {
        "jar": fileName
        }
    ]
    }

    response = databricksDBFSOps(DOMAIN, TOKEN, 'uninstall', jsonObj)

    if response.status_code == 200:
        print(response.json())
    else:
        print("Error Uninstalling library: %s: %s" % (response.json()["error_code"], response.json()["message"]))
    #Uninstall library ends