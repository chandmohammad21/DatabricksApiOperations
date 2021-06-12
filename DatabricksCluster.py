import json
import requests

#Funtion to create/update/delete a databricks cluster
def databricksClusterOps(domain: str, token: str, ops: str, jsonObj: str):
  endpoint = ''
  if ops == 'create':
    endpoint = 'https://{0}/api/2.0/clusters/{1}'.format(domain, ops)
  elif ops == 'edit':
    endpoint = 'https://{0}/api/2.0/clusters/{1}'.format(domain, ops)
  elif ops == 'delete':
    endpoint = 'https://{0}/api/2.0/clusters/{1}'.format(domain, 'permanent-delete')
  elif ops == 'start':
    endpoint = 'https://{0}/api/2.0/clusters/{1}'.format(domain, ops)  
  elif ops == 'stop':
    endpoint = 'https://{0}/api/2.0/clusters/{1}'.format(domain, 'delete') 
  elif ops == 'restart':
    endpoint = 'https://{0}/api/2.0/clusters/{1}'.format(domain, ops)
  elif ops == 'get':
    endpoint = 'https://{0}/api/2.0/clusters/{1}'.format(domain, ops)
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

  #this can be done by defining a json string as well
  with open('clusterConfig\config.json') as f:
      jsonObj = json.load(f)

  #Cluster creation start
  response = databricksClusterOps(DOMAIN, TOKEN, 'create', jsonObj)

  if response.status_code == 200:
    print(response.json()['cluster_id'])
  else:
    print("Error launching cluster: %s: %s" % (response.json()["error_code"], response.json()["message"]))
  #Cluster creation ends    

  #Cluster Get start
  jsonObj = '{ "cluster_id": "1234-567890-reef123" }'
  response = databricksClusterOps(DOMAIN, TOKEN, 'get', jsonObj)

  if response.status_code == 200:
    print(response.json())
  else:
    print("Error getting cluster: %s: %s" % (response.json()["error_code"], response.json()["message"]))
  #Cluster Get ends      

  #Cluster Delete start
  jsonObj = '{ "cluster_id": "1234-567890-reef123" }'
  response = databricksClusterOps(DOMAIN, TOKEN, 'delete', jsonObj)

  if response.status_code == 200:
    print(response.json())
  else:
    print("Error Deleting cluster: %s: %s" % (response.json()["error_code"], response.json()["message"]))
  #Cluster Delete ends      

  #Cluster edit start
  with open('clusterConfig\config_update.json') as f:
      jsonObj = json.load(f)
  response = databricksClusterOps(DOMAIN, TOKEN, 'delete', jsonObj)

  if response.status_code == 200:
    print(response.json())
  else:
    print("Error Updating cluster: %s: %s" % (response.json()["error_code"], response.json()["message"]))
  #Cluster edit ends       

  #Cluster spin start
  jsonObj = '{ "cluster_id": "1234-567890-reef123" }'
  response = databricksClusterOps(DOMAIN, TOKEN, 'start', jsonObj)

  if response.status_code == 200:
    print(response.json())
  else:
    print("Error Starting cluster: %s: %s" % (response.json()["error_code"], response.json()["message"]))
  #Cluster spin ends     

  #Cluster stop/terminate start
  jsonObj = '{ "cluster_id": "1234-567890-reef123" }'
  response = databricksClusterOps(DOMAIN, TOKEN, 'stop', jsonObj)

  if response.status_code == 200:
    print(response.json())
  else:
    print("Error Termination cluster: %s: %s" % (response.json()["error_code"], response.json()["message"]))
  #Cluster stop/terminate ends   

  #Cluster restart start
  jsonObj = '{ "cluster_id": "1234-567890-reef123" }'
  response = databricksClusterOps(DOMAIN, TOKEN, 'restart', jsonObj)

  if response.status_code == 200:
    print(response.json())
  else:
    print("Error Restarting cluster: %s: %s" % (response.json()["error_code"], response.json()["message"]))
  #Cluster restart ends     

  