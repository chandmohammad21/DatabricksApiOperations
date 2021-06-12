import requests

#Funtion to create/update/delete a databricks cluster
def databricksSecretScopeOps(domain: str, token: str, ops: str, jsonObj: str):
  endpoint = ''
  if ops == 'scope-create':
    endpoint = 'https://{0}/api/2.0/scopes/{1}'.format(domain, 'create')
  elif ops == 'scope-delete':
    endpoint = 'https://{0}/api/2.0/scopes/{1}'.format(domain, 'delete')
  elif ops == 'secret-put':
    endpoint = 'https://{0}/api/2.0/secrets/{1}'.format(domain, 'put')
  elif ops == 'secret-delete':
    endpoint = 'https://{0}/api/2.0/secrets/{1}'.format(domain, 'delete')    
  else:
    raise Exception(f"Sorry, Unexpected Databricks Operation: {ops}")

  return requests.post(
    endpoint,
    headers={'Authorization': 'Bearer %s' % TOKEN},
    json=jsonObj
  ) 

if __name__ == "__main__":
  #add you domain and token here
  DOMAIN = ''
  TOKEN = ''
  jsonObj = ''

  #Secret scope create starts
  jsonObj = {
  "scope": "my-scope",
  "initial_manage_principal": "users"
  }
  response = databricksClusterOps(DOMAIN, TOKEN, 'scope-create', jsonObj)

  if response.status_code == 200:
    print(response.json())
  else:
    print("Error Creating secret scope: %s: %s" % (response.json()["error_code"], response.json()["message"]))
  #Secret scope create ends 

  #Secret scope delete starts
  jsonObj = {
    "scope": "my-scope"
  }
  response = databricksClusterOps(DOMAIN, TOKEN, 'scope-delete', jsonObj)

  if response.status_code == 200:
    print(response.json())
  else:
    print("Error Deleting secret scope: %s: %s" % (response.json()["error_code"], response.json()["message"]))
  #Secret scope delete ends   

  #Secret Put starts
  jsonObj = {
    "scope": "my-scope",
    "key": "client_key",
    "string_value": "test_value"
  }
  response = databricksClusterOps(DOMAIN, TOKEN, 'secret-put', jsonObj)

  if response.status_code == 200:
    print(response.json())
  else:
    print("Error Putting secret: %s: %s" % (response.json()["error_code"], response.json()["message"]))
  #Secret put ends     

  #Secret Delete starts
  jsonObj = {
    "scope": "my-scope",
    "key": "client_key"    
  }
  response = databricksClusterOps(DOMAIN, TOKEN, 'secret-delete', jsonObj)

  if response.status_code == 200:
    print(response.json())
  else:
    print("Error Deleting secret : %s: %s" % (response.json()["error_code"], response.json()["message"]))
  #Secret Delete ends     
