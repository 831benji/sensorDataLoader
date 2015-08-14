import os
import requests
import json
import csv
try:
  from SimpleHTTPServer import SimpleHTTPRequestHandler as Handler
  from SocketServer import TCPServer as Server
except ImportError:
  from http.server import SimpleHTTPRequestHandler as Handler
  from http.server import HTTPServer as Server

csvFileName = 'output.csv'
JSONconfigFile = 'dataConfig.csv'
username = 'bsp'
password = 'demoPass'
db_name = 'july'

baseUri = "https://{0}.cloudant.com/{1}".format(username, db_name)

creds = (username, password)

# Read port selected by the cloud for our application
PORT = int(os.getenv('VCAP_APP_PORT', 8000))
# Change current directory to avoid exposure of control files
os.chdir('static')

httpd = Server(("", PORT), Handler)

def loadData():
	print('inside this function!!!')
	
try:
  print("Start serving at port %i" % PORT)
  r = requests.get('http://sct.gpacsys.net/query.php?username=dford&password=dford1234&logtype=DATA&format=$2&start_year=2015&start_month=0&start_day=0&start_hour=00&start_min=00&start_sec=00&end_year=2015&end_month=2&end_day=28&end_hour=23&end_min=59&end_sec=59')
  f = open(csvFileName, 'w')
  f.write(r.text)
  f.close()

  # open csv with data (will be a HTTP call to GPAC in the future)
  f = open(csvFileName, 'rU')
  csv_f = csv.reader(f)

  # get field names from first row of csv file
  fieldnames = csv_f.next()

  # establish the array to hold the bulk load documents
  docs = [[]]

  # establish counters to break up the loads into smaller chunks
  arrayCounter = 0
  docsCounter = 0

  # set the fieldnames as the dictionary keys
  reader = csv.DictReader(f, fieldnames)

  # read JSONconfigFile
  jsonF = open(JSONconfigFile, 'rU')
  jsonConfig = csv.reader(jsonF)

  # get field names from first row of JSONconfigFile
  JSONfieldnames = jsonConfig.next()

  for row in reader:
  	jsonDocument = {}
  	for fieldname in JSONfieldnames:
  		if fieldname in row:
  			jsonDocument[fieldname] = row[fieldname]

  	# append the jsonDocument to the array of json docs
  	docs[arrayCounter].append(jsonDocument)
  	docsCounter += 1

  	# if the number of docs gets larger than the bulk_load size, break into a new chunk
  	if docsCounter > 999:
  		arrayCounter += 1
  		docsCounter = 0
  		docs.append([])

  	# print jsonDocument
  # print docs

  response = requests.put(
    baseUri,
    auth=creds
    )

  print "Created database at {0}".format(baseUri)

  # arrayCounter = 0
  # for bulkDocs in docs:
  # 	response = requests.post(
  # 		baseUri+'/_bulk_docs',
  # 		data=json.dumps({
  #       "docs": docs[arrayCounter]
  #       }),
  #       auth=creds,
  #       headers={"Content-Type": "application/json"}
  #       )
  # 	arrayCounter += 1

  loadData()

  


  httpd.serve_forever()
except KeyboardInterrupt:
  pass
httpd.server_close()