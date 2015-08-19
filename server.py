import os
import requests
import json
import csv
import sched
import time
# from apscheduler.scheduler import Scheduler
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
db_name = 'testing_database'

scheduler = sched.scheduler(time.time, time.sleep)

currentTime = time.gmtime()

first_time_flag = true;

gpac_username = 'dford'
gpac_password = 'dford1234'
log_type = 'DATA'
start_year = '2015'
start_month = '8'
start_day = '18'
start_hour = '00'
start_min = '00'
start_sec = '00'
end_year = '2015'
end_month = '7' 
end_day = '18'
end_hour = '23'
end_min = '59'
end_sec = '59'

loopCounter = 0

baseUri = "https://{0}.cloudant.com/{1}".format(username, db_name)

creds = (username, password)

# Read port selected by the cloud for our application
PORT = int(os.getenv('VCAP_APP_PORT', 8000))
# Change current directory to avoid exposure of control files
os.chdir('static')

httpd = Server(("", PORT), Handler)

def loadData():
	global start_year
	global start_month
	global start_day
	global start_hour
	global start_min
	global start_sec

	currentTime = time.gmtime()
	start_year = str(currentTime.tm_year)
	start_month = str(currentTime.tm_mon)
	start_day = str(currentTime.tm_mday)
	start_hour = str((currentTime.tm_hour+24))
	start_min = str(currentTime.tm_hour)
	start_sec = str(currentTime.tm_year)

	if first_time_flag:
		response = requests.put(
		baseUri,
		auth=creds
		)
		print "Created database at {0}".format(baseUri)
		response = requests.post(
			baseUri+'/_cloudant_date',
			data=json.dumps({
	      "cloudant_year": start_year
	      }),
	      auth=creds,
	      headers={"Content-Type": "application/json"}
	      )
	totalDocs = 0
	print('this is the current time:')
	currentTime = time.gmtime()
	print currentTime
	end_year = str(currentTime.tm_year)
	end_month = str(currentTime.tm_mon)
	end_day = str(currentTime.tm_mday)
	end_hour = str(currentTime.tm_hour)
	end_min = str(currentTime.tm_min)
	end_sec = str(currentTime.tm_sec)
	
	r = requests.get(
		'http://sct.gpacsys.net/query.php?username='+
		gpac_username
		+'&password='+
		gpac_password
		+'&logtype='+
		log_type
		+'&format=$2&start_year='+
		start_year
		+'&start_month='+
		start_month
		+'&start_day='+
		start_day
		+'&start_hour='+
		start_hour
		+'&start_min='+
		start_min
		+'&start_sec='+
		start_sec
		+'&end_year='+
		end_year
		+'&end_month='+
		end_month
		+'&end_day='+
		end_day
		+'&end_hour='+
		end_hour
		+'&end_min='+
		end_min
		+'&end_sec='+
		end_sec
		)
	f = open(csvFileName, 'w')
	f.write(r.text)
	f.close()

	global start_year
	start_year = end_year
	global start_month
	start_month = end_month
	global start_day
	start_day = end_day
	global start_hour
	start_hour = end_hour
	global start_min
	start_min = end_min
	global start_sec
	start_sec = end_sec

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
		totalDocs +=1 

		# if the number of docs gets larger than the bulk_load size, break into a new chunk
		if docsCounter > 999:
			arrayCounter += 1
			docsCounter = 0
			docs.append([])

		# print jsonDocument

	# print docs

	print 'this is loop number'
	print loopCounter
	global loopCounter
	loopCounter += 1

	print 'total docs = '+str(totalDocs)

	response = requests.put(
		baseUri,
		auth=creds
		)

	print "Created database at {0}".format(baseUri)

	arrayCounter = 0
	for bulkDocs in docs:
		response = requests.post(
			baseUri+'/_bulk_docs',
			data=json.dumps({
	      "docs": docs[arrayCounter]
	      }),
	      auth=creds,
	      headers={"Content-Type": "application/json"}
	      )
		arrayCounter += 1

	scheduler.enter(600, 1, loadData, ())
	scheduler.run()


try:
  print("Start serving at port %i" % PORT)

  # schedule.every(10).seconds.do(loadData)
  # sched = Scheduler()
  # sched.start()
  # sched.add_interval_job(loadData, seconds=30)

  loadData()


  httpd.serve_forever()
except KeyboardInterrupt:
  pass
httpd.server_close()