import os
import requests
import json
import csv
import sched
import time
import string
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
db_name = 'bus_data'

scheduler = sched.scheduler(time.time, time.sleep)

os.environ['TZ'] = 'US/Pacific'
time.tzset()
currentTime = time.localtime()

gpac_username = 'dford'
gpac_password = 'dford1234'

log_type = 'DATA'

start_year = '2015'
start_month = '08'
start_day = '00'
start_hour = '00'
start_min = '00'
start_sec = '00'

end_year = str(currentTime.tm_year)
end_month = str(currentTime.tm_mon)
end_day = str(currentTime.tm_mday)
end_hour = str(currentTime.tm_hour)
end_min = str(currentTime.tm_min)
end_sec = str(currentTime.tm_sec)

restart_flag = 1

loopCounter = 0

baseUri = "https://{0}.cloudant.com/{1}".format(username, db_name)

creds = (username, password)

# Read port selected by the cloud for our application
PORT = int(os.getenv('VCAP_APP_PORT', 8000))
# Change current directory to avoid exposure of control files
os.chdir('static')

httpd = Server(("", PORT), Handler)

def loadData():
	totalDocs = 0

	global start_year
	global start_month
	global start_day
	global start_hour
	global start_min
	global start_sec

	response = requests.put(
		baseUri,
		auth=creds
		)

	print "Created database at {0}".format(baseUri)

	if restart_flag:
		getCloudantDate()

	startTimeTuple = (int(start_year), int(start_month), int(start_day), int(start_hour), int(start_min), int(start_sec), 0, 0, -1)
	startTime = time.struct_time(startTimeTuple)
	print('this is the start time - '+str(time.asctime(startTime)))

	os.environ['TZ'] = 'US/Pacific'
	time.tzset()
	currentTime = time.localtime()
	print('this is the current time - '+str(time.asctime(currentTime)) )
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

	start_year = end_year
	start_month = end_month
	start_day = end_day
	start_hour = end_hour
	start_min = end_min
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

	lastDoc = {}

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
		lastDoc = jsonDocument

	# print docs
	print lastDoc

	print 'this is loop number'
	print loopCounter
	global loopCounter
	loopCounter += 1

	print 'total docs = '+str(totalDocs)

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

	postCloudantDate([end_year, end_month, end_day, end_hour, end_min, end_sec])

	scheduler.enter(600, 1, loadData, ())
	scheduler.run()

# pull start date from Cloudant if exists
def getCloudantDate():
	global start_year
	global start_month
	global start_day
	global start_hour
	global start_min
	global start_sec
	global restart_flag

	design_doc = requests.get(
		baseUri+'/_design/views',
		auth=creds
		)

	if '_id' in json.loads(design_doc.text):
		cloudant_date_doc = requests.get(
			baseUri+'/_design/views/_view/dateKeeper?descending=true&limit=1',
			auth=creds
			)
		cloudant_date = json.loads(cloudant_date_doc.text)["rows"][0]["key"]
		date_and_time = string.split(cloudant_date, " ")
		cDate = string.split(date_and_time[0], "-")
		cTime = string.split(date_and_time[1], ":")
		for i in range(0,3):
			if len(cDate[i])<2:
				cDate[i]='0'+cDate[i]
			if len(cTime[i])<2:
				cTime[i]='0'+cTime[i]
		start_year = cDate[0]
		print 'this is the start_year: '+start_year
		start_month = cDate[1]
		print 'this is the start_year: '+start_month
		start_day = cDate[2]
		print 'this is the start_year: '+start_day
		start_hour = cTime[0]
		print 'this is the start_year: '+start_hour
		start_min = cTime[1]
		print 'this is the start_year: '+start_min
		start_sec = cTime[2]
		print 'this is the start_year: '+start_sec

		print 'here is the date '+ start_year+start_month+start_day+start_hour+start_min+start_sec
	else:
		design_doc = requests.put(
			baseUri+'/_design/views',
			data=json.dumps({
				"_id": "_design/views",
				"views": {
				"parse_date": {
				"map": "function (doc) { if (doc.Timestamp) { emit(doc.Timestamp, doc); } }"
				},
				"dateKeeper": {
				"map": "function (doc) { if (doc.cloudant_date) { emit(doc.cloudant_date) } }"
				}
				}
				}),
			auth=creds,
			headers={"Content-Type": "application/json"}
			)

		cloudant_date_doc = requests.post(
			baseUri,
			data=json.dumps({
				"cloudant_date": 
				start_year+"-"+start_month+"-"+start_day+" "+start_hour+":"+start_min+":"+start_sec
				}),
			auth=creds,
			headers={"Content-Type": "application/json"}
			)
	restart_flag = 0

def postCloudantDate(end_list):
	for i in range(0,6):
		if len(end_list[i])<2:
			end_list[i]='0'+end_list[i]
	print end_list
	cloudant_date_doc = requests.post(
		baseUri,
		data=json.dumps({
			"cloudant_date": end_list[0]+"-"+end_list[1]+"-"+end_list[2]+" "+end_list[3]+":"+end_list[4]+":"+end_list[5]
			}),
		auth=creds,
		headers={"Content-Type": "application/json"}
		)

	print cloudant_date_doc.text

try:
  print("Start serving at port %i" % PORT)

  loadData()

  httpd.serve_forever()
except KeyboardInterrupt:
  pass
httpd.server_close()