import os
import requests
import json
import csv
import sched
import time
import string
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
db_name = ['bus_3', 'bus_4', 'bus_5']
port_num = ['sct.gpacsys.net', '166.184.185.1:40001', '166.184.185.159:40001']

scheduler = sched.scheduler(time.time, time.sleep)

os.environ['TZ'] = 'US/Pacific'
time.tzset()
currentTime = time.localtime()

gpac_username = 'benperl'
gpac_password = 'benperl1'

log_type = 'DATA'

start_year = '2015'
start_month = '10'
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

baseUri = "https://{0}.cloudant.com/{1}".format(username, db_name[0])

creds = (username, password)

# Read port selected by the cloud for our application
PORT = int(os.getenv('VCAP_APP_PORT', 8000))
# Change current directory to avoid exposure of control files
os.chdir('static')

httpd = Server(("", PORT), Handler)

def loadData(port_num_var, db_name_var):
	for pI in range(0, len(port_num_var)):

		totalDocs = 0

		global start_year
		global start_month
		global start_day
		global start_hour
		global start_min
		global start_sec

		baseUri = "https://{0}.cloudant.com/{1}".format(username, db_name_var[pI])

		response = requests.put(
			baseUri,
			auth=creds
			)
		
		cloudantDateTime = getCloudantDate(db_name_var[pI], port_num_var[pI])

		print 'Last run was at: '
		print cloudantDateTime

		startTimeTuple = (int(cloudantDateTime[0]), int(cloudantDateTime[1]), int(cloudantDateTime[2]), int(cloudantDateTime[3]), int(cloudantDateTime[4]), int(cloudantDateTime[5]), 0, 0, -1)
		startTime = time.struct_time(startTimeTuple)

		os.environ['TZ'] = 'US/Pacific'
		time.tzset()
		currentTime = time.localtime()

		end_year = str(currentTime.tm_year)
		end_month = str(currentTime.tm_mon)
		end_day = str(currentTime.tm_mday)
		end_hour = str(currentTime.tm_hour)
		end_min = str(currentTime.tm_min)
		end_sec = str(currentTime.tm_sec)

		gpac_url = 'http://'+port_num_var[pI]+'/query.php?username='+gpac_username+'&password='+gpac_password+'&logtype='+log_type+'&format=$2&start_year='+start_year+'&start_month='+start_month+'&start_day='+start_day+'&start_hour='+start_hour+'&start_min='+start_min+'&start_sec='+start_sec+'&end_year='+end_year+'&end_month='+end_month+'&end_day='+end_day+'&end_hour='+end_hour+'&end_min='+end_min+'&end_sec='+end_sec
		print gpac_url
		
		try:
			r = requests.get(gpac_url, timeout=5)
			online = 1
		except requests.exceptions.Timeout:
			print port_num_var[pI] + ' is not currently online'
			online = 0
		except requests.exceptions:
			print port_num_var[pI] + 'not currently online'
			online = 0

		if online:

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
				jsonDocument = {
				"bus_id":port_num_var[pI],
				"notify_flag":'false'
				}
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

			global loopCounter
			loopCounter += 1

			print 'This load added '+str(totalDocs)+' documents in '+db_name_var[pI]

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

			postCloudantDate([end_year, end_month, end_day, end_hour, end_min, end_sec], db_name_var[pI], port_num_var[pI])

# pull start date from Cloudant if exists
def getCloudantDate(db_name_date, port_num_date):
	global start_year
	global start_month
	global start_day
	global start_hour
	global start_min
	global start_sec
	global restart_flag

	baseUri = "https://{0}.cloudant.com/{1}".format(username, db_name_date)

	design_doc = requests.get(
		baseUri+'/_design/'+port_num_date+'_views',
		auth=creds
		)

	if '_id' in json.loads(design_doc.text):
		cloudant_date_doc = requests.get(
			baseUri+'/_design/'+port_num_date+'_views/_view/dateKeeper?descending=true&limit=1',
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
		start_month = cDate[1]
		start_day = cDate[2]
		start_hour = cTime[0]
		start_min = cTime[1]
		start_sec = cTime[2]

	else:
		design_doc = requests.put(
			baseUri+'/_design/'+port_num_date+'_views',
			data=json.dumps({
				"views": {
				"parse_date": {
				"map": "function (doc) { if (doc.Timestamp && doc.bus_id=='"+port_num_date+"') { emit(doc.Timestamp, doc); } }"
				},
				"dateKeeper": {
				"map": "function (doc) { if (doc.cloudant_date && doc.bus_id=='"+port_num_date+"') { emit(doc.cloudant_date) } }"
				},
			    "counts_by_day": {
			    "map": "function(doc) {\n var d = new Date(Date.parse(doc.Timestamp.split(' ')[0])); if(doc.Timestamp && doc.bus_id=='"+port_num_date+"') { emit([d.getFullYear(), d.getMonth()+1, d.getDate()], doc.Timestamp);}\n}",
			    "reduce": "_count"
			    }
			    },
				"indexes": {
				"by_device": {
				"analyzer": "standard",
				"index": "function(doc) { \n index('timestamp', doc.Timestamp, {'store':true}); \n    index ('device', doc.Device_Name, {'store':true}); \n   index ('value', doc.Value, {'store':true}); \n   index ('unit', doc.Unit, {'store':true}); \n   index ('bus_id', doc.bus_id, {'store':true});}"
				}
				}
				}),
			auth=creds,
			headers={"Content-Type": "application/json"}
			)

		start_year = '2015'
		start_month = '10'
		start_day = '00'
		start_hour = '00'
		start_min = '00'
		start_sec = '00'

		cloudant_date_doc = requests.post(
			baseUri,
			data=json.dumps({
				"cloudant_date": 
				start_year+"-"+start_month+"-"+start_day+" "+start_hour+":"+start_min+":"+start_sec,
				"bus_id":port_num_date
				}),
			auth=creds,
			headers={"Content-Type": "application/json"}
			)
	restart_flag = 0
	return (start_year, start_month, start_day, start_hour, start_min, start_sec)

def postCloudantDate(end_list, db_name_post, port_num_post):
	for i in range(0,6):
		if len(end_list[i])<2:
			end_list[i]='0'+end_list[i]

	baseUri = "https://{0}.cloudant.com/{1}".format(username, db_name_post)
	cloudant_date_doc = requests.post(
		baseUri,
		data=json.dumps({
			"cloudant_date": end_list[0]+"-"+end_list[1]+"-"+end_list[2]+" "+end_list[3]+":"+end_list[4]+":"+end_list[5],
			"bus_id":port_num_post
			}),
		auth=creds,
		headers={"Content-Type": "application/json"}
		)

try:
  print("Start serving at port %i" % PORT)
  loadData(port_num, db_name)
  while 1:
  	scheduler.enter(120, 1, loadData, (port_num, db_name))
  	scheduler.run()
  httpd.serve_forever()
  

except KeyboardInterrupt:
  pass
httpd.server_close()

