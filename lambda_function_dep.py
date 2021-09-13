#!/usr/bin/env python3
import boto3
import csv
import datetime
import mysql.connector
import os
import re
import s3fs
import sys
import time
import uuid
from botocore.exceptions import ClientError
from mysql.connector import errorcode

AWS_REGION = os.environ['region']
importRoleArn = os.environ['importRoleArn']
projectId = os.environ['projectId']
s3 = s3fs.S3FileSystem(anon = False)
db_host = os.environ['db_host']
db_database = os.environ['db_database']
db_username = os.environ['db_user']
db_password = os.environ['db_pass']
practice = os.environ['Practice']
plan = int(os.environ['PlanID'])
# age_min = int(os.environ['age_min'])
template_sms = os.environ['TemplateSMS']
template_email = os.environ['TemplateEmail']
# template_sms_oops = os.environ['TemplateSMSOops']
# template_email_oops = os.environ['TemplateEmailOops']
seconds = int(os.environ['seconds'])

date_today = datetime.datetime.today()
if "age_min" in os.environ:
	age_min = int(os.environ['age_min'])
elif "age_min" not in os.environ:
	age_min = 0

# supported file formats:
formats = ['.csv']

# don't know if thesea are needed, but variables for later...
records = []
records_error = []
patients = []
visits = []
locations = []
messages = []
reports = []
count_messages = 0
skip_age = 0
skip_expired = 0
skip_out = 0
skip_invalid = 0
skip_old = 0

# fieldnames for the segment CSV
fieldnames = [
	'ChannelType',
	'Address',
	'Id',
	'User.UserAttributes.PracticeName',
	'User.UserAttributes.PatientName',
	'Location.Country',
	'User.UserAttributes.Age',
	'User.UserAttributes.DateOfService',
	'User.UserAttributes.ServicingProvider',
	'User.UserAttributes.LocationName',
	'User.UserAttributes.VisitNumber',
	'User.UserAttributes.PostDate',
	'User.UserAttributes.DateofDeath',
	'User.UserAttributes.MessageID',
	'User.UserAttributes.SurveyLink'
]

def connect_db(database_name):
	try:
		cnx = mysql.connector.connect(
			user = db_username,
			password = db_password,
			host = db_host,
			database = database_name
		)
	except mysql.connector.Error as error:
		print(f'Failed to connect to database. Error was: {error}.')
		return
	else:
		print('Succesfully connected to DB.')
	return cnx

def mysql_query(
	db_cnx,
	qry,
	commit = False
):
	'''
	Function to query a MySQL database and return results.
	Takes a MySQL database connection object and a query string as input and returns a list of results.
	Query can be a SELECT, INSERT, or UPDATE statement.

	** In theory ** a query string could be lots of other acceptable SQL commands, but this function was built for SELECT, INSERT, or UPDATE.
	'''
	try:
		db_cursor = db_cnx.cursor(buffered = True)
	except mysql.connector.Error as error:
		print(f'Failed to open a cursor to the database. The error was: {error}.')
	try:
		if commit:
			db_cursor.execute(qry)
			# print(f"attempted to: {db_cursor.statement} into DB")
			db_cnx.commit()
			return
		else:
			db_cursor.execute(qry)
			# print(f"attempted to: {db_cursor.statement} from DB")
			results = db_cursor.fetchall()
			# print(f"DB results: {results}")
			return results
	except mysql.connector.Error as error:
		print(f'There was an error with the following query: {db_cursor.statement}. The error was {error}.')
	db_cursor.close()
	return

def errorout(
	method = None,
	object = None,
	destination = None
):
	if method == 'file':
		s3.move(object, destination)
		print(f'[ERROR] {object} moved to error folder ({destination}).')
	state_error = True
	return state_error

def check_phone_number(country, phone, region, skip = False):
	'''
	Phone number validation can generate an E.164-compliant phone number as
	long as you provide it with the correct country code. This function looks
	for the appropriate country code at the beginning of the phone number. If
	the country code isn't present, it adds it to the beginning of the phone
	number that was provided to the function, and then sends it to the phone
	number validation service. The phone number validation service performs
	additional cleansing of the phone number, removing things like
	unnecessary leading digits. It also provides metadata, such as the phone
	number type (mobile, landline, etc.).
	Add more countries and regions to this function if necessary.
	'''
	if skip:
		phone =  re.sub("[^0-9]", "", phone)
		phone = "+1" + phone
		phoneType = 1
		return phone, phoneType
	client = boto3.client('pinpoint',region_name=region)
	phone =  re.sub("[^0-9]", "", phone)
	if (country == 'BD') and not phone.startswith('880'):
		phone = "+880" + phone
	elif (country == 'BR') and not phone.startswith('55'):
		phone = "+55" + phone
	elif (country == 'CA' or country == 'US') and not phone.startswith('1'):
		# US and Canada (country code 1) area codes and phone numbers can't use
		# 1 as their first digit, so it's fine to search for a 1 at the
		# beginning of the phone number to determine whether or not the number
		# contains a country code.
		phone = "+1" + phone
	elif (country == 'CN') and not phone.startswith('86'):
		phone = "+86" + phone
	elif (country == 'IN') and not phone.startswith('91'):
		phone = "+91" + phone
	elif (country == 'ID') and not phone.startswith('62'):
		phone = "+62" + phone
	elif (country == 'IE') and not phone.startswith('353'):
		phone = "+353" + phone
	elif (country == 'JP') and not phone.startswith('81'):
		phone = "+81" + phone
	elif (country == 'MX') and not phone.startswith('52'):
		phone = "+52" + phone
	elif (country == 'NG') and not phone.startswith('234'):
		phone = "+234" + phone
	elif (country == 'PK') and not phone.startswith('92'):
		phone = "+92" + phone
	elif (country == 'RU') and not phone.startswith('7'):
		# No area codes in Russia begin with 7. However, Kazakhstan also uses
		# country code 7, and Kazakh area codes can begin with 7. If your
		# contact database contains Kazakh phone numbers, you might have to
		# use some additional logic to identify them.
		phone = "+7" + phone
	elif (country == 'GB') and not phone.startswith('44'):
		phone = "+44" + phone
	try:
		response = client.phone_number_validate(
			NumberValidateRequest={
				'IsoCountryCode': country,
				'PhoneNumber': phone
			}
		)
	except ClientError as e:
		print(e.response)
	else:
		returnValues = [
			response['NumberValidateResponse']['CleansedPhoneNumberE164'],  response['NumberValidateResponse']['PhoneType']
		]
		# print(f'phone values are: {returnValues}')
		return returnValues
	return

def check_valid_phone(number):
	'''
	Check if the provided phone number is valid (begins with +1, is the expected length).  Based on the assumption that the number is a US number.
	'''

	return False

def check_valid_file(filename):
	if os.path.splitext(os.path.basename(filename))[1] in formats:
		return True
	return False

def check_patient_db(db_cnx, PatientID):
	'''
	Lookup a patient in the database by patient ID.

	Returns True or False.
	'''
	patients = mysql_query(db_cnx, (f"SELECT PatientID FROM patients WHERE PatientID = {PatientID} LIMIT 1"))
	if not patients:
		return False
	for patient in patients:
		if int(PatientID) == patient[0]:
			return True
	return False

def check_visit_db(db_cnx, VisitID):
	'''
	Check if the VisitNumber provided from the client is in use in the database already.

	Returns True or False.
	'''
	visits = mysql_query(db_cnx, (f"SELECT VisitID FROM visits WHERE VisitID = '{VisitID}'"))
	if not visits:
		return False
	for visit in visits:
		if VisitID == visit[0]:
			return True
	return False

def check_location_db(db_cnx, location):
	'''
	Check if the Location provided is in use in the database already.

	Returns True or False.
	'''
	if not location:
		return False
	if '(' in location:
		locationID = location.partition(" ")[0].replace("(", "").replace(")", "")
		results = mysql_query(db_cnx, f"SELECT LocationID FROM locations WHERE LocationID = {locationID} LIMIT 1")
		for result in results:
			if int(locationID) == int(result[0]):
				return True
	else:
		results = mysql_query(db_cnx, f"SELECT LocationID, LocationName FROM locations WHERE LocationName = '{location}' LIMIT 1")[0][1]
		if location == results:
			return True
	# print('new location')
	return False

def check_message_db(
	db_cnx,
	VisitID,
	type = 0,
	reason = None
):
	'''
	Check if an initial message for this visit exists in the database already.

	Returns True or False.
	'''
	# print(f'checking to see if a message has been sent for VisitID {VisitID}, Type {type}, Reason {reason}')
	if not reason:
		# print('no reason given for lookup')
		messages = mysql_query(db_cnx, (f"SELECT VisitID FROM messages WHERE VisitID = '{VisitID}' and TypeID = {type} LIMIT 1"))
	elif reason:
		# print('got a reason')
		messages = mysql_query(db_cnx, (f"SELECT VisitID FROM messages WHERE VisitID = '{VisitID}' and TypeID = {type} and ReasonID = {reason} LIMIT 1"))
	# print(f'got {messages}')
	for message in messages:
		if VisitID == message[0]:
			return True
	return False

def check_landline(db_cnx, PatientID):
	'''
	Check if the patient has a landline phone.  Patient cannot recieve SMS if they have provided a landline phone.

	Returns True or False.
	'''
	types = mysql_query(db_cnx, (f"SELECT PhoneType FROM patients WHERE PatientID = {PatientID} LIMIT 1"))
	for type in types:
		if type[0] == 2:
			return True
	return False

def check_date_service(
	db_cnx,
	date,
	history = False
):
	'''
	Check if the date of service is greatar than 30 days.

	Returns True or False.
	'''
	if history:
		if practice == 'MAG':
			date_started = '2021-04-03'
		elif practice == 'OCCPM':
			date_started = 	'2021-04-15'
		elif practice == 'DEMO':
			date_started = '2021-04-01'
		if datetime.datetime.strptime(date, '%Y-%m-%d') < datetime.datetime.strptime(date_started, '%Y-%m-%d'):
			return True
	else:
		if (date_today - datetime.timedelta(30)) > datetime.datetime.strptime(date, '%Y-%m-%d'):
			return True
	return False

def check_opt_out(db_cnx, PatientID):
	'''
	Check if the patient has opted out of messaging.

	Returns True or False.
	'''
	opts = mysql_query(db_cnx, (f"SELECT OptOut FROM patients WHERE PatientID = {PatientID} LIMIT 1"))
	for opt in opts:
		if opt[0] == 1:
			return True
	return False

def get_provider_id(db_cnx, Provider):
	'''
	Lookup a provider in the database by first and last name and return their ID.  If the provider does not exist, it will return a default provider: ID = 1.
	'''
	providername = name_separate(Provider, 'Provider')
	providers = mysql_query(db_cnx, (f"SELECT ProviderID FROM providers WHERE ProviderLast = '{providername[0]}' AND ProviderFirst = '{providername[1]}' LIMIT 1"))
	# print(f"provider is: {providername}; found {providers}")
	if not providers:
		return 1
	return providers[0][0]

def get_survey_link(db_cnx, Provider):
	'''
	Lookup a provider's base survey URL and return it.
	'''
	providerid = get_provider_id(db_cnx, Provider)
	urls = mysql_query(db_cnx, (f"SELECT SurveyURL FROM providers WHERE ProviderID = '{get_provider_id(db_cnx, Provider)}' LIMIT 1"))
	for url in urls:
		url = url[0]
	return url

def get_phone_db(db_cnx, PatientID):
	'''
	Lookup a patient's phone number in the DB.
	'''
	numbers = mysql_query(db_cnx, (f"SELECT Phone FROM patients WHERE PatientID = {PatientID} LIMIT 1"))
	for number in numbers:
		if number:
			patientPhone = number[0]
			return patientPhone
	patientPhone = ''
	return patientPhone

def get_email_db(db_cnx, PatientID):
	'''
	Lookup a patient's email addresss in the DB.
	'''
	emails = mysql_query(db_cnx, (f"SELECT Email FROM patients WHERE PatientID = {PatientID} LIMIT 1"))
	for email in emails:
		if email:
			patientEmail = email[0]
			return patientEmail
	patientEmail = ''
	return patientEmail

def convert_sql_datetime(
	convert,
	time = False
):
	if time:
		# change this:
		time = datetime.datetime.strftime(datetime.datetime.strptime(convert, '%m/%d/%Y'), '%Y-%m-%d')
		return time
	date = datetime.datetime.strftime(datetime.datetime.strptime(convert, '%Y-%m-%d'), '%Y-%m-%d')
	return date

def name_separate(fullname, source):
	last = 'ERROR'
	first = 'ERROR'
	middle = 'ERROR'
	if source == 'Provider':
		half1 = fullname.split(',')[0]
		half2 = fullname.split(',')[1].strip()
		#print(f"half2 is {half2}")
		last = half1
		first = half2.split(' ')[0]
		#print(f"first is {first}?")
		if len(half2.split(' ')) > 1:
			#print(f"split half2 {half2.split(' ')}")
			middle = half2.split(' ')[1]
		if len(half2.split(' ')) < 2:
			middle = ''
		# print(f"name is last: {last}, {first} {middle}")
		return last, first, middle
	if source == 'Patient':
		# print(f'trying to separate {fullname}')
		half1 = fullname.split(',')[0]
		half2 = fullname.split(',')[1]
		# print(f"half2 is {half2}")
		last = half1
		# print(f"split half2 {half2.split(' ')}")
		middle = half2.split(' ')[1]
		first = half2.split(' ')[2]
		# print(f"name is last: {last}, first: {first} middle: {middle}")
		return last, first, middle
	return last, first, middle

def insert_patients(db_cnx, patients):
	for patient in patients:
		# break fullname into parts
		patientname = name_separate(patient['PatientName'], 'Patient')
		if patientname:
			patientLast = patientname[0]
			patientFirst = patientname[1]
		elif not patientname:
			patientname = ''
		if len(patientname) > 2:
			patientMiddle = patientname[2]
		# convert blanks to nulls
		if patient['Age'] == '':
			patientAge = None
		if patient['Email'] == '':
			patientEmail = None
		# get phone information and convert to E.164 format
		if patient['Phone']:
			phoneInfo = check_phone_number('US', patient['Phone'], AWS_REGION)
			patientPhone = phoneInfo[0]
			if phoneInfo[1] == 'VOICE':
				patientPhoneType = 2
			elif phoneInfo[1] == 'MOBILE':
				patientPhoneType = 1
			elif phoneInfo[1] == 'PREPAID':
				patientPhoneType = 1
			elif phoneInfo[1] == 'VOIP':
				patientPhoneType = 1
			elif phoneInfo[1] == 'INVALID':
				patientPhoneType = 0
			else:
				patientPhoneType = 0
		elif not patient['Phone']:
			patientPhone = ''
			patientPhoneType = 0
		else:
			patientPhone = ''
			patientPhoneType = 0

		patientEmail = patient['Email']
		if patient['DateOfDeath']:
			patientDOD = convert_sql_datetime(patient['DateOfDeath'])
		else:
			patientDOD = None
		if patientDOD:
			mysql_query(
				db_cnx,
				f"INSERT INTO patients (PatientID, PatientLast, PatientFirst, PatientMiddle, Imported, Age, Death, Phone, PhoneType, Email) VALUES ('{patient['PatientID']}', '{patientLast}', '{patientFirst}', '{patientMiddle}', '{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}', {int(patient['Age'])}, '{patientDOD}', '{patientPhone}', {patientPhoneType}, '{patientEmail}')",
				commit = True
			)
		else:
			print(f"going to: INSERT INTO patients (PatientID, PatientLast, PatientFirst, PatientMiddle, Imported, Age, Death, Phone, PhoneType, Email) VALUES ('{patient['PatientID']}', '{patientLast}', '{patientFirst}', '{patientMiddle}', '{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}', {int(patient['Age'])}, '{patientDOD}', '{patientPhone}', {patientPhoneType}, '{patientEmail}')")
			mysql_query(
				db_cnx,
				f"INSERT INTO patients (PatientID, PatientLast, PatientFirst, PatientMiddle, Imported, Age, Phone, PhoneType, Email) VALUES ('{patient['PatientID']}', '{patientLast}', '{patientFirst}', '{patientMiddle}', '{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}', {int(patient['Age'])}, '{patientPhone}', {patientPhoneType}, '{patientEmail}')",
				commit = True
			)
	return True

def insert_visits(db_cnx, visits):
	for visit in visits:
		# convert blanks to nulls
		if visit['DateOfService'] == '':
			dateOfService = None
		else:
			dateOfService = visit['DateOfService']
		if visit['PostDate'] == '':
			postDate = None
		else:
			postDate = visit['PostDate']
		# convert the m/d/Y DOD format to Y-m-d format
		if dateOfService is not None:
			dateOfService = convert_sql_datetime(visit['DateOfService'])
		if postDate is not None:
			postDate = convert_sql_datetime(visit['PostDate'])
		locationID = visit['LocationName'].partition(" ")[0].replace("(", "").replace(")", "")
		providerID = get_provider_id(db_cnx, visit['ServicingProvider'])
		mysql_query(
			db_cnx,
			f"INSERT INTO visits (VisitID, PatientID, LocationID, ProviderID, DateOfService, DatePosted, VisitNumber) VALUES ('{visit['SurveyRequestID']}', '{visit['PatientID']}', '{locationID}', '{providerID}', '{dateOfService}', '{postDate}', '{visit['VisitNumber']}')",
			commit = True
		)
	return True

def insert_locations(db_cnx, locations):
	for location in locations:
			if not location:
				print('location is blank')
				mysql_query(
					db_cnx,
					f"INSERT INTO locations (LocationName) VALUES ('')",
					commit = True
				)
			if '(' in location['LocationName']:
				locationID = location['LocationName'].partition(" ")[0].replace("(", "").replace(")", "")
				mysql_query(
					db_cnx,
					f"INSERT IGNORE INTO locations (LocationID, LocationName) VALUES ({locationID}, '{location['LocationName'].partition(' ')[2]}')",
					commit = True
				)
			else:
				mysql_query(
					db_cnx,
					f"INSERT IGNORE INTO locations (LocationName) VALUES ('{location['LocationName']}')",
					commit = True
				)
	return True

def insert_messages(db_cnx, messages, history = False):
	for message in messages:
		# print(message)
		if history:
			if 'Address' in message:
				mysql_query(
					db_cnx,
					f"INSERT INTO messages (DTGSent, TypeID, VisitID, ReasonID, SurveyLink, Address, Comments) VALUES ('{message['DTGSent']}', {message['TypeID']}, '{message['SurveyRequestID']}', {message['ReasonID']}, '{message['SurveyLink']}', '{message['Address']}', '{message['Comment']}')",
					commit = True
				)
			else:
				mysql_query(
					db_cnx,
					f"INSERT INTO messages (DTGSent, TypeID, VisitID, ReasonID, Comments) VALUES ('{message['DTGSent']}', {message['TypeID']}, '{message['SurveyRequestID']}', {message['ReasonID']}, '{message['Comment']}')",
					commit = True
				)
			continue
		if 'Address' in message:
			mysql_query(
				db_cnx,
				f"INSERT INTO messages (TypeID, VisitID, DTGSent, ReasonID, SurveyLink, Address, Comments) VALUES ({message['TypeID']}, '{message['SurveyRequestID']}', '{message['DTGSent']}', {message['ReasonID']}, '{message['SurveyLink']}', '{message['Address']}', '{message['Comment']}')",
				commit = True
			)
		else:
			mysql_query(
				db_cnx,
				f"INSERT INTO messages (TypeID, VisitID, DTGSent, ReasonID, Comments) VALUES ({message['TypeID']}, '{message['SurveyRequestID']}', '{message['DTGSent']}', {message['ReasonID']}, '{message['Comment']}')",
				commit = True
			)
	# print(f"Added {len(messages)} messages to DB.")
	return True

def import_new(rec):
	state_error = False
	records = []
	patients = []
	visits = []
	locations = []
	messages = []
	messages_send = []
	messages_no_send = []
	messages_errors = []
	reports = []
	count_messages = 0
	skip_age = 0
	skip_expired = 0
	skip_out = 0
	skip_invalid = 0
	skip_old = 0
	while not state_error:
		try:
			db_cnx = connect_db(db_database)
			bucket = rec['s3']['bucket']['name']
			file_in = rec['s3']['object']['key']
			file_error = os.path.splitext(os.path.basename(file_in))[0] + '-error.csv'
			file_errors = os.path.splitext(os.path.basename(file_in))[0] + '-errors.csv'
			file_out = os.path.splitext(os.path.basename(file_in))[0] + '-segment.csv'
			file_processed = os.path.splitext(os.path.basename(file_in))[0] + '-processed.csv'
			file_report = os.path.splitext(os.path.basename(file_in))[0] + '-report.csv'
			segment_name = practice + '-' + str(datetime.datetime.now().strftime('%Y-%m-%d_%H%M'))
			if not check_valid_file(file_in):
				state_error = errorout(
					method = 'file',
					object = 's3://' + bucket + file_in,
					destination = 's3://' + bucket + '/error/' + file_error
				)
				break
			with s3.open(
				os.path.join(
					bucket,
					file_in
				),
				 'r'
			) as csvfile:
				reader = csv.DictReader(csvfile)
				header = reader.fieldnames
				for line in reader:
					records.append(line)
			print(f'Processing received file: {file_in}.  Assuming provider is {practice}.  There are {len(records)} visits in this file.')
			# print(f'plan is {plan}')
			# check if the file contains records from the right practice
			if practice not in records[0]['PracticeName']:
				print(f"[ERROR] Got {records[0]['PracticeName']} but expected {practice}.")
				state_error = errorout(
					method = 'file',
					object = os.path.join(
						bucket,
						file_in
					),
					destination = os.path.join(
						bucket,
						'error', os.path.splitext(os.path.basename(file_in))[0],
						'-error.csv'
					)
				)
				break
			print('Checking for new patients, visits, and locations.')
			for record in records:
				# check for some common errors:
				if not record['SurveyRequestID'] or not record['PatientID'] or not record['PatientName'] or not record['ServicingProvider'] or not record['DateOfService'] or not record['PostDate'] or not record['LocationName']:
					print(f'A critical field was blank.  Skipping this record.  Error records will be saved to {bucket}/error/{file_error}')
					messages_errors.append(record)
					continue
				if not check_patient_db(db_cnx, record['PatientID']):
					patients.append(record)
				if not check_visit_db(db_cnx, record['SurveyRequestID']):
					visits.append(record)
				if not check_location_db(db_cnx,  record['LocationName']):
					locations.append(record)
			if patients:
				# print('inserting patients')
				insert_patients(db_cnx, patients)
				print(f'Added {len(patients)} new patients to the DB.')
			if locations :
				insert_locations(db_cnx, locations)
				print(f'Added {len(locations)} new locations to the DB.')
			if visits:
				insert_visits(db_cnx, visits)
				print(f'Added {len(visits)} new visits to the DB.')
			# Add pending messages to the DB, but only if a message for that visit doesn't already exist
			# this probably isn't the most efficient way to do it, but iterate over the list again to add the messages
			print(f"Comparing visits in {file_in} to messages in db.")
			for record in records:
				#try
				email = {}
				# regardless of plan type, check if: 1) valid service date, 2) meet age criteria, 3) not expired
				if check_date_service(db_cnx, record['DateOfService']):
					if not check_message_db(db_cnx, record['SurveyRequestID'], type = 0, reason = 10):
						record['ReasonID'] = 10
						record['Comment'] = f'A message will not be sent for this visit, because the date of service is over 30 days.'
						record['TypeID'] = 0
						record['DTGSent'] = datetime.datetime.today()
						# print('adding message to no send list')
						messages_no_send.append(record)
						skip_old += 1
						continue
					else:
						messages_errors.append(record)
					continue
				# check if patient is old enough
				if int(record['Age']) < age_min:
					if not check_message_db(db_cnx, record['SurveyRequestID'], type = 0, reason = 3):
						record['ReasonID'] = 3
						record['Comment'] = f'A message will not be sent for this visit becuase the patient is under the minimum age of {age_min}.'
						record['TypeID'] = 0
						record['DTGSent'] = datetime.datetime.today()
						# print('adding message to no send list')
						messages_no_send.append(record)
						skip_age =+ 1
						continue
					else:
						messages_errors.append(record)
					continue
				# check if the patient is expired
				if record['DateOfDeath']:
					# print(f"patient {record['PatientName']} has expired")
					if not check_message_db(db_cnx, record['SurveyRequestID'], type = 0, reason = 4):
						record['ReasonID'] = 4
						record['Comment'] = f"A message will not be sent for this visit because the patient expired on {record['DateOfDeath']}."
						record['TypeID'] = 0
						record['DTGSent'] = datetime.datetime.today()
						# print('adding message to no send list')
						messages_no_send.append(record)
						skip_expired =+ 1
						continue
					else:
						messages_errors.append(record)
					continue
				# check if the patient has opted out
				if check_opt_out(db_cnx, record['PatientID']):
					if not check_message_db(db_cnx, record['SurveyRequestID'], type = 0, reason = 8):
						record['ReasonID'] = 8
						record['Comment'] = f'A message will not be sent for this visit because the patient opted out of messaging.'
						record['TypeID'] = 0
						record['DTGSent'] = datetime.datetime.today()
						# print('adding message to no send list')
						messages_no_send.append(record)
						skip_out =+ 1
						continue
					else:
						messages_errors.append(record)
					continue
				# plan 2 is the only one that wont' send a text...
				if plan == 1 or plan == 3:
					# print(f'plan is {plan}')
					record['PatientPhone'] = get_phone_db(db_cnx, record['PatientID'])
					# check if a text message isn't already pending
					# check if we can text the phone...
					if not check_landline(db_cnx, record['PatientID']) and len(record['PatientPhone']) != 12:
						if check_message_db(db_cnx, record['SurveyRequestID'], type = 0, reason = 7):
							continue
						record['ReasonID'] = 7
						record['Comment'] = f'A message will not be sent for this visit, because the phone is invalid.'
						record['TypeID'] = 0
						record['DTGSent'] = datetime.datetime.today()
						# print('adding message to no send list')
						messages_no_send.append(record)
						skip_invalid += 1
						continue
					if not check_message_db(db_cnx, record['SurveyRequestID'], type = 2):
						record['SurveyLink'] = get_survey_link(db_cnx, record['ServicingProvider']) + '?id=' + record['SurveyRequestID']
						record['Address'] = get_phone_db(db_cnx, record['PatientID'])
						record['ReasonID'] = 1
						record['Comment'] = f'Initial entry.  Added {date_today}.'
						record['TypeID'] = 2
						record['DTGSent'] = datetime.datetime.today()
						# print('adding message to send list')
						messages_send.append(record)
						if plan == 1:
							continue
					else:
						messages_errors.append(record)
					# 	print(f"A message already exists in the DB for visit {record['SurveyRequestID']}. Skipping.")
				# if the plan is three, patients will get a text and an email
				if plan == 2 or plan == 3:
					# print(f'plan is {plan}')
					# check if an email isn't already pending
					if not check_message_db(db_cnx, record['SurveyRequestID'], type = 3):
						# print(f'record for email is {record}')
						email['SurveyRequestID'] = record['SurveyRequestID']
						email['SurveyLink'] = get_survey_link(db_cnx, record['ServicingProvider']) + '?id=' + record['SurveyRequestID']
						email['Address'] = get_email_db(db_cnx, record['PatientID'])
						email['ReasonID'] = 1
						email['Comment'] = f'Initial entry.  Added {date_today}.'
						email['TypeID'] = 3
						email['DTGSent'] = datetime.datetime.today()
						# print('adding message to send list')
						messages_send.append(email)
					else:
						messages_errors.append(email)
						# print(f"A message already exists in the DB for visit {record['SurveyRequestID']}. Skipping.")
			if messages_errors:
				with s3.open(
					's3://' + bucket + '/error/' + file_errors,
					'w',
					newline = '',
					encoding = 'utf-8'
				) as out_file:
					writer_csv = csv.DictWriter(
						out_file,
						fieldnames = [
							'SurveyRequestID',
							'UUID',
							'PracticeName',
							'PatientID',
							'PatientName',
							'Age',
							'Phone',
							'PatientPhone',
							'Email',
							'patientEmail',
							'DateOfService',
							'ProviderTitle',
							'ServicingProvider',
							'LocationName',
							'VisitNumber',
							'PostDate',
							'DateOfDeath',
							'SurveyLink',
							'Address',
							'ReasonID',
							'TypeID',
							'DTGSent',
							'Comment'
						]
					)
					writer_csv.writeheader()
					for message in messages_errors:
						writer_csv.writerow(message)
					print(f'[WARNING] There were errors with some records.  Those records have been saved to: {bucket}/error/{file_error}')
			# update DB with message status
			messages = messages_send + messages_no_send
			print(f'Updating {db_database} DB with {len(messages)} messages')
			insert_messages(db_cnx, messages)
			s3.move(
				's3://' + bucket + '/' + file_in,
				's3://' + bucket + '/processed/' + file_processed
				)
			print(f"[SUCCESS] Processed {file_in} and moved to processed/{os.path.splitext(os.path.basename(file_in))[0]}-processed.csv")
			# messages = mysql_query(db_cnx, "SELECT * FROM viewMessagesPending")
			print(f'Processing {len(messages_send)} pending messages into Pinpoint.')
			with s3.open(
				's3://' + bucket + '/segment/' + file_out,
				'w',
				newline = '',
				encoding = 'utf-8'
			) as out_file:
				writer_csv = csv.DictWriter(
					out_file,
					fieldnames = fieldnames
				)
				writer_csv.writeheader()
				for message in messages_send:
					# print(f'message is {message}')
					pend = {}
					# print(f"getting pending messages for {message['SurveyRequestID']} type {message['TypeID']}")
					pending = mysql_query(db_cnx, (f"SELECT * FROM viewMessagesPending WHERE VisitID = '{message['SurveyRequestID']}' AND TypeID = {message['TypeID']} LIMIT 1"))[0]
					pend['TypeID'] = pending[2]
					pend['Address'] = pending[9]
					pend['Patient'] = pending[6]
					pend['Age'] = pending[12]
					pend['DateOfService'] = pending[4]
					pend['Provider'] = pending[7]
					pend['Location'] = pending[14]
					pend['SurveyRequestID'] = pending[1]
					pend['PostDate'] = pending[12]
					pend['DateOfDeath'] = pending[13]
					pend['MessageID'] = pending[0]
					pend['SurveyLink'] = pending[8]
					# print(f'pending is {pending}')
					if pend['TypeID'] == 3:
						channelType = 'EMAIL'
					else:
						channelType = 'SMS'
					writer_csv.writerow({
						'ChannelType': channelType,
						'Address': pend['Address'],
						'Id': str(uuid.uuid4()),
						'User.UserAttributes.PracticeName': practice,
						'User.UserAttributes.PatientName': pend['Patient'],
						'Location.Country': "US",
						'User.UserAttributes.Age': pend['Age'],
						'User.UserAttributes.DateOfService': pend['DateOfService'],
						'User.UserAttributes.ServicingProvider': pend['Provider'],
						'User.UserAttributes.LocationName': pend['Location'],
						'User.UserAttributes.VisitNumber': pend['SurveyRequestID'],
						'User.UserAttributes.PostDate': pend['PostDate'],
						'User.UserAttributes.DateofDeath': pend['DateOfDeath'],
						'User.UserAttributes.MessageID': pend['MessageID'],
						'User.UserAttributes.SurveyLink': pend['SurveyLink'],
					})
			if len(messages_send) > 0:
				print(f"[SUCCESS] Processed {len(messages_send)} messages into a file for import into Pinpoint.  {skip_age} patients were underage, {skip_expired} patients were expired, {skip_out} patients opted out, and {skip_invalid} patients had invalid phone numbers. Scheduling {bucket}/{file_out} for import into a Pinpoint segment.")
				client = boto3.client('pinpoint')
				try:
					response = client.create_import_job(
						ApplicationId = projectId,
						ImportJobRequest = {
							'DefineSegment': True,
							'Format': 'CSV',
							'RegisterEndpoints': True,
							'RoleArn': importRoleArn,
							'S3Url': 's3://' + bucket + '/segment/' + file_out,
							'SegmentName': segment_name
						}
					)
					segment_id = response['ImportJobResponse']['Definition']['SegmentId']
				except ClientError as e:
					print("Error: " + e.response['Error']['Message'])
					continue
				else:
					print("Import job " + response['ImportJobResponse']['Id'] + " " + response['ImportJobResponse']['JobStatus'] + ".")
					print("Segment ID: " + response['ImportJobResponse']['Definition']['SegmentId'])
					print("Application ID: " + projectId)
				print(f'Updating {db_database} database with segment schedule.')
				with s3.open(
					's3://' + bucket + '/segment/' + file_out,
					'r',
					newline = '',
					encoding = 'utf-8'
				) as file_segment:
					reader_csv = csv.DictReader(
						file_segment,
						fieldnames = fieldnames
					)
					for row in reader_csv:
						if row['Id'] == 'Id':
							continue
						reason = 9
						comment = f'Sent to Pinpoint to be imported into segment {segment_name}.'
						mysql_query(
							db_cnx,
							f"UPDATE messages SET ReasonID = {reason}, Comments = '{comment}' WHERE MessageID = {row['User.UserAttributes.MessageID']}",
							commit = True
						)
				print(f'Create Pinpoint campaign from segment {segment_name} (id: {segment_id}), using template {template_sms} and {template_email}.  Pausing for {seconds} seconds so Pinpoint has time to process import job.')
				time.sleep(seconds)
				client = boto3.client('pinpoint')
				try:
					response = client.create_campaign(
						ApplicationId = projectId,
						WriteCampaignRequest = {
							'Description': 'Campaign created to send Clearsurvey surveys to patients after their visit to a provider.',
							'AdditionalTreatments': [],
							'IsPaused': False,
							'Schedule': {
								'Frequency': 'ONCE',
								'IsLocalTime': False,
								'StartTime': 'IMMEDIATE',
								'Timezone': 'UTC',
								'QuietTime': {}
							},
							'TemplateConfiguration': {
								'EmailTemplate': {
									'Name': template_email
								},
								'SMSTemplate': {
									'Name': template_sms
								}
							},
							'Name': segment_name,
							'SegmentId': segment_id,
							'SegmentVersion': 1,
						}
					)
				except ClientError as e:
					 print(e.response['Error']['Message'])
					 state_error = True
					 break
				else:
					print(response)
				print(f'Updating {db_database} database with campaign created.')
				with s3.open(
					's3://' + bucket + '/segment/' + file_out,
					'r',
					newline = '',
					encoding = 'utf-8'
				) as file_campaign:
					reader_csv = csv.DictReader(
						file_campaign,
						fieldnames = fieldnames
					)
					for row in reader_csv:
						if row['Id'] == 'Id':
							continue
						reason = 2
						comment = f'Campaign created in pinpoint.'
						mysql_query(
							db_cnx,
							f"UPDATE messages SET Sent = 1, ReasonID = {reason}, Comments = '{comment}', DTGSent = '{datetime.datetime.now():%Y-%m-%d %H:%M:%S}' WHERE MessageID = {row['User.UserAttributes.MessageID']}",
							commit = True
						)
			else:
				print(f'[WARNING] No messages to process into Pinpoint.  Check error file.')
			print(f'Creating report')
			if messages_send:
				with s3.open(
					's3://' + bucket + '/reports/' + file_report,
					'w',
					newline = '',
					encoding = 'utf-8'
				) as out_file:
					header.extend(['SurveyNotSentReason', 'SurveySentDateTime'])
					writer_csv = csv.writer(out_file)
					writer_csv.writerow(['SurveyRequestID', 'SurveyNotSentReason', 'SurveySentDateTime'])
					for record in records:
						result = mysql_query(db_cnx, (f"SELECT Sent, MessageID, ReasonID, DTGSent FROM messages WHERE VisitID = '{record['SurveyRequestID']}' LIMIT 1"))[0]
						if result[0] == 1:
							record['SurveySentDateTime'] = result[3].strftime('%Y-%m-%d %h:%m:%d')
						else:
							record['SurveySentDateTime'] = ''
						if result[2] == 2:
							record['SurveyNotSentReason'] = ''
						elif result[2] == 3:
							record['SurveyNotSentReason'] = 'Patient underage'
						elif result[2] == 4:
							record['SurveyNotSentReason'] = 'Patient expired'
						elif result[2] == 7:
							record['SurveyNotSentReason'] = 'Patient phone invalid'
						elif result[2] == 8:
							record['SurveyNotSentReason'] = 'Patient opted out'
						elif result[2] == 10:
							record['SurveyNotSentReason'] = 'Date of service > 30 days'
						else:
							record['SurveyNotSentReason'] = 'Unknown'
						writer_csv.writerow([
							record['SurveyRequestID'],
							record['SurveyNotSentReason'],
							record['SurveySentDateTime']
						])
				print(f"Report saved to {bucket}/reports/{os.path.splitext(os.path.basename(file_in))[0] + '-report.csv'}.")
			elif not messages_send:
				print('No messages to send, so no report will be created.')
		except Exception as msg_err:
			print(f"There was an error.  The error was: {msg_err}")
			state_error = True
			return
		if state_error:
			print('[FAILED] Check the error folder or the logs.')
		else:
			print(f'[SUCCESS] Done, quitting.')
			return
	return

def import_history(rec):
	count_email = 0
	count_phone = 0
	count_no_phone = 0
	records = []
	patients = []
	visits = []
	locations = []
	messages = []
	messages_send = []
	messages_no_send = []
	messages_errors = []
	patients = []
	visits = []
	locations = []
	reports = []
	count_messages = 0
	skip_age = 0
	skip_expired = 0
	skip_out = 0
	skip_invalid = 0
	skip_old = 0
	db_cnx = connect_db(db_database)
	file_in = rec['s3']['object']['key']
	bucket = rec['s3']['bucket']['name']
	file_error = os.path.splitext(os.path.basename(file_in))[0] + '-error.csv'
	file_errors = os.path.splitext(os.path.basename(file_in))[0] + '-errors.csv'
	file_out = os.path.splitext(os.path.basename(file_in))[0] + '-resent_segment.csv'
	file_processed = os.path.splitext(os.path.basename(file_in))[0] + '-resent.csv'
	# folder_processed = os.path.join('OCCPM', 'Received', 'imported/')
	# folder_error = os.path.join('OCCPM', 'Received', 'error/')
	# filename = os.path.basename(file_in)
	filename_noext = os.path.splitext(os.path.basename(file_in))[0]
	filename_ext = os.path.splitext(os.path.basename(file_in))[1]
	# file_out_processed = folder_processed + filename_noext + '-processed' + filename_ext
	# file_out_error = folder_error + filename_noext + '-error' + filename_ext
	print(f'Received file: {file_in}.  Assuming provider is {practice}.')
	with s3.open(
		os.path.join(
			bucket,
			file_in
		),
		'r'
	) as csvfile:
		reader_csv = csv.DictReader(csvfile)
		for record in reader_csv:
			# print(f"processing record: {record}")
			if not check_patient_db(db_cnx, record['PatientID']):
				patients.append(record)
			if not check_visit_db(db_cnx, record['SurveyRequestID']):
				visits.append(record)
			if not check_location_db(db_cnx,  record['LocationName']):
				locations.append(record)
			item = {}
			email = {}
			phone = {}
			# print(f"Checking that {record['DateOfService']} is after we started.")
			if check_date_service(db_cnx, record['DateOfService'], history = True):
				print(f"{record['DateOfService']} is before we started")
				continue
			dtg = os.path.splitext(os.path.basename(file_in))[0].partition("_")[2]
			date = datetime.datetime.strftime(datetime.datetime.strptime(dtg, '%Y-%m-%d'), '%Y-%m-%d %H:%M:%S')
			# print(f"Checking that the patient is older than {age_min} ({record['Age']})")
			if int(record['Age']) < age_min:
				if check_message_db(db_cnx, record['SurveyRequestID'], 0, 3):
					print(f"{record['SurveyRequestID']} (skipped) already in messages table. Skipping.")
					continue
				item['DTGSent'] = date
				item['SurveyRequestID'] = record['SurveyRequestID']
				item['ReasonID'] = 3
				item['Comment'] = f'Rebuilt. A message will not be sent for this visit becuase the patient is under the minimum age of {age_min}.'
				item['TypeID'] = 0
				messages_no_send.append(item)
				skip_age =+ 1
				continue
			# check if the patient is expired
			# print(f"Checking if the patient has expired DoD: {record['DateOfDeath']}.")
			if record['DateOfDeath']:
				if check_message_db(db_cnx, record['SurveyRequestID'], 0, 4):
					print(f"{record['SurveyRequestID']} (skipped) already in messages table. Skipping.")
					continue
				item['DTGSent'] = date
				item['SurveyRequestID'] = record['SurveyRequestID']
				item['ReasonID'] = 4
				item['Comment'] = f"Rebuilt. A message will not be sent for this visit because the patient expired on {record['DateOfDeath']}."
				item['TypeID'] = 0
				messages_no_send.append(item)
				skip_expired =+ 1
				continue
			# print(f"Checking to see if we would have sent them an email {record['Email']}.")
			if record['Email']:
				if check_message_db(db_cnx, record['SurveyRequestID'], 3, 2):
					print(f"{record['SurveyRequestID']} (Email) already in messages table. Skipping.")
					continue
				email['DTGSent'] = date
				email['SurveyRequestID'] = record['SurveyRequestID']
				email['SurveyLink'] = get_survey_link(db_cnx, record['ServicingProvider']) + '?id=' + email['SurveyRequestID']
				email['Address'] = record['Email']
				email['ReasonID'] = 2
				email['Comment'] = f'Rebuilt from history.  Added {date_today}.'
				email['TypeID'] = 3
				email['DTGSent'] = date
				messages_send.append(email)
				count_email += 1
			# print(f"Checking to see if we would have sent them an SMS {record['Phone']}.")
			if record['Phone']:
				# print(f"Record has a phone number {record['Phone']}")
				if check_message_db(db_cnx, record['SurveyRequestID'], 2, 2):
					print(f"{record['SurveyRequestID']} (SMS) already in messages table. Skipping.")
					continue
				if record['Phone'] != '':
					phoneInfo = check_phone_number('US', record['Phone'], 'us-east-1', skip = True)
					patientPhone = phoneInfo[0]
				else:
					patientPhone = ''
					patientPhoneType = 0
				if phoneInfo[1] == 'VOICE':
					patientPhoneType = 2
				elif phoneInfo[1] == 'MOBILE' or phoneInfo[1] == 'PREPAID':
					patientPhoneType = 1
				else:
					patientPhoneType = 0
				if not patientPhoneType == 1 and len(patientPhone) != 12:
					if check_message_db(db_cnx, record['SurveyRequestID'], 0, 7):
						print(f"{record['SurveyRequestID']} (skipped) already in messages table. Skipping.")
						continue
					phone['DTGSent'] = date
					phone['SurveyRequestID'] = record['SurveyRequestID']
					phone['ReasonID'] = 7
					phone['Comment'] = f'Rebuilt. A message will not be sent for this visit, because the phone is invalid.'
					phone['TypeID'] = 0
					messages_no_send.append(phone)
					skip_invalid += 1
					count_no_phone =+ 1
					continue
				phone['SurveyRequestID'] = record['SurveyRequestID']
				phone['SurveyLink'] = get_survey_link(db_cnx, record['ServicingProvider']) + '?id=' + record['SurveyRequestID']
				phone['Address'] = patientPhone
				phone['ReasonID'] = 2
				phone['Comment'] = f'Rebuilt from history.  Added {date_today}.'
				phone['TypeID'] = 2
				phone['DTGSent'] = date
				count_phone =+ 1
				# print(f"Adding {record} to be sent")
				messages_send.append(phone)
	print(f'Updating {db_database} DB')
	if patients:
		insert_patients(db_cnx, patients)
		print(f'Added {len(patients)} new patients to the DB.')
	elif not patients:
		print("No new patients to import.")
	if locations :
		insert_locations(db_cnx, locations)
		print(f'Added {len(locations)} new locations to the DB.')
	elif not locations:
		print("No new locations to import.")
	if visits:
		insert_visits(db_cnx, visits)
		print(f'Added {len(visits)} new visits to the DB.')
	elif not visits:
		print("No new visits to import.")
	if messages_send:
		print(f"Inserting {len(messages_send)} messages (send) into the DB.")
		# for message in messages_send:
			# print(message)
		insert_messages(db_cnx, messages_send, history = True)
	if messages_no_send:
		print(f"Inserting {len(messages_no_send)} messages (not sent) into the DB.")
		# for message in messages_no_send:
			# print(message)
		insert_messages(db_cnx, messages_no_send, history = True)
	return

def resend(rec):
	state_error = False
	records = []
	patients = []
	visits = []
	locations = []
	messages = []
	messages_send = []
	messages_no_send = []
	messages_errors = []
	reports = []
	count_messages = 0
	skip_age = 0
	skip_expired = 0
	skip_out = 0
	skip_invalid = 0
	skip_old = 0
	while not state_error:
		try:
			db_cnx = connect_db(db_database)
			bucket = rec['s3']['bucket']['name']
			file_in = rec['s3']['object']['key']
			file_error = os.path.splitext(os.path.basename(file_in))[0] + '-error.csv'
			file_errors = os.path.splitext(os.path.basename(file_in))[0] + '-errors.csv'
			file_out = os.path.splitext(os.path.basename(file_in))[0] + '-resent_segment.csv'
			file_processed = os.path.splitext(os.path.basename(file_in))[0] + '-resent.csv'
			file_report = os.path.splitext(os.path.basename(file_in))[0] + '-resent_report.csv'
			segment_name = practice + '-resend-' + str(datetime.datetime.now().strftime('%Y-%m-%d_%H%M'))
			if not check_valid_file(file_in):
				state_error = errorout(
					method = 'file',
					object = 's3://' + bucket + file_in,
					destination = 's3://' + bucket + '/error/' + file_error
				)
				break
			with s3.open(
				os.path.join(
					bucket,
					file_in
				),
				 'r'
			) as csvfile:
				reader = csv.DictReader(csvfile)
				header = reader.fieldnames
				for line in reader:
					records.append(line)
			print(f'Processing received file: {file_in}.  Assuming provider is {practice}.  There are {len(records)} visits in this file.')
			# check if the file contains records from the right practice
			if practice not in records[0]['PracticeName']:
				print(f"[ERROR] Got {records[0]['PracticeName']} but expected {practice}.")
				state_error = errorout(
					method = 'file',
					object = os.path.join(
						bucket,
						file_in
					),
					destination = os.path.join(
						bucket,
						'error', os.path.splitext(os.path.basename(file_in))[0],
						'-error.csv'
					)
				)
				break
			print('Checking for new patients, visits, and locations.')
			for record in records:
				# check for some common errors:
				if not record['SurveyRequestID'] or not record['PatientID'] or not record['PatientName'] or not record['ServicingProvider'] or not record['DateOfService'] or not record['PostDate'] or not record['LocationName']:
					print(f'A critical field was blank.  Skipping this record.  Error records will be saved to {bucket}/error/{file_error}')
					messages_errors.append(record)
					continue
				if not check_patient_db(db_cnx, record['PatientID']):
					patients.append(record)
				if not check_visit_db(db_cnx, record['SurveyRequestID']):
					visits.append(record)
				if not check_location_db(db_cnx,  record['LocationName']):
					locations.append(record)
			if patients:
				insert_patients(db_cnx, patients)
				print(f'Added {len(patients)} new patients to the DB.')
			if locations :
				insert_locations(db_cnx, locations)
				print(f'Added {len(locations)} new locations to the DB.')
			if visits:
				insert_visits(db_cnx, visits)
				print(f'Added {len(visits)} new visits to the DB.')
			# Add pending messages to the DB, but only if a message for that visit doesn't already exist
			# this probably isn't the most efficient way to do it, but iterate over the list again to add the messages
			print(f"Comparing visits in {file_in} to messages in db.")
			for record in records:
				# regardless of plan type, check if: 1) valid service date, 2) meet age criteria, 3) not expired
				# if check_date_service(db_cnx, record['DateOfService']):
				# 	record['ReasonID'] = 10
				# 	record['Comment'] = f'A message will not be sent for this visit, because the date of service is over 30 days.'
				# 	record['TypeID'] = 0
				# 	messages_no_send.append(record)
				# 	skip_old += 1
				# 	continue
				# # check if patient is old enough
				# if int(record['Age']) < age_min:
				# 	record['ReasonID'] = 3
				# 	record['Comment'] = f'A message will not be sent for this visit becuase the patient is under the minimum age of {age_min}.'
				# 	record['TypeID'] = 0
				# 	messages_no_send.append(record)
				# 	skip_age =+ 1
				# 	continue
				# check if the patient is expired
				if record['DateOfDeath']:
					record['ReasonID'] = 4
					record['Comment'] = f"A message will not be sent for this visit because the patient expired on {record['DateOfDeath']}."
					record['TypeID'] = 0
					messages_no_send.append(record)
					skip_expired =+ 1
					continue
				# check if the patient has opted out
				if check_opt_out(db_cnx, record['PatientID']):
					record['ReasonID'] = 8
					record['Comment'] = f'A message will not be sent for this visit because the patient opted out of messaging.'
					record['TypeID'] = 0
					messages_no_send.append(record)
					skip_out =+ 1
					continue
				# plan 2 is the only one that wont' send a text...
				if plan != 2:
					record['PatientPhone'] = get_phone_db(db_cnx, record['PatientID'])
					# check if a text message isn't already pending
					if not check_message_db(db_cnx, record['SurveyRequestID'], 6):
						# check if we can text the phone...
						if not check_landline(db_cnx, record['PatientID']) and len(record['PatientPhone']) != 12:
							record['ReasonID'] = 7
							record['Comment'] = f'A message will not be sent for this visit, because the phone is invalid.'
							record['TypeID'] = 0
							messages_no_send.append(record)
							skip_invalid += 1
							continue
						record['SurveyLink'] = get_survey_link(db_cnx, record['ServicingProvider']) + '?id=' + record['SurveyRequestID']
						record['Address'] = get_phone_db(db_cnx, record['PatientID'])
						record['ReasonID'] = 1
						record['Comment'] = f'Resent. Added {date_today}.'
						record['TypeID'] = 6
						messages_send.append(record)
					else:
						messages_errors.append(record)
						print(f"A message already exists in the DB for visit {record['SurveyRequestID']}. Skipping.")
				# if the plan is three, patients will get a text and an email
				if plan == 3:
					# check if an email isn't already pending
					if not check_message_db(db_cnx, record['SurveyRequestID'], 7):
						record['SurveyLink'] = get_survey_link(db_cnx, record['ServicingProvider']) + '?id=' + record['SurveyRequestID']
						record['Address'] = get_email_db(db_cnx, record['PatientID'])
						record['ReasonID'] = 1
						record['Comment'] = f'Resent. Added {date_today}.'
						record['TypeID'] = 7
						messages_send.append(record)
					else:
						messages_errors.append(record)
						print(f"A message already exists in the DB for visit {record['SurveyRequestID']}. Skipping.")
			if messages_errors:
				with s3.open(
					's3://' + bucket + '/error/' + file_errors,
					'w',
					newline = '',
					encoding = 'utf-8'
				) as out_file:
					writer_csv = csv.DictWriter(
						out_file,
						fieldnames = [
							'SurveyRequestID',
							'UUID',
							'PracticeName',
							'PatientID',
							'PatientName',
							'Age',
							'Phone',
							'PatientPhone',
							'Email',
							'patientEmail',
							'DateOfService',
							'ProviderTitle',
							'ServicingProvider',
							'LocationName',
							'VisitNumber',
							'PostDate',
							'DateOfDeath',
							'SurveyLink',
							'Address',
							'ReasonID',
							'TypeID',
							'Comment'
						]
					)
					writer_csv.writeheader()
					for message in messages_errors:
						writer_csv.writerow(message)
					print(f'[WARNING] There were errors with some records.  Those records have been saved to: {bucket}/error/{file_error}')
			# update DB with message status
			print(f'Updating {db_database} DB with messages')
			messages = messages_send + messages_no_send
			insert_messages(db_cnx, messages)
			s3.move(
				's3://' + bucket + '/' + file_in,
				's3://' + bucket + '/processed/' + file_processed
				)
			print(f"[SUCCESS] Processed {file_in} and moved to processed/{os.path.splitext(os.path.basename(file_in))[0]}-resent_processed.csv")
			# messages = mysql_query(db_cnx, "SELECT * FROM viewMessagesPending")
			print(f'Processing {len(messages_send)} pending messages into Pinpoint.')
			with s3.open(
				's3://' + bucket + '/segment/' + file_out,
				'w',
				newline = '',
				encoding = 'utf-8'
			) as out_file:
				writer_csv = csv.DictWriter(
					out_file,
					fieldnames = fieldnames
				)
				writer_csv.writeheader()
				for message in messages_send:
					message['Patient'] = mysql_query(db_cnx, (f"SELECT Patient FROM viewMessagesPending WHERE VisitID = '{message['SurveyRequestID']}' LIMIT 1"))[0][0]
					message['Provider'] = mysql_query(db_cnx, (f"SELECT Provider FROM viewMessagesPending WHERE VisitID = '{message['SurveyRequestID']}' LIMIT 1"))[0][0]
					message['Location'] = mysql_query(db_cnx, (f"SELECT Location FROM viewMessagesPending WHERE VisitID = '{message['SurveyRequestID']}' LIMIT 1"))[0][0]
					message['MessageID'] = mysql_query(db_cnx, (f"SELECT MessageID FROM viewMessagesPending WHERE VisitID = '{message['SurveyRequestID']}' LIMIT 1"))[0][0]
					if message['TypeID'] == 7:
						channelType = 'EMAIL'
					else:
						channelType = 'SMS'
					writer_csv.writerow({
						'ChannelType': channelType,
						'Address': message['Address'],
						'Id': str(uuid.uuid4()),
						'User.UserAttributes.PracticeName': practice,
						'User.UserAttributes.PatientName': message['Patient'],
						'Location.Country': "US",
						'User.UserAttributes.Age': message['Age'],
						'User.UserAttributes.DateOfService': message['DateOfService'],
						'User.UserAttributes.ServicingProvider': message['Provider'],
						'User.UserAttributes.LocationName': message['Location'],
						'User.UserAttributes.VisitNumber': message['SurveyRequestID'],
						'User.UserAttributes.PostDate': message['PostDate'],
						'User.UserAttributes.DateofDeath': message['DateOfDeath'],
						'User.UserAttributes.MessageID': message['MessageID'],
						'User.UserAttributes.SurveyLink': message['SurveyLink'],
					})
			if len(messages_send) > 0:
				print(f"[SUCCESS] Processed {len(messages_send)} messages into a file for import into Pinpoint.  {skip_age} patients were underage, {skip_expired} patients were expired, {skip_out} patients opted out, and {skip_invalid} patients had invalid phone numbers. Scheduling {bucket}/{file_out} for import into a Pinpoint segment.")
				client = boto3.client('pinpoint')
				try:
					response = client.create_import_job(
						ApplicationId = projectId,
						ImportJobRequest = {
							'DefineSegment': True,
							'Format': 'CSV',
							'RegisterEndpoints': True,
							'RoleArn': importRoleArn,
							'S3Url': 's3://' + bucket + '/segment/' + file_out,
							'SegmentName': segment_name
						}
					)
					segment_id = response['ImportJobResponse']['Definition']['SegmentId']
				except ClientError as e:
					print("Error: " + e.response['Error']['Message'])
					continue
				else:
					print("Import job " + response['ImportJobResponse']['Id'] + " " + response['ImportJobResponse']['JobStatus'] + ".")
					print("Segment ID: " + response['ImportJobResponse']['Definition']['SegmentId'])
					print("Application ID: " + projectId)
				print(f'Updating {db_database} database with segment schedule.')
				with s3.open(
					's3://' + bucket + '/segment/' + file_out,
					'r',
					newline = '',
					encoding = 'utf-8'
				) as file_segment:
					reader_csv = csv.DictReader(
						file_segment,
						fieldnames = fieldnames
					)
					for row in reader_csv:
						if row['Id'] == 'Id':
							continue
						reason = 9
						comment = f'Sent to Pinpoint to be imported into segment {segment_name}.'
						mysql_query(
							db_cnx,
							f"UPDATE messages SET ReasonID = {reason}, Comments = '{comment}' WHERE MessageID = {row['User.UserAttributes.MessageID']}",
							commit = True
						)
				print(f'Create Pinpoint campaign from segment {segment_name} (id: {segment_id}), using template {template_sms_oops} and {template_email_oops}.  Pausing for {seconds} seconds so Pinpoint has time to process import job.')
				time.sleep(seconds)
				client = boto3.client('pinpoint')
				try:
					response = client.create_campaign(
						ApplicationId = projectId,
						WriteCampaignRequest = {
							'Description': 'Campaign created to send Clearsurvey surveys to patients after their visit to a provider.',
							'AdditionalTreatments': [],
							'IsPaused': False,
							'Schedule': {
								'Frequency': 'ONCE',
								'IsLocalTime': False,
								'StartTime': 'IMMEDIATE',
								'Timezone': 'UTC',
								'QuietTime': {}
							},
							'TemplateConfiguration': {
								'EmailTemplate': {
									'Name': template_email_oops
								},
								'SMSTemplate': {
									'Name': template_sms_oops
								}
							},
							'Name': segment_name,
							'SegmentId': segment_id,
							'SegmentVersion': 1,
						}
					)
				except ClientError as e:
					 print(e.response['Error']['Message'])
					 state_error = True
					 break
				else:
					print(response)
				print(f'Updating {db_database} database with campaign created.')
				with s3.open(
					's3://' + bucket + '/segment/' + file_out,
					'r',
					newline = '',
					encoding = 'utf-8'
				) as file_campaign:
					reader_csv = csv.DictReader(
						file_campaign,
						fieldnames = fieldnames
					)
					for row in reader_csv:
						if row['Id'] == 'Id':
							continue
						reason = 2
						comment = f'Campaign created in pinpoint.'
						mysql_query(
							db_cnx,
							f"UPDATE messages SET Sent = 1, ReasonID = {reason}, Comments = '{comment}', DTGSent = '{datetime.datetime.now():%Y-%m-%d %H:%M:%S}' WHERE MessageID = {row['User.UserAttributes.MessageID']}",
							commit = True
						)
			else:
				print(f'[WARNING] No messages to process into Pinpoint.  Check error file.')
			print(f'Creating report')
			if messages_send:
				with s3.open(
					's3://' + bucket + '/reports/' + file_report,
					'w',
					newline = '',
					encoding = 'utf-8'
				) as out_file:
					header.extend(['SurveyNotSentReason', 'SurveySentDateTime'])
					writer_csv = csv.writer(out_file)
					writer_csv.writerow(['SurveyRequestID', 'SurveyNotSentReason', 'SurveySentDateTime'])
					for record in records:
						result = mysql_query(db_cnx, (f"SELECT Sent, MessageID, ReasonID, DTGSent FROM messages WHERE VisitID = '{record['SurveyRequestID']}' LIMIT 1"))[0]
						if result[0] == 1:
							record['SurveySentDateTime'] = result[3].strftime('%Y-%m-%d %h:%m:%d')
						else:
							record['SurveySentDateTime'] = ''
						if result[2] == 2:
							record['SurveyNotSentReason'] = ''
						elif result[2] == 3:
							record['SurveyNotSentReason'] = 'Patient underage'
						elif result[2] == 4:
							record['SurveyNotSentReason'] = 'Patient expired'
						elif result[2] == 7:
							record['SurveyNotSentReason'] = 'Patient phone invalid'
						elif result[2] == 8:
							record['SurveyNotSentReason'] = 'Patient opted out'
						elif result[2] == 10:
							record['SurveyNotSentReason'] = 'Date of service > 30 days'
						else:
							record['SurveyNotSentReason'] = 'Unknown'
						writer_csv.writerow([
							record['SurveyRequestID'],
							record['SurveyNotSentReason'],
							record['SurveySentDateTime']
						])
				print(f"Report saved to {bucket}/reports/{os.path.splitext(os.path.basename(file_in))[0] + '-resent_report.csv'}.")
			elif not messages_send:
				print('No messages to send, so no report will be created.')
		except Exception as msg_err:
			print(f"There was an error.  The error was: {msg_err}")
			state_error = True
			return
		if state_error:
			print('[FAILED] Check the error folder or the logs.')
		else:
			print(f'[SUCCESS] Done, quitting.')
			return
	return

def lambda_handler(event, context):
	print(f"Received event(s): {str(event)}")
	for rec in event['Records']:
		if rec['s3']['object']['key'].startswith('input/'):
			print(f"Based on file ({rec['s3']['object']['key']}), Courier will process this event normally.")
			import_new(rec)
			return
		elif rec['s3']['object']['key'].startswith('history/'):
			print(f"Based on file ({rec['s3']['object']['key']}), Courier will process this event as a historic import.  No messages will be sent.")
			import_history(rec)
			return
		elif rec['s3']['object']['key'].startswith('resend/'):
			print(f"Based on file ({rec['s3']['object']['key']}), Courier will process this event as a resend.  Messages will be resent to patients based on the assumption that an error occured processing messages or surveys previously.")
			resend(rec)
			return
		else:
			print('Unknown bucket.')
			return
	return
