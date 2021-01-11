import json

import pymysql

import random

import requests

import sys

import time

# ###################################################

from utils import dbconn

# ###################################################

cursor = dbconn.cursor
db = dbconn.db

# for uuid generation
alpha = ["a", "b", "c", "d", "e", "f", "g", "h", "j", "k", "m", "n", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"]

good_status_codes = [200, 201]

error_status_codes = [400, 429, 500]

expected_status_codes = good_status_codes + error_status_codes

#####################################################
# functions

def check_api_key(api_key, tenant):

	url = get_url("user_schema")

	r = requests.get(url, headers=headers)

	if r.status_code == 200:
		print("The api key + tenant combination is valid.")

		update_rate_limits(tenant, r.headers)

	else:
		print("something went wrong with the API key and tenant combination.")
		exit()

def check_api_rate_limit(tenant):

	query = "SELECT * FROM rate_limit WHERE tenant = '" + tenant + "'"

	try:
		cursor.execute(query)

		row = cursor.fetchone()

		x_rate_limit_remaining = row["x_rate_limit_remaining"]

		x_rate_limit_reset = row["x_rate_limit_reset"]

		if (x_rate_limit_remaining > min_rate_limit_remaining):
			return

		else:
			print("********APPROACHING API RATE LIMIT*********")
			print("rate limit remaining: " + str(x_rate_limit_remaining))

			x = 0

			while (time.time() < x_rate_limit_reset + 2):

				time.sleep(1)

				print("rate limit reset: " + str(x_rate_limit_reset))

				current_time = int(time.time())

				print("current time: " + str(current_time))

				remaining_time = x_rate_limit_reset - current_time + 1

				print("time remaining until api limit resets: " + str(remaining_time))

			return

	except pymysql.Error as e:
		print("Error %d: %s" % (e.args[0], e.args[1]))

def log_error(err_msg):

	print (err_msg)

	t = time.localtime()

	s = time.asctime(t)

	log_line = "\n" + s + " | " + err_msg + " | " + project_id

	log_file = open("error_log.txt", "a")
	log_file.write(log_line)
	log_file.close()

def create_obj(object_type):

	uuid = get_uuid()

	url = get_url(object_type)

	url += get_query_string(object_type)

	body = get_body(object_type, uuid)

	if object_type == "notifications":

		okta_id = body["users"][0]

		body = "{\"target\":\"GROUPS_AND_USERS\",\"groups\":[],\"users\":[\""
		body += okta_id + "\"],\"message\":\"this is a custom message for user_id "
		body += okta_id + "\"}"

	print(body)

	check_api_rate_limit(tenant)

	print("attempting call to:", url)

	req_start_time = int(time.time())

	try:

		if object_type == "notifications":
			r = requests.request("POST", url, data = body, headers=headers)
		else:
			r = requests.post(url, headers=headers, json=body)
		r.raise_for_status()

	except requests.exceptions.HTTPError as errh:
		err_msg = "an http error occurred:" + repr(errh)
		log_error(err_msg)
		return

	except requests.exceptions.ConnectionError as errc:
		err_msg = "an error connecting to the API occurred:" + repr(errc)
		log_error(err_msg)
		return

	except requests.exceptions.Timeout as errt:
		err_msg = "a timeout error occurred:" + repr(errt)
		log_error(err_msg)
		return

	except requests.exceptions.RequestException as err:
		err_msg = "an unknown error occurred" + repr(err)
		log_error(err_msg)
		return

	req_elapsed_time = time.time() - req_start_time

	req_elapsed_time = int(req_elapsed_time * 100)

	req_elapsed_time = req_elapsed_time / 100

	print("The API call took " + str(req_elapsed_time) + " seconds")

	update_rate_limits(tenant, r.headers)

	update_batch_table(req_elapsed_time, r.headers, r.status_code, r.text)

	if r.status_code in good_status_codes:

		last_inserted_id = update_objects_table(uuid, project_id)

		print("the last inserted id was: " + str(last_inserted_id))

		if last_inserted_id % count_interval == 0:

			print("the response object is: ")
			print(r.json())

			update_okta_object_name(object_type, r.json(), last_inserted_id, uuid)

			update_project_table(last_inserted_id)

		if object_type == "notifications":

			j = r.json()

			notification_id = j["id"]

			query = "UPDATE objects_salesforce_users_dev SET notified = 1"

			query += ", notification_id = '" + notification_id + "'"

			query += " WHERE okta_id = '" + okta_id + "'"

			print(query)

			try:
				cursor.execute(query)

				db.commit()

			except pymysql.Error as e:
				print("Error %d: %s" % (e.args[0], e.args[1]))
				exit()
	else:
		if r.status_code == 400:
			print("the status code was 400. Time to bail.")
			exit()

def get_api_key(project_id):

	with open('projects/' + project_id + '/secrets.json') as f:
		secrets = json.load(f)

	if "okta_api_key" not in secrets:
		print("could not find an api key in the secrets object.")
		exit()

	if not secrets["okta_api_key"]:
		print("the api key appears to be empty.")
		exit()

	print("found an api key.")

	return secrets["okta_api_key"]

def get_body(object_type, uuid):

	if object_type == "clients":

		body = {
			"client_name": "test_obj_" + uuid,
			"response_types": [
				"token"
			],
			"grant_types": [
				"client_credentials"
			],
			"token_endpoint_auth_method": "client_secret_basic",
			"application_type": "service"
		}

		return body

	if object_type == "notifications":

		# query = """SELECT uuid FROM objects_salesforce_users_dev 
		# 	WHERE okta_id IS NULL ORDER BY index_val ASC LIMIT 1"""

		query = """SELECT index_val, uuid FROM objects_salesforce_users_dev
		WHERE notified = 0 ORDER BY index_val ASC LIMIT 1"""

		try:
			cursor.execute(query)

			data = cursor.fetchone()

			if (data):
				print(json.dumps(data))

				uuid = data["uuid"]

				index_val = data["index_val"]

			else:
				print("query did not succeed.")
				exit()

		except pymysql.Error as e:
			print("Error %d: %s" % (e.args[0], e.args[1]))
			exit()

		email = "test.user_"

		if index_val % count_interval == 0:

			email += str(index_val) + "_"

		email += uuid + "@atkodemo.com"


		# new_email = "test.user_" + str(index) + "_" + uuid + "@atkodemo.com"


		print("the email is: " + email)

		url = "https://dev-640315.oktapreview.com/api/v1/users/" + email

		headers = {
		  'Accept': 'application/json',
		  'Content-Type': 'application/json',
		  'Authorization': 'SSWS '
		}

		response = requests.request("GET", url, headers=headers)

		j = response.json()

		okta_id = j["id"]

		email = j["profile"]["email"]

		##############################################

		query = "UPDATE objects_salesforce_users_dev SET okta_id = '" + okta_id + "'"

		query += ", email = '" + email + "'"

		query += " WHERE uuid = '" + uuid + "'"

		try:
			cursor.execute(query)

			db.commit()

		except pymysql.Error as e:
			print("Error %d: %s" % (e.args[0], e.args[1]))
			exit()

		body = {
			"target": "GROUPS_AND_USERS",
			"groups":[],
			"users":[okta_id],
			"message": "This is a custom message especially for " + email
		}

		return body

	if object_type == "users":

		email = "test.user_" + uuid + "@atkodemo.com"

		body = {
			"profile": {
				"firstName": "test",
				"lastName": "user",
				"email": email,
				"login": email
			},
			"credentials": {
				"password": { "value": "Okta1234!" },
				"recovery_question": {
					"question": "Who is Clark Kent?",
					"answer": "Superman"
				}
			}
		}

		return body

def get_project_id():

	if len(sys.argv) > 1:
		flag = sys.argv[1]
		if flag == "-p":

			project_id = sys.argv[2]

			print ("got the project name from the command line:", project_id)

			return project_id

	else:
		print("no project name found.")
		print("indicate a project like this:")
		print("python3 app.py -p my_project_name")

		exit();

def get_project_settings(project_id):

	query = "SELECT * FROM projects WHERE project_id = '" + project_id + "'"

	try:
		cursor.execute(query)

		data = cursor.fetchone()

		if data:
			return data
		else:
			print("could not find a project with that id in the table.")
			exit()

	except pymysql.Error as e:
		print("Error %d: %s" % (e.args[0], e.args[1]))
		exit()

def get_query_string(object_type):

	if object_type == "users":
		return "?activate=true"
	else:
		return ""

def get_url(object_type):

	url = "https://" + tenant

	if (object_type == "clients"):
		url += "/oauth2"

	else:
		url += "/api"

	if (object_type == "notifications"):
		url += "/internal/admin/notification"
		return url

	else:
		url += "/v1"

	if (object_type == "user_schema"):
		url += "/meta/schemas/user/default"
	else:
		url += "/" + object_type

	return url

def get_uuid():

	uuid = ""

	for x in range(0, 16):
		uuid += random.choice(alpha)

	return uuid

def initialize_batch(project_id):

	query = "SELECT * FROM batches WHERE project_id = '" + project_id + "'"

	try:
		cursor.execute(query)

		data = cursor.fetchone()

		if (data):

			query = "SELECT MAX(end_time) as last_insert_time FROM batches"

			query += " WHERE project_id = '" + project_id + "'"

			try:
				cursor.execute(query)

				row = cursor.fetchone()

				last_insert_time = row["last_insert_time"]

				# if the last record inserted was more than 90 seconds ago,
				# then let's declare this a new batch

				time_diff = time.time() - last_insert_time

				print("the time delta is", time_diff)

				if (time_diff > 90):
					start_new_batch(project_id)
					print("we should have started a new batch")

				else:
					update_batch_record(project_id)

			except pymysql.Error as e:
				print("Error %d: %s" % (e.args[0], e.args[1]))

		else:
			start_new_batch(project_id)

	except pymysql.Error as e:
		print("Error %d: %s" % (e.args[0], e.args[1]))

def initialize_rate_limits(tenant):

	query = "SELECT * FROM rate_limit WHERE tenant = '" + tenant + "'"

	try:
		cursor.execute(query)

		row = cursor.fetchone()

		if (row):
			print("found a rate limit row")
		else:
			print("could not find a rate limit row")

			query = "INSERT INTO rate_limit SET tenant = '" + tenant + "'"

			try:
				cursor.execute(query)

				db.commit()

			except pymysql.Error as e:
				print("Error %d: %s" % (e.args[0], e.args[1]))

	except pymysql.Error as e:
		print("Error %d: %s" % (e.args[0], e.args[1]))

def start_new_batch(project_id):

	query = "DELETE FROM batches WHERE project_id = '" + project_id + "'"

	try:
		cursor.execute(query)

		db.commit()

	except pymysql.Error as e:
		print("Error %d: %s" % (e.args[0], e.args[1]))

	#####################################################################

	current_time = str(int(time.time()))

	query = "INSERT INTO batches SET start_time = " + current_time

	query += ", end_time = " + current_time

	query += ", api_total_duration = 0.00"

	query += ", low_duration = 10000.00"

	query += ", iterations = 1"

	query += ", project_id = '" + project_id + "'"

	print ("\n" + query)

	try:
		cursor.execute(query)

		db.commit()

	except pymysql.Error as e:
		print("Error %d: %s" % (e.args[0], e.args[1]))

def update_batch_record(project_id):

	current_time = str(int(time.time()))

	query = "UPDATE batches SET "
	query += "start_time = " + current_time + ", "
	query += "end_time = " + current_time

	query += " WHERE project_id = '" + project_id + "'"

	print ("\n" + query)

	try:
		cursor.execute(query)

		db.commit()

	except pymysql.Error as e:
		print("Error %d: %s" % (e.args[0], e.args[1]))

def update_project_table(last_inserted_id):

	query = "UPDATE projects SET highest_index = " + str(last_inserted_id)

	query += " WHERE project_id = '" + project_id + "'"

	try:
		cursor.execute(query)

		db.commit()

	except pymysql.Error as e:
		print("Error %d: %s" % (e.args[0], e.args[1]))

def update_batch_table(api_duration, headers, status_code, text):

	x_rate_limit_remaining = headers["X-Rate-Limit-Remaining"]

	if status_code not in expected_status_codes:

		print("received an unexpected status code:", status_code)

		exit()

	if status_code in good_status_codes:

		print ("The API call was successful.")

	end_time = int(time.time())

	query = "UPDATE batches SET iterations = iterations + 1"

	query += ", end_time = " + str(end_time)

	query += ", api_total_duration = api_total_duration + " + str(api_duration)

	query += ", low_duration = LEAST(low_duration, " + str(api_duration) + ")"

	query += ", high_duration = GREATEST(high_duration, " + str(api_duration) + ")"

	query += ", low_rate_limit_remaining = LEAST(low_rate_limit_remaining, " + str(x_rate_limit_remaining) + ")"

	if status_code not in good_status_codes:

		col_name = "err_" + str(status_code) + "_count"

		query += ", " + col_name + " = " + col_name + " + 1"

		print("something went wrong with the API call:")
		print(text)

	query += " WHERE project_id = '" + project_id + "'"

	print(query)

	try:
		cursor.execute(query)

		db.commit()

	except pymysql.Error as e:
		print("Error %d: %s" % (e.args[0], e.args[1]))

def update_objects_table(uuid, project_id):

	query = "INSERT INTO objects_" + project_id + " SET uuid = '" + uuid + "'"

	try:
		cursor.execute(query)

		db.commit()

	except pymysql.Error as e:
		print("Error %d: %s" % (e.args[0], e.args[1]))

	return cursor.lastrowid

def update_okta_object_name(object_type, obj, index, uuid):

	check_api_rate_limit(tenant)

	url = get_url(object_type)

	if object_type == "clients":

		obj["client_name"] = "test_obj_" + str(index) + "_" + uuid

		url += "/" + obj["client_id"]

		r = requests.put(url, json=obj, headers=headers)

	elif object_type == "notifications":
		return

	elif object_type == "users":

		new_email = "test.user_" + str(index) + "_" + uuid + "@atkodemo.com"

		obj["profile"]["login"] = new_email
		obj["profile"]["email"] = new_email

		url += "/" + obj["id"]

		r = requests.post(url, json=obj, headers=headers)

	else:
		print ("invalid object type " + object_type)
		exit()

	if r.status_code == 200 or r.status_code == 201:
		print("The API call was successful.")

		update_rate_limits(tenant, r.headers)

	else:
		print("something went wrong with the API call:")
		print(r.text)
		# exit()

def update_rate_limits(tenant, headers):

	x_rate_limit_remaining = headers["X-Rate-Limit-Remaining"]
	x_rate_limit_reset = headers["X-Rate-Limit-Reset"]

	print ("x_rate_limit_remaining: " + x_rate_limit_remaining)
	print ("x_rate_limit_reset: " + x_rate_limit_reset)

	query = "UPDATE rate_limit SET x_rate_limit_remaining=" + x_rate_limit_remaining

	query += ", x_rate_limit_reset=" + x_rate_limit_reset

	query += " WHERE tenant = '" + tenant + "'"

	try:
		cursor.execute(query)

		db.commit()

	except pymysql.Error as e:
		print("Error %d: %s" % (e.args[0], e.args[1]))

##################################################################
# Main program

project_id = get_project_id()

print("the project id is: " + project_id)

project_settings = get_project_settings(project_id)

print("the project settings are:")

print(project_settings)

batch_size = project_settings["batch_size"]
count_interval = project_settings["count_interval"]
min_rate_limit_remaining = project_settings["min_rate_limit_remaining"]
object_type = project_settings["object_type"]
target_count = project_settings["target_count"]
tenant = project_settings["tenant"]

###################################################
# initialize the rate limit table

initialize_rate_limits(tenant)

###################################################
# check validity of api key + tenant combo

api_key = get_api_key(project_id)

headers = {
	'Authorization': "SSWS " + api_key,
	'Content-Type': "application/json",
	'Accept': "application/json",
	'Cache-Control': "no-cache",
	'Connection': "keep-alive",
	'cache-control': "no-cache"
}

check_api_key(api_key, tenant)

###################################################

# update the batch table in the db
initialize_batch(project_id)

#############################################

print("the batch size is: " + str(batch_size))

for x in range(0, batch_size):

	project_settings = get_project_settings(project_id)

	highest_index = project_settings["highest_index"]
	target_count = project_settings["target_count"]

	if (highest_index >= target_count):
		print("the highest_index is " + str(highest_index) + " and the target_count is " + str(target_count))
		exit()

	print("\n*********************************")

	print ("iterations remaining: " + str(batch_size - x))

	create_obj(object_type)

##################################################################

exit()
