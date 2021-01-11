#!/usr/bin/python3

import pymysql

###################################################
# MYSQL CONNECTION
db = pymysql.connect("localhost","root","root","okta_scale", port=8889 )

cursor = db.cursor(pymysql.cursors.DictCursor)

cursor.execute("SELECT VERSION()")

data = cursor.fetchone()

print ("Database version : %s " % data)
