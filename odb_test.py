#!/usr/bin/env python
#-*- coding: UTF-8 -*- 

'''
    File Name: 
         odb_auto_test.py
    Description: 
         It test load, extract, copy and general function of odb untility.
		 This script can create data file to load automantically in folder odb_auto_loadfile.
		 Check result in odb_auto_log/odb_log_sumary_timestamp and details in odb_log_detail_timestamp
		 create file	: scripts/ddl_person1_extract.sql ddl_person1.sql ddl_person2_extract.sql ddl_person2.sql ddl_person3_dup_test.sql ddl_person3_extract.sql ddl_person3.sql ddl_person_extract.sql ddl_person.sql script.sql
						  odb_auto_loadfile load_data_1 ~ load_data_12 load_data_bad
						  conf/export_tbl_list
		 check result	: odb_auto_log/odb_log_sumary_timestamp odb_log_detail_timestamp
     Author: 
         liang.zhang@esgyn.cn
     FileCreatedTime: 
         2016.07.4
     LastModifiedTime: 
         2016.07.7
'''

import commands
import logging
import time
import os
import random

max_to_load = 1000
max_to_test_performance = 100000

print
print "now prepare files/scripts/tables for odb test ..."
print 
# create logger
logger_sumary = logging.getLogger('sumary_log')
logger_sumary.setLevel(logging.DEBUG)
logger_detail = logging.getLogger('sumary_detail')
logger_detail.setLevel(logging.DEBUG)

#create log folder
folder = 'odb_auto_log'
if os.path.exists(folder):
	if os.path.isfile(folder):
		os.remove(folder)
		os.mkdir(folder)
else:
	os.mkdir(folder)

# create handler，to write log file
ISOTIMEFORMAT = '%Y-%m-%d-%H-%M-%S'
log_path_sum = folder + '/odb_log_sumary_' + str(time.strftime(ISOTIMEFORMAT))
sum_handle = logging.FileHandler(log_path_sum)
sum_handle.setLevel(logging.DEBUG)
log_path_detail = folder + '/odb_log_detail_' + str(time.strftime(ISOTIMEFORMAT))
detail_handle = logging.FileHandler(log_path_detail)
detail_handle.setLevel(logging.DEBUG)
print "now create log file"
print "sumary log file :" + log_path_sum
print "detail log file :" + log_path_detail
print 

# create handler，to write consle
consle_handle = logging.StreamHandler()
consle_handle.setLevel(logging.DEBUG)

# define handler format
# %(asctime)s - %(name)s - %(levelname)s - 
formatter = logging.Formatter('%(message)s')
sum_handle.setFormatter(formatter)
consle_handle.setFormatter(formatter)
detail_handle.setFormatter(formatter)

# add handler to logger
logger_sumary.addHandler(sum_handle)
logger_detail.addHandler(consle_handle)
logger_detail.addHandler(detail_handle)	

# create config file determine the list of src to be extracted
def create_config_file():
	config_folder = 'conf'
	if os.path.exists(config_folder):
		if os.path.isfile(config_folder):
			os.remove(config_folder)
			os.mkdir(config_folder)
	else:
		os.mkdir(config_folder)	

	config_file = file("conf/export_tbl_list", "w")
	config_file.write("src=trafodion.odb_test_extract.person1_e\n")
	config_file.write("src=trafodion.odb_test_extract.person2_e\n")
	config_file.write("src=trafodion.odb_test_extract.person3_e\n")
	config_file.close()	
		
# create script for odb automantical test
def create_script():
	print "now create scripts for odb automantical test"
	print "please wait..."

	script_folder = 'scripts'
	if os.path.exists(script_folder):
		if os.path.isfile(script_folder):
			os.remove(script_folder)
			os.mkdir(script_folder)
	else:
		os.mkdir(script_folder)
		
	#script to test odb run a sql script
	script_file = file("scripts/script.sql", "w")
	script_file.write("SELECT COUNT(*) FROM trafodion.odb_test.person;")
	script_file.close()
	
	#script to create schema odb_test and table TRAFODION.odb_test.person
	script_file = file("scripts/ddl_person.sql", "w")
	script_file.write("create schema TRAFODION.ODB_TEST;\n")
	script_file.write("drop table TRAFODION.odb_test.person;\n")
	script_file.write('CREATE TABLE TRAFODION.odb_test."PERSON" (PID BIGINT SIGNED NOT NULL,FNAME CHAR(20) NOT NULL,LNAME CHAR(20) NOT NULL,COUNTRY VARCHAR(40) NOT NULL,CITY VARCHAR(40) NOT NULL,BDATE DATE NOT NULL,SEX CHAR(1) NOT NULL,EMAIL VARCHAR(40) NOT NULL,SALARY NUMERIC(9,2) NOT NULL,EMPL VARCHAR(40) NOT NULL,NOTES VARCHAR(80),LOADTS TIMESTAMP(0),PRIMARY KEY (PID));')
	script_file.close()
	
	#script to create table TRAFODION.odb_test.person1
	script_file = file("scripts/ddl_person1.sql", "w")
	script_file.write("drop table TRAFODION.odb_test.person;\n")
	script_file.write('CREATE TABLE TRAFODION.odb_test."PERSON1" (PID BIGINT SIGNED NOT NULL,FNAME CHAR(20) NOT NULL,LNAME CHAR(20) NOT NULL,COUNTRY VARCHAR(40) NOT NULL,CITY VARCHAR(40) NOT NULL,BDATE DATE NOT NULL,SEX CHAR(1) NOT NULL,EMAIL VARCHAR(40) NOT NULL,SALARY NUMERIC(9,2) NOT NULL,EMPL VARCHAR(40) NOT NULL,NOTES VARCHAR(80),LOADTS TIMESTAMP(0),PRIMARY KEY (PID));')
	script_file.close()

	#script to create table TRAFODION.odb_test.person2
	script_file = file("scripts/ddl_person2.sql", "w")
	script_file.write("drop table TRAFODION.odb_test.person2;\n")
	script_file.write('CREATE TABLE TRAFODION.odb_test."PERSON2" (PID BIGINT SIGNED NOT NULL,FNAME CHAR(20) NOT NULL,LNAME CHAR(20) NOT NULL,COUNTRY VARCHAR(40) NOT NULL,CITY VARCHAR(40) NOT NULL,BDATE DATE NOT NULL,SEX CHAR(1) NOT NULL,EMAIL VARCHAR(40) NOT NULL,SALARY NUMERIC(9,2) NOT NULL,EMPL VARCHAR(40) NOT NULL,NOTES VARCHAR(80),LOADTS TIMESTAMP(0),PRIMARY KEY (PID));')
	script_file.close()

	#script to create table TRAFODION.odb_test.person3
	script_file = file("scripts/ddl_person3.sql", "w")
	script_file.write("drop table TRAFODION.odb_test.person3;\n")
	script_file.write('CREATE TABLE TRAFODION.odb_test."PERSON3" (PID BIGINT SIGNED NOT NULL,FNAME CHAR(20) NOT NULL,LNAME CHAR(20) NOT NULL,COUNTRY VARCHAR(40) NOT NULL,CITY VARCHAR(40) NOT NULL,BDATE DATE NOT NULL,SEX CHAR(1) NOT NULL,EMAIL VARCHAR(40) NOT NULL,SALARY NUMERIC(9,2) NOT NULL,EMPL VARCHAR(40) NOT NULL,NOTES VARCHAR(80),LOADTS TIMESTAMP(0),PRIMARY KEY (PID));')
	script_file.close()

	#script to create schema odb_test_extract and table TRAFODION.odb_test_extract.person_e
	#this table is to test performance of different compression, should be large
	script_file = file("scripts/ddl_person_extract.sql", "w")
	script_file.write("create schema TRAFODION.ODB_TEST_EXTRACT;\n")
	script_file.write("drop table TRAFODION.odb_test_extract.person_e;")
	script_file.write('CREATE TABLE TRAFODION.odb_test_extract."PERSON_E" (PID BIGINT SIGNED NOT NULL,FNAME CHAR(20) NOT NULL,LNAME CHAR(20) NOT NULL,COUNTRY VARCHAR(40) NOT NULL,CITY VARCHAR(40) NOT NULL,BDATE DATE NOT NULL,SEX CHAR(1) NOT NULL,EMAIL VARCHAR(40) NOT NULL,SALARY NUMERIC(9,2) NOT NULL,EMPL VARCHAR(40) NOT NULL,NOTES VARCHAR(80),LOADTS TIMESTAMP(0),PRIMARY KEY (PID));')
	script_file.close()

	#script to create table TRAFODION.odb_test_extract.person1_e
	script_file = file("scripts/ddl_person1_extract.sql", "w")
	script_file.write("drop table TRAFODION.odb_test_extract.person1_e;\n")
	script_file.write('CREATE TABLE TRAFODION.odb_test_extract."PERSON1_E" (PID BIGINT SIGNED NOT NULL,FNAME CHAR(20) NOT NULL,LNAME CHAR(20) NOT NULL,COUNTRY VARCHAR(40) NOT NULL,CITY VARCHAR(40) NOT NULL,BDATE DATE NOT NULL,SEX CHAR(1) NOT NULL,EMAIL VARCHAR(40) NOT NULL,SALARY NUMERIC(9,2) NOT NULL,EMPL VARCHAR(40) NOT NULL,NOTES VARCHAR(80),LOADTS TIMESTAMP(0),PRIMARY KEY (PID));')
	script_file.close()
	
	#script to create table TRAFODION.odb_test_extract.person2_e
	script_file = file("scripts/ddl_person2_extract.sql", "w")
	script_file.write("drop table TRAFODION.odb_test_extract.person2_e;\n")
	script_file.write('CREATE TABLE TRAFODION.odb_test_extract."PERSON2_E" (PID BIGINT SIGNED NOT NULL,FNAME CHAR(20) NOT NULL,LNAME CHAR(20) NOT NULL,COUNTRY VARCHAR(40) NOT NULL,CITY VARCHAR(40) NOT NULL,BDATE DATE NOT NULL,SEX CHAR(1) NOT NULL,EMAIL VARCHAR(40) NOT NULL,SALARY NUMERIC(9,2) NOT NULL,EMPL VARCHAR(40) NOT NULL,NOTES VARCHAR(80),LOADTS TIMESTAMP(0),PRIMARY KEY (PID));')
	script_file.close()
	
	#script to create table TRAFODION.odb_test_extract.person3_e
	script_file = file("scripts/ddl_person3_extract.sql", "w")
	script_file.write("drop table TRAFODION.odb_test_extract.person3_e;\n")
	script_file.write('CREATE TABLE TRAFODION.odb_test_extract."PERSON3_E" (PID BIGINT SIGNED NOT NULL,FNAME CHAR(20) NOT NULL,LNAME CHAR(20) NOT NULL,COUNTRY VARCHAR(40) NOT NULL,CITY VARCHAR(40) NOT NULL,BDATE DATE NOT NULL,SEX CHAR(1) NOT NULL,EMAIL VARCHAR(40) NOT NULL,SALARY NUMERIC(9,2) NOT NULL,EMPL VARCHAR(40) NOT NULL,NOTES VARCHAR(80),LOADTS TIMESTAMP(0),PRIMARY KEY (PID));')
	script_file.close()

	#script to create table TRAFODION.odb_tes.person3_dup
	script_file = file("scripts/ddl_person3_dup_test.sql", "w")
	script_file.write("drop table TRAFODION.odb_test.person3_dup;\n")
	script_file.write('CREATE TABLE TRAFODION.odb_test."PERSON3_DUP" (PID BIGINT SIGNED NOT NULL,FNAME CHAR(20) NOT NULL,LNAME CHAR(20) NOT NULL,COUNTRY VARCHAR(40) NOT NULL,CITY VARCHAR(40) NOT NULL,BDATE DATE NOT NULL,SEX CHAR(1) NOT NULL,EMAIL VARCHAR(40) NOT NULL,SALARY NUMERIC(9,2) NOT NULL,EMPL VARCHAR(40) NOT NULL,NOTES VARCHAR(80),LOADTS TIMESTAMP(0),PRIMARY KEY (PID));')
	script_file.write('INSERT INTO TRAFODION.odb_test."PERSON3_DUP" VALUES (1,\'Fu-Jin\',\'Pu\',\'China\',\'Xian\',DATE\'1968-10-29\',\'U\',\'apple@qq.edu\',699649,\'Google\',\'bbbbbb\',TIMESTAMP\'2016-07-07 10:28:43\');')
	script_file.close()
	
	print "script create success"
	print
	
#create a random bithday date by timestamp
def create_random_birthday(start, end):
	birthday_stamp = random.randint(start, end)
	timeArray = time.localtime(birthday_stamp)
	birthday = time.strftime("%Y-%m-%d", timeArray)
	return birthday
	
#this function is to create files to test load 
#file load_data_1 ~ load_data_12 to test load with differnt separator
#file load_data_bad to test load with parameter bad
def prepare_load_data(sp, fs, num):
	list_fname = ['Jian-Guo', 'Li-Ru', 'Shi-Fu', 'Bo', 'Liang', 'Yun-Peng', 'Fu-Jin']
	list_lname = ['Zhang', 'Yu', 'Zhou', 'Pu', 'Wu', 'Luo']
	list_country = ['China', 'US', 'Canada', 'Japan', 'India']
	list_city = ['Beijing', 'Xian', 'Shanghai', 'Chongqing', 'Shenzheng', 'Guangzhou', 'Guiyang']
	list_sex = ['M', 'F', 'U']
	list_employ = ['Google', 'Esgyn', 'Microsoft', 'HP', 'Apple', 'Tencent']
	list_note = ['aaaa', 'bbbbbb', 'cccccc']
	list_loadts = time.time()
	file_name = 0
	ret = 0
	
	timeArray = time.localtime(list_loadts)
	list_loadts_str = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
	
	#create folder of load data file
	input_folder = 'odb_auto_loadfile'
	if os.path.exists(input_folder):
		if os.path.isfile(input_folder):
			os.remove(input_folder)
			os.mkdir(input_folder)
	else:
		os.mkdir(input_folder)

	if (sp == '!'):
		file_name = '1'
	elif (sp == '@'):
		file_name = '2'
	elif (sp == '#'):
		file_name = '3'	
	elif (sp == '$'):
		file_name = '4'
	elif (sp == '%'):
		file_name = '5'
	elif (sp == '^'):
		file_name = '6'
	elif (sp == '&'):
		file_name = '7'
	elif (sp == '*'):
		file_name = '8'	
	elif (sp == ','):
		file_name = '9'
	elif (sp == '.'):
		file_name = '10'
	elif (sp == '"'):
		file_name = '11'		
	elif (sp == '|'):
		file_name = '12'
	elif (sp == 'bad'):	# this file is to test load with parameter bad
		sp = ','
		file_name = 'bad'

	f = open(input_folder + '/load_data_' + file_name, 'w')
	for i in range(num):
		birthday = create_random_birthday(-2209017943, 1467648000) # create date between 1990-1-1 00:00:00 and 2016-7-5 00:00:00
		email = random.choice (['apple', 'pear', 'peach', 'orange', 'lemon']) + '@' + random.choice (['gmail', '163', 'sina', 'qq', 'esgyn']) + random.choice (['.com', '.cn', '.edu', '.org'])
		if (file_name == 'bad'):
			f.write(random.choice('123456789abcdefg') + sp + fs + random.choice(list_fname) + fs + sp + fs + random.choice(list_lname) + fs + sp + fs + random.choice(list_country) + fs + sp + fs + random.choice(list_city) + fs + sp + birthday + sp +  fs + random.choice(list_sex) + fs + sp + fs + email + fs + sp + str(random.randint(100000,2000000)) + sp + fs + random.choice(list_employ) + fs + sp + fs + random.choice(list_note) + fs + sp + list_loadts_str + '\n')
		else:
			f.write(str(i + 1) + sp + fs + random.choice(list_fname) + fs + sp + fs + random.choice(list_lname) + fs + sp + fs + random.choice(list_country) + fs + sp + fs + random.choice(list_city) + fs + sp + birthday + sp +  fs + random.choice(list_sex) + fs + sp + fs + email + fs + sp + str(random.randint(100000,2000000)) + sp + fs + random.choice(list_employ) + fs + sp + fs + random.choice(list_note) + fs + sp + list_loadts_str + '\n')
	f.close	
	return ret

	
#create tables to test extract 
#schema : odn_test_extract
#tables : person_e, person1_e, person2_e, person3_e
def prepare_extract_data():
	print "now create tables to test extract functions"
	print "please wait..."
	cmd = './odb64luo -u trafodion -p traf123 -d traf  -l src=odb_auto_loadfile/load_data_9:pre=@scripts/ddl_person_extract.sql:tgt=trafodion.odb_test_extract.person_e:max=1000000:rows=5000:parallel=5:loadcmd=UL:fs=,:sq=\\" -l src=odb_auto_loadfile/load_data_9:pre=@scripts/ddl_person1_extract.sql:tgt=trafodion.odb_test_extract.person1_e:max=1000:rows=5000:parallel=5:loadcmd=UL:fs=,:sq=\\" -l src=odb_auto_loadfile/load_data_9:pre=@scripts/ddl_person2_extract.sql:tgt=trafodion.odb_test_extract.person2_e:max=1000:rows=5000:parallel=5:loadcmd=UL:fs=,:sq=\\" -l src=odb_auto_loadfile/load_data_9:pre=@scripts/ddl_person3_extract.sql:tgt=trafodion.odb_test_extract.person3_e:max=1000:rows=5000:parallel=5:loadcmd=UL:fs=,:sq=\\"'
	(status, output) = commands.getstatusoutput(cmd)
	if status == 0:
		print "table to extract create success"
		return status
	else:
		print "table to extract create failed"
		print 
		return status
	
def execute_cmd(num, usecase, cmd):
	session_start = 0
	session_end = 0
	session_start = time.time()
	(status, output) = commands.getstatusoutput(cmd)
	session_end = time.time()
	logger_sumary.info('------------------ test : %d ------------------', num)
	logger_detail.info('------------------ test : %d ------------------', num)
	logger_sumary.info('Usecase : %s', usecase)
	logger_detail.info('Usecase : %s', usecase)
	logger_sumary.info('Command : %s', cmd)
	logger_detail.info('Command : %s', cmd)

	if status == 0:
		if output.find('Error') != -1:
			status = -1
			logger_sumary.info('Result : failed')
			logger_detail.info('Result : failed')
		else:
			logger_sumary.info('Result : success')
			logger_detail.info('Result : success')
	else:
		logger_sumary.info('Result : failed')
		logger_detail.info('Result : failed')
	
	logger_sumary.info('During : %ds\n', session_end - session_start)
	logger_detail.info('During : %ds', session_end - session_start)
	
	logger_detail.info('Details :\n%s\n', output)
	print
	return status

def main():
	global max_to_load
	global max_to_test_performance
	
	total_num = 0
	failed_num = 0
	ret = 0
	start_time = 0
	end_time = 0
	
	start_time = time.time()

	#test 1
	total_num += 1
	cmd = './odb64luo -u trafodion -p traf123 -d traf -i'
	usecase = 'Connect trafodion as the data source'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1

	#test 2
	total_num += 1
	cmd = './odb64luo  -u trafodion -p traf123 -d traf -i c'
	usecase = 'List catalogs'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1

	#test 3
	total_num += 1
	cmd = './odb64luo -u trafodion -p traf123 -d traf -i s:trafodion'
	usecase = 'List schemas in specific catalg'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1

	#test 4
	total_num += 1
	cmd = './odb64luo -lsdrv;./odb64luo -lsdsn'
	usecase = 'List Available ODBC Drivers and Data Sources'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1
	
	#test 5
	total_num += 1
	cmd = './odb64luo -u trafodion -p traf123 -d traf -x "select count(*) from trafodion.odb_test.person"'
	usecase = 'Run a sql command'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1 
	 
	#test 6
	total_num += 1
	cmd = './odb64luo -u trafodion -p traf123 -d traf -f scripts/script.sql'
	usecase = 'Run a sql script'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1

	#test 7
	total_num += 1
	cmd = './odb64luo -u trafodion -p traf123 -d traf -x "select count(*) from trafodion.odb_test.person1" -f scripts/script.sql'
	usecase = 'Parallelize multiple commands and scripts'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1

	#test 8
	total_num += 1
	cmd = './odb64luo -l src=nofile:pre=@scripts/ddl_person.sql:tgt=trafodion.odb_test.person:max=1000000:map=person.map:rows=5000:parallel=4:loadcmd=UL -u trafodion -p traf123 -d traf'
	usecase = 'Generating and Loading Data with map but no data file'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1
	
	#test 9
	total_num += 1
	cmd = './odb64luo -u trafodion -p traf123 -d traf  -l src=nofile:pre=@scripts/ddl_person1.sql:tgt=trafodion.odb_test.person1:max=10000:map=person1.map:rows=5000:parallel=5:loadcmd=UL -l src=nofile:pre=@scripts/ddl_person2.sql:tgt=trafodion.odb_test.person2:max=10000:map=person2.map:rows=5000:parallel=5:loadcmd=UL -l src=nofile:pre=@scripts/ddl_person3.sql:tgt=trafodion.odb_test.person3:max=10000:map=person3.map:rows=5000:parallel=5:loadcmd=UL'
	usecase = 'Generating and Loading multiple Data tables with map but no data file'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1

	#test 10
	total_num += 1
	cmd = './odb64luo -u trafodion -p traf123 -d traf  -l src=odb_auto_loadfile/load_data_1:pre=@scripts/ddl_person3.sql:tgt=trafodion.odb_test.person3:max=' + str(max_to_load) + ':rows=5000:parallel=5:loadcmd=UL:fs=!:sq=\\"'
	usecase = 'load with ! as sp'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1	

	#test 11
	total_num += 1
	cmd = './odb64luo -u trafodion -p traf123 -d traf  -l src=odb_auto_loadfile/load_data_2:pre=@scripts/ddl_person3.sql:tgt=trafodion.odb_test.person3:max=' + str(max_to_load) + ':rows=5000:parallel=5:loadcmd=UL:fs=@:sq=\\"'
	usecase = 'load with @ as sp'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1			

	#test 12
	total_num += 1
	cmd = './odb64luo -u trafodion -p traf123 -d traf  -l src=odb_auto_loadfile/load_data_3:pre=@scripts/ddl_person3.sql:tgt=trafodion.odb_test.person3:max=' + str(max_to_load) + ':rows=5000:parallel=5:loadcmd=UL:fs=#:sq=\\"'
	usecase = 'load with # as sp'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1			

	#test 13
	total_num += 1
	cmd = './odb64luo -u trafodion -p traf123 -d traf  -l src=odb_auto_loadfile/load_data_4:pre=@scripts/ddl_person3.sql:tgt=trafodion.odb_test.person3:max=' + str(max_to_load) + ':rows=5000:parallel=5:loadcmd=UL:fs=$:sq=\\"'
	usecase = 'load with $ as sp'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1	

	#test 14
	total_num += 1
	cmd = './odb64luo -u trafodion -p traf123 -d traf  -l src=odb_auto_loadfile/load_data_5:pre=@scripts/ddl_person3.sql:tgt=trafodion.odb_test.person3:max=' + str(max_to_load) + ':rows=5000:parallel=5:loadcmd=UL:fs=%:sq=\\"'
	usecase = 'load with % as sp'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1	
	
	#test 15
	total_num += 1
	cmd = './odb64luo -u trafodion -p traf123 -d traf  -l src=odb_auto_loadfile/load_data_6:pre=@scripts/ddl_person3.sql:tgt=trafodion.odb_test.person3:max=' + str(max_to_load) + ':rows=5000:parallel=5:loadcmd=UL:fs=^:sq=\\"'
	usecase = 'load with ^ as sp'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1	

	#test 16
	total_num += 1
	cmd = './odb64luo -u trafodion -p traf123 -d traf  -l src=odb_auto_loadfile/load_data_7:pre=@scripts/ddl_person3.sql:tgt=trafodion.odb_test.person3:max=' + str(max_to_load) + ':rows=5000:parallel=5:loadcmd=UL:fs=\\&:sq=\\"'
	usecase = 'load with & as sp'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1	

	#test 17
	total_num += 1
	cmd = './odb64luo -u trafodion -p traf123 -d traf  -l src=odb_auto_loadfile/load_data_8:pre=@scripts/ddl_person3.sql:tgt=trafodion.odb_test.person3:max=' + str(max_to_load) + ':rows=5000:parallel=5:loadcmd=UL:fs=*:sq=\\"'
	usecase = 'load with * as sp'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1	

	#test 18
	total_num += 1
	cmd = './odb64luo -u trafodion -p traf123 -d traf  -l src=odb_auto_loadfile/load_data_9:pre=@scripts/ddl_person3.sql:tgt=trafodion.odb_test.person3:max=' + str(max_to_load) + ':rows=5000:parallel=5:loadcmd=UL:fs=,:sq=\\"'
	usecase = 'load with , as sp'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1	

	#test 19
	total_num += 1
	cmd = './odb64luo -u trafodion -p traf123 -d traf  -l src=odb_auto_loadfile/load_data_10:pre=@scripts/ddl_person3.sql:tgt=trafodion.odb_test.person3:max=' + str(max_to_load) + ':rows=5000:parallel=5:loadcmd=UL:fs=.:sq=\\"'
	usecase = 'load with . as sp'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1	

	#test 20
	total_num += 1
	cmd = './odb64luo -u trafodion -p traf123 -d traf  -l src=odb_auto_loadfile/load_data_11:pre=@scripts/ddl_person3.sql:tgt=trafodion.odb_test.person3:max=' + str(max_to_load) + ':rows=5000:parallel=5:loadcmd=UL:fs=\\":sq=\\"'
	usecase = 'load with " as sp'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1	

	#test 21
	total_num += 1
	cmd = './odb64luo -u trafodion -p traf123 -d traf -l src=odb_auto_loadfile/load_data_12:pre=@scripts/ddl_person3.sql:tgt=trafodion.odb_test.person3:max=' + str(max_to_load) + ':rows=5000:parallel=5:loadcmd=UL:fs=\\|:sq=\\"'
	usecase = 'load with | as sp'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1	
	
	#test 22
	total_num += 1
	cmd = './odb64luo -u trafodion -p traf123 -d traf -l src=odb_auto_loadfile/load_data_bad:pre=@scripts/ddl_person3.sql:tgt=trafodion.odb_test.person3:max=' + str(max_to_load) + ':rows=5000:parallel=5:loadcmd=IN:fs=,:sq=\\":bad=output_data/bad_records -v'
	usecase = 'Load bad records and output specified bad records output file'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1	

	#test 23
	total_num += 1
	cmd = './odb64luo -u trafodion -p traf123 -d traf -l src=odb_auto_loadfile/load_data_9:pre=@scripts/ddl_person3_dup_test.sql:tgt=trafodion.odb_test.person3_dup:max=' + str(max_to_load) + ':rows=5000:parallel=5:loadcmd=UL:fs=,:sq=\\" -v'
	usecase = 'Load duplicate data using upsert can successfully'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1	

	#test 24
	total_num += 1
	cmd = './odb64luo -u trafodion -p traf123 -d traf -l src=odb_auto_loadfile/load_data_9:pre=@scripts/ddl_person3_dup_test.sql:tgt=trafodion.odb_test.person3_dup:max=' + str(max_to_load) + ':rows=5000:parallel=5:loadcmd=IN:fs=,:sq=\\" -v'
	usecase = 'Load duplicate data using insert will fail'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1	

	#test 25
	total_num += 1
	cmd = './odb64luo -u trafodion -p traf123 -d traf -l src=odb_auto_loadfile/load_data_9:pre=@scripts/ddl_person3.sql:tgt=trafodion.odb_test.person3:max=' + str(max_to_test_performance) + ':rows=5000:parallel=5:loadcmd=IN:fs=,:sq=\\" -v'
	usecase = 'Performance with loadcmd IN(insert)'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1	

	#test 26
	total_num += 1
	cmd = './odb64luo -u trafodion -p traf123 -d traf -l src=odb_auto_loadfile/load_data_9:pre=@scripts/ddl_person3.sql:tgt=trafodion.odb_test.person3:max=' + str(max_to_test_performance) + ':rows=5000:parallel=5:loadcmd=UP:fs=,:sq=\\" -v'
	usecase = 'Performance with loadcmd UP(upsert)'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1	

	#test 27
	total_num += 1
	cmd = './odb64luo -u trafodion -p traf123 -d traf -l src=odb_auto_loadfile/load_data_9:pre=@scripts/ddl_person3.sql:tgt=trafodion.odb_test.person3:max=' + str(max_to_test_performance) + ':rows=5000:parallel=5:loadcmd=UL:fs=,:sq=\\" -v'
	usecase = 'Performance with loadcmd UL(upsert using load)'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1		
	
	#test 28
	total_num += 1
	cmd = './odb64luo -u trafodion -p traf123 -d traf -l src=odb_auto_loadfile/load_data_9:pre=@scripts/ddl_person3.sql:tgt=trafodion.odb_test.person3:max=' + str(max_to_test_performance) + ':rows=5000:parallel=2:loadcmd=UL:fs=,:sq=\\" -v'
	usecase = 'Performance of loading with 2 parallel threads'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1
	
	#test 29
	total_num += 1
	cmd = './odb64luo -u trafodion -p traf123 -d traf -l src=odb_auto_loadfile/load_data_9:pre=@scripts/ddl_person3.sql:tgt=trafodion.odb_test.person3:max=' + str(max_to_test_performance) + ':rows=5000:parallel=4:loadcmd=UL:fs=,:sq=\\" -v'
	usecase = 'Performance of loading with 4 parallel threads'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1

	#test 30
	total_num += 1
	cmd = './odb64luo -u trafodion -p traf123 -d traf -l src=odb_auto_loadfile/load_data_9:pre=@scripts/ddl_person3.sql:tgt=trafodion.odb_test.person3:max=' + str(max_to_test_performance) + ':rows=5000:parallel=100:loadcmd=UL:fs=,:sq=\\" -v'
	usecase = 'Performance of loading with parallel threads more than avalible mxosrvrs'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1
	
	#test 31
	total_num += 1
	cmd = './odb64luo -u trafodion -p traf123 -d traf -l src=odb_auto_loadfile/load_data_9:pre=@scripts/ddl_person3.sql:tgt=trafodion.odb_test.person3:max=' + str(max_to_test_performance) + ':rows=1000:parallel=4:loadcmd=UL:fs=,:sq=\\" -v'
	usecase = 'Performance using 1000 as rowset'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1

	#test 32
	total_num += 1
	cmd = './odb64luo -u trafodion -p traf123 -d traf -l src=odb_auto_loadfile/load_data_9:pre=@scripts/ddl_person3.sql:tgt=trafodion.odb_test.person3:max=' + str(max_to_test_performance) + ':rows=5000:parallel=4:loadcmd=UL:fs=,:sq=\\" -v'
	usecase = 'Performance using 5000 as rowset'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1

	#test 33
	total_num += 1
	cmd = './odb64luo -u trafodion -p traf123 -d traf -l src=odb_auto_loadfile/load_data_9:pre=@scripts/ddl_person3.sql:tgt=trafodion.odb_test.person3:max=' + str(max_to_test_performance) + ':rows=10000:parallel=4:loadcmd=UL:fs=,:sq=\\" -v'
	usecase = 'Performance using 10000 as rowset'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1
	
	#test 34
	total_num += 1
	cmd = './odb64luo -u trafodion -p traf123 -d traf -e src=trafodion.odb_test_extract.person_e:tgt=output_data/ext_nor_%t.csv:rows=m10:fs=,:trim:sq=\\"'
	usecase = 'Extract single table using default format'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1	
		
	#test 35
	total_num += 1
	cmd = './odb64luo -u trafodion -p traf123 -d traf -e src=trafodion.odb_test_extract.person_e:tgt=output_data/ext_xml_%t.csv:rows=m10:fs=,:trim:sq=\\":xml'
	usecase = 'Extract single table using xml format'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1	
		
	#test 36
	total_num += 1
	cmd = './odb64luo -u trafodion -p traf123 -d traf -e src=trafodion.odb_test_extract.person_e:tgt=output_data/ext_gzip_%t.csv.gz:rows=m10:fs=,:trim:sq=\\":gzip'
	usecase = 'Extract single table using gzip format'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1			
	
	#test 37
	total_num += 1
	cmd = './odb64luo -u trafodion -p traf123 -d traf -e src=-conf/export_tbl_list:tgt=output_data/ext_multi_%t.csv:rows=m10:fs=,:trim:sq=\\"'
	usecase = 'Extract multiple data tables using default format'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1	
	
	#test 38
	total_num += 1
	cmd = './odb64luo -u trafodion -p traf123 -d traf -e src=-conf/export_tbl_list:tgt=output_data/ext_multi_sub_%t.csv:rows=m10:fs=,:trim:sq=\\":pwhere=[country=\\\'Canada\\\'] -v'
	usecase = 'Extract interested subset of rows by pwhere'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1	

	#test 39
	total_num += 1
	cmd = './odb64luo -u trafodion -p traf123 -d traf -e src=trafodion.odb_test_extract.person_e:tgt=output_data/ext_wb9_gzip_%t.csv.gz:rows=m10:fs=,:trim:sq=\\":gzip:gzpar=wb9'
	usecase = 'Extract with compression algorithm wb9'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1	

	#test 40
	total_num += 1
	cmd = './odb64luo -u trafodion -p traf123 -d traf -e src=trafodion.odb_test_extract.person_e:tgt=output_data/ext_wb1_gzip_%t.csv.gz:rows=m10:fs=,:trim:sq=\\":gzip:gzpar=wb1'
	usecase = 'Extract with compression algorithm wb1'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1	

	#test 41
	total_num += 1
	cmd = './odb64luo -u trafodion -p traf123 -d traf -e src=trafodion.odb_test_extract.person_e:tgt=output_data/ext_wb6h_gzip_%t.csv.gz:rows=m10:fs=,:trim:sq=\\":gzip:gzpar=wb6h'
	usecase = 'Extract with compression algorithm wb6h'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1			
		
	#test 42
	total_num += 1
	cmd = './odb64luo -u trafodion -p traf123 -d traf -e src=trafodion.odb_test_extract.person_e:tgt=output_data/ext_wb6R_gzip_%t.csv.gz:rows=m10:fs=,:trim:sq=\\":gzip:gzpar=wb6R'
	usecase = 'Extract with compression algorithm wb6R'
	ret = execute_cmd(total_num, usecase, cmd)
	if ret != 0:
		failed_num += 1			
		
		
	end_time = time.time()
	logger_detail.info('======================================================================================\nTOTAL : %d\nFASSED : %d\nFAILED : %d\nDURING : %ds\n', 
	total_num, total_num - failed_num, failed_num, end_time - start_time)
	logger_sumary.info('======================================================================================\nTOTAL : %d\nFASSED : %d\nFAILED : %d\nDURING : %ds\n', 
	total_num, total_num - failed_num, failed_num, end_time - start_time)

create_script()

#create date file to test load
print "now create files to test load "
print "please wait..."
sp = ['!', '@', '#', '$', '%', '^', '&', '*', '.', '"', '|', 'bad']
for i in sp :
	ret = prepare_load_data(i, '"', 10000)
	if ret != 0:
		break

#create a large file to test performance 		
prepare_load_data(',', '"', 1000000)
print "files create success"
print 

#create tables to test extract
ret = prepare_extract_data()
if ret != 0:
	print "create tables to extract faied, exit."
	os._exit(0)

# create config file determine the list of src to be extracted
create_config_file()

#start test
print "======================================================================================"
print "now start odb test ..."
print 
main()
