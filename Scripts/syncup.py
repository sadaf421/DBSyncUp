import sys
import pandas as pd
import datetime
import mysql.connector
import numpy as np
import time
import os
import inspect
from mysql.connector import errorcode
import colorama
from colorama import Fore
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)
from Lib import pymysql_connector
from Lib import pymysql_validate

colorama.init()
start_time = time.time()
source_schema=pymysql_connector.schema_src()
target_schema=pymysql_connector.schema_trg()
source_server=pymysql_connector.server_src()
target_server=pymysql_connector.server_trg()
Source_DB=mysql.connector
Target_DB=mysql.connector

insertFile=os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(__file__))), 'SQL', r''+target_server+'_'+sys.argv[1]+'_'+'insert'+'.SQL')
deleteFile=os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(__file__))), 'SQL', r''+target_server+'_'+sys.argv[1]+'_'+'delete'+'.SQL')
updateFile=os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(__file__))), 'SQL', r''+target_server+'_'+sys.argv[1]+'_'+'update'+'.SQL')

try:
	print(Fore.WHITE + "\n\nConnecting source server (",source_server,") : ", end =" ")
	Source_DB=pymysql_connector.connect_src()
	print(Fore.GREEN +"Done")
	print(Fore.WHITE +"Connecting Target server (",target_server,") : ", end =" ")
	Target_DB=pymysql_connector.connect_trg()
	print(Fore.GREEN +"Done")
	print(Fore.YELLOW +'\n------------ Validation Begin --------------- \n')
	print(Fore.WHITE +'\n')
	if(pymysql_validate.checkTableExists(Source_DB,sys.argv[1],source_schema,"source")== False):
		print(Fore.RED +"\nValidation Error : Table does not exist in source schema")
		sys.exit(1)
	elif(pymysql_validate.checkTableExists(Target_DB,sys.argv[2],target_schema,"Target")== False):
		print(Fore.RED +"\nValidation Error : Table does not exist in Target schema")
		sys.exit(1)
	elif(pymysql_validate.checkTableMdata(Source_DB,sys.argv[1],source_schema,Target_DB,sys.argv[2],target_schema)==False):
		sys.exit(1)
	elif(pymysql_validate.FetchSourceCount(Source_DB,sys.argv[1],source_schema)==False):
		sys.exit(1)
	else :
		ComaprisonKeySrc=pymysql_validate.FetchComparisonKey(Source_DB,sys.argv[1],source_schema,"source")
		ComaprisonKeyTrg=pymysql_validate.FetchComparisonKey(Target_DB,sys.argv[2],target_schema,"Target")
		if ((ComaprisonKeySrc[0]).equals(ComaprisonKeyTrg[0])== False):
			print(Fore.RED+"\nValidation Error : Data cannot be compared, Primary Keys between the source and target tables are different")
			sys.exit(1)
		print(Fore.GREEN+"\nPrimary key matched between source and target tables")
		print(Fore.YELLOW +'\n\n------------ Validation Ends --------------- \n\n')
		ComaprisonKeyList=ComaprisonKeySrc.astype(str).values.flatten().tolist()
		print(Fore.WHITE +"Fetching Data Size 		: ", end =" ")
		sql_data_cnvrt=pymysql_validate.FetchIterator(Source_DB,sys.argv[1],source_schema,Target_DB,sys.argv[2],target_schema)
		if sql_data_cnvrt == -1:
			print(Fore.RED +"\nSorce Table empty")
			sys.exit(1)
		else:
			print(sql_data_cnvrt)
except mysql.connector.Error as err:
	print(Fore.RED +'\n------------ MYSQL CONNECTION ERROR --------------- \n\n')
	print(Fore.RED +"Error Code   :   ", err.errno)
	print(Fore.RED +"SQLSTATE     :   ", err.sqlstate)
	print(Fore.RED +"Message      :   ", err.msg)
	Source_DB.close()
	Target_DB.close()
	sys.exit(1)

chunksize=10

# Initiate dataframes
sql_data_src = pd.DataFrame()
sql_data_trg = pd.DataFrame()
update_record= pd.DataFrame()
insert_record= pd.DataFrame()
delete_record= pd.DataFrame()
merged_df=pd.DataFrame()
sql_data_update= pd.DataFrame()
sql_data_update1= pd.DataFrame()

# create empty files and overwrite if any
sql_data_src.to_csv(insertFile, sep='\t',index=False,header=False)
sql_data_src.to_csv(deleteFile, sep='\t',index=False,header=False)
sql_data_src.to_csv(updateFile, sep='\t',index=False,header=False)

# get column names list
columnlistfull=pymysql_validate.getColumnNames(Source_DB,sys.argv[1],source_schema)
columnlistfullstr=pymysql_validate.listToString(columnlistfull)

# Get start and end key values
min_key_val=pymysql_validate.FetchMinValue(Source_DB,sys.argv[1],source_schema,Target_DB,sys.argv[2],target_schema,ComaprisonKeyList)
max_key_val=pymysql_validate.FetchMaxValue(Source_DB,sys.argv[1],source_schema,Target_DB,sys.argv[2],target_schema,ComaprisonKeyList)

Source_DB.close()
Target_DB.close()

# Geterate queries and get data
startkey=min_key_val
iterationcnt=1
print(Fore.YELLOW +'\n\n------------ Data Comparison Begin --------------- \n\n')
while True:
	try:
		Source_DB.connect()
		Target_DB.connect()
		# Source query
		src_query="select " + columnlistfullstr + " from "+sys.argv[1] +" where 1=1 "
		filterstr=" ("
		orderstr=" ORDER BY "
		if iterationcnt==1:
			for key, val in startkey.iloc[0].iteritems():
				filterstr= filterstr + " " + key + " >= '" + str(val) + "' AND"
				orderstr=orderstr+" "+key+ " ASC ,"
		else:
			for key, val in startkey.iloc[0].iteritems():
				orderstr=orderstr+" "+key+ " ASC ,"
				filterstr2=" ("
				for key1, val1 in startkey.iloc[0].iteritems():
					if key == key1:
						filterstr2= filterstr2 + " " + key1 + " > '" + str(val1) + "' AND"
						break
					else:
						filterstr2= filterstr2 + " " + key1 + " >= '" + str(val1) + "' AND"
				filterstr= filterstr + filterstr2[:-4:]  + " ) OR "
				orderstr=orderstr+" "+key+ " ASC ,"
		src_query=src_query+" AND "+ filterstr[:-4] + " ) " +  orderstr[:-2] + " LIMIT "+str(chunksize)
		#print(Fore.CYAN + "\n Source Query:" + src_query)
		sql_data_src= pd.read_sql_query(src_query ,Source_DB,parse_dates=True)
		# Target query
		if sql_data_src.empty == False:
			src_max_key=sql_data_src[-1:]
		print(Fore.CYAN + "\nData from :" + startkey.to_string(header=False,
                  index=False,
                  index_names=False) + " to :" + src_max_key[ComaprisonKeyList].to_string(header=False,
		                    index=False,
		                    index_names=False))
		trg_query="select " + columnlistfullstr + " from "+sys.argv[2] +" where 1=1 "
		filterstr1=" ( "
		if sql_data_src.empty:
			filterstr1=  " LIMIT "  + str(chunksize)
			trg_query=trg_query+" AND "+ filterstr[:-4] + " ) " +orderstr[:-2] + filterstr1
		else:
			itrcnt = 1
			for key, val in src_max_key[ComaprisonKeyList].iloc[0].iteritems():
				filterstr3= " ( "
				for key1, val1 in src_max_key[ComaprisonKeyList].iloc[0].iteritems():
					if key == key1:
						if len(ComaprisonKeyList) == 1:
							filterstr3= filterstr3 + " " + key1 + " <= '" + str(val1) + "' AND"
						else:
							if itrcnt ==1:
								filterstr3= filterstr3 + " " + key1 + " < '" + str(val1) + "' AND"
							else:
								filterstr3= filterstr3 + " " + key1 + " <= '" + str(val1) + "' AND"
							itrcnt=itrcnt+1
						break
					else:
						filterstr3= filterstr3 + " " + key1 + " = '" + str(val1) + "' AND"
				filterstr1= filterstr1 + filterstr3[:-4:]  + " ) OR "
			trg_query=trg_query+" AND "+ filterstr[:-4] +" ) AND " + filterstr1[:-4] + " ) " + orderstr[:-2]
		# print(Fore.CYAN + "\n Target Query:" + trg_query)
		sql_data_trg= pd.read_sql_query(trg_query ,Target_DB,parse_dates=True)
		Source_DB.close()
		Target_DB.close()
	except mysql.connector.Error as err:
		print(Fore.RED +'------------ MYSQL CONNECTION ERROR --------------- \n\n')
		print(Fore.RED +"Error Code   :   ", err.errno)
		print(Fore.RED +"SQLSTATE     :   ", err.sqlstate)
		print(Fore.RED +"Message      :   ", err.msg)
		Source_DB.close()
		Target_DB.close()
		sys.exit(1)
	# If source and target return no records, exit
	if sql_data_trg.empty and sql_data_src.empty:
		print(Fore.GREEN +'------------ Completed --------------- \n\n')
		sys.exit(0)

	# Get insert statements
	print(Fore.WHITE +"Generating insert statements :  ", end =" ")
	sql_data_src=sql_data_src.astype('str')
	sql_data_trg=sql_data_trg.astype('str')
	merged_df=sql_data_src.merge(sql_data_trg, on=ComaprisonKeyList, how='outer', indicator=True, suffixes=('','_del'))
	insert_record=merged_df[merged_df['_merge']=='left_only'][columnlistfull]
	column_name=(', '.join(map(str, insert_record.columns.tolist())))
	insert_string_begin='insert into '+sys.argv[2]+'('+column_name+')values('
	insert_string_end=')'
	Total_col=insert_record.shape[1]-1
	first_col=(insert_record.columns[0])
	last_Col=(insert_record.columns[Total_col])
	if insert_record.empty == True :
		print(Fore.RED +"No records found to Insert")
	else :
		insert_record=insert_record.apply(lambda x:  "'"+x+"',"	, axis = 1)
		insert_record=insert_record.replace([r'^\s*$',"'nan'","'NaT'","'None'"], ["''","NULL","NULL","NULL"], regex=True)
		insert_record[first_col]=insert_string_begin+insert_record[first_col]
		insert_record[last_Col]=insert_record[last_Col].replace(",",");",regex=True)
		insert_record.to_csv(insertFile, sep='\t',index=False,header=False,mode='a')
		print(Fore.GREEN +"Done")
	del insert_record

	# Get delete statements
	print(Fore.WHITE +"Generating delete statements :  ", end =" ")
	delete_record=merged_df[merged_df['_merge']=='right_only'][ComaprisonKeyList]
	if delete_record.empty == True :
		print(Fore.RED +"No records found to delete")
	else :
		delete_string_begin='delete from '+sys.argv[2] +' where 1=1 '
		delete_record=delete_record.apply(lambda x : 'and '+x.name+"= '"+x+"'")
		delete_record.insert(0, 'del_string', delete_string_begin)
		delete_record.insert(len(ComaprisonKeyList)+1, 'delete_end', ";")
		delete_record.to_csv(deleteFile, sep='\t',index=False,header=False,mode='a')
		print(Fore.GREEN +"Done")
	del delete_record

	# Generate update statements
	print(Fore.WHITE +"Generating Update statements :  ", end =" ")
	othercols=pymysql_validate.diffList(columnlistfull,ComaprisonKeyList)
	if len(othercols) > 0:
		dffilterstr1=""
		dict1=dict({})
		for item in othercols:
			dict1[item ] = item+"_del"
			sql_data_update1=merged_df[(merged_df['_merge']=='both')]
			query =' | '.join(['{} != {}'.format(k, v) for k, v in dict1.items()])
			if len(othercols) > 0:
				sql_data_update=sql_data_update1[sql_data_update1.eval(query)][columnlistfull]
				update_record=pd.concat([update_record, sql_data_update], axis= 0)
				if update_record.empty == True :
					print(Fore.RED +"No records found to update")
				else :
					update_condition=update_record[ComaprisonKeyList]
					update_condition=update_condition.apply(lambda x : ' and '+x.name+"= '"+x+"'").astype(str).apply(''.join,1)
					update_record=update_record.apply(lambda x:  "'"+ x +"'," ,axis=1 )
					update_record=update_record.replace([r'^\s*$',"'nan'","'NaT'","'None'"], ["''","NULL","NULL","NULL"], regex=True)
					update_string_begin='update '+ sys.argv[2] + " set "
					update_record=update_record.drop(ComaprisonKeyList,axis=1)
					Total_col=update_record.shape[1]-1
					update_record=update_record.apply(lambda x:  x.name +"=" +x  )
					update_record.insert(0, 'update', update_string_begin)
					update_record[update_record.columns[Total_col+1]].replace(',',' where 1=1 ',regex=True,inplace=True)
					update_record.insert(Total_col+2, 'update_clause', update_condition)
					update_record.insert(Total_col+3, 'update_end', ";")
					update_record[update_record.columns[Total_col+1]].replace(',','',regex=True,inplace=True)
					update_record.to_csv(updateFile, sep='\t',index=False,header=False,mode='a')
					print(Fore.GREEN +"Done")
	if sql_data_src.empty == True:
		src_max_key=sql_data_trg[-1:]
	del sql_data_update
	del sql_data_update1
	del sql_data_src
	del sql_data_trg
	del update_record
	sql_data_src = pd.DataFrame()
	sql_data_trg = pd.DataFrame()
	sql_data_update=pd.DataFrame()
	sql_data_update1=pd.DataFrame()
	delete_record = pd.DataFrame()
	insert_record = pd.DataFrame()
	update_record = pd.DataFrame()
	startkey = src_max_key[ComaprisonKeyList]
	iterationcnt=iterationcnt+1
print(Fore.YELLOW +'\n\n------------ Data Comparison Ends --------------- \n\n')
print(Fore.YELLOW +"---Total execution Time 		: %s seconds ---" % (time.time() - start_time))
