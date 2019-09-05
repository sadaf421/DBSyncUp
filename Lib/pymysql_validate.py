import mysql.connector
import pandas as pd
import colorama
import sys
from colorama import Fore
colorama.init()

def diffList(li1, li2):
    li_dif = [i for i in li1 + li2 if i not in li1 or i not in li2]
    return li_dif

def checkTableExists(dbcon, Table_name,Table_schema,Table_Type):
	print(Fore.WHITE +"Validating ",Table_Type," table ",Table_name," :  " , end =" ")
	val_query="select count(*) from information_schema.tables where table_name='"+ Table_name +"' and table_schema= '" + Table_schema +"' AND TABLE_TYPE='BASE TABLE'"
	db_cur=dbcon.cursor()
	db_cur.execute(val_query)
	if db_cur.fetchone()[0] == 1:
		db_cur.close()
		print(Fore.GREEN +"Passed")
		return True
	db_cur.close()
	return False

def checkTableMdata(dbcon_src, Table_name_src,source_schema,dbcon_trg, Table_name_trg,target_schema):
	val_query_Src="select column_name,data_type from information_schema.columns where table_name='"+ Table_name_src +"' and table_schema= '" + source_schema +"' ORDER BY ORDINAL_POSITION"
	val_query_trg="select column_name,data_type from information_schema.columns where table_name='"+ Table_name_trg +"' and table_schema= '" + target_schema +"' ORDER BY ORDINAL_POSITION"
	db_cur_src=dbcon_src.cursor()
	db_cur_src.execute(val_query_Src)
	sql_data_src=pd.DataFrame(db_cur_src.fetchall())
	db_cur_trg=dbcon_trg.cursor()
	db_cur_trg.execute(val_query_trg)
	sql_Data_trg=pd.DataFrame(db_cur_trg.fetchall())
	db_cur_src.close()
	db_cur_trg.close()
	print(Fore.WHITE +"Validating meta data of source(",Table_name_src,") and target(",Table_name_trg,") :  ", end =" ")
	if(sql_data_src[0].shape!=sql_Data_trg[0].shape):
		print(Fore.RED +"\nValidation Error : Column count does not match between the source and target table.")
		return False
	elif((sql_data_src[0]).equals(sql_Data_trg[0])== False):
		print(Fore.RED +"\nValidation Error : Columns or column order do not match between the source and target table.")
		return False
	elif((sql_data_src[[0,1]]).equals(sql_Data_trg[[0,1]])== False):
		print(Fore.RED +"\nValidation Error : Column daya types are not same")
		return False
	print(Fore.GREEN +"passed")
	return True

def getColumnNames(dbcon_src, Table_name_src,source_schema):
	val_query_Src="select column_name from information_schema.columns where table_name='"+ Table_name_src +"' and table_schema= '" + source_schema +"' ORDER BY ORDINAL_POSITION"
	db_cur_src=dbcon_src.cursor()
	db_cur_src.execute(val_query_Src)
	sql_data_src=pd.DataFrame(db_cur_src.fetchall())
	db_cur_src.close()
	sql_data_src.set_axis(['columnName'], axis=1, inplace=True)
	return(list(sql_data_src.columnName))

def listToString(lst):
	str1 = ""
	for ele in lst:
		str1 = str1 + ele +','
	return str1[:-1]

def FetchSourceCount(dbcon, Table_name,Table_schema):
	# No need to check for full count
	Count_query="SELECT count(*) FROM "+Table_schema+"."+Table_name+" LIMIT 100 "
	db_cur=dbcon.cursor()
	db_cur.execute(Count_query)
	Row_Count=db_cur.fetchone()
	db_cur.close()
	if Row_Count[0] == 0:
		print(Fore.RED +'\nValidation Error : ',Table_schema,".",Table_name," table is empty.")
		return False
	return Row_Count[0]

def FetchComparisonKey(dbcon, Table_name,Table_schema,Table_Type):
	Pri_key=0
	print(Fore.WHITE +"Fetching ",Table_Type," Comparison(primary) key 	: ", end =" ")
	# Pri_key_query="select ifnull(group_concat(column_name),'0') from information_schema.columns where table_name='"+ Table_name +"' and table_schema= '" + Table_schema +"' and column_key='pri'"
	Pri_key_query="SELECT k.column_name FROM information_schema.table_constraints t JOIN information_schema.key_column_usage k USING(constraint_name,table_schema,table_name) WHERE t.constraint_type='PRIMARY KEY' AND t.table_schema='" + Table_schema + "' AND t.table_name='" + Table_name + "' ORDER BY ORDINAL_POSITION"
	db_cur=dbcon.cursor()
	db_cur.execute(Pri_key_query)
	sql_key_cols=pd.DataFrame(db_cur.fetchall())
	db_cur.close()
	if  sql_key_cols.empty:
		print(Fore.RED +'\nValidation Error : Primary key was not found for ',Table_schema,'.',Table_name)
		sys.exit(1)
	print(Fore.GREEN +"Done")
	return sql_key_cols

def FetchIterator(dbcon_src, Table_name_src,source_schema,dbcon_trg, Table_name_trg,target_schema):
	Count_query_Src="SELECT count(*) FROM "+source_schema+"."+Table_name_src
	Count_query_Trg="SELECT count(*) FROM "+target_schema+"."+Table_name_trg
	db_cur_src=dbcon_src.cursor()
	db_cur_src.execute(Count_query_Src)
	sql_data_src=db_cur_src.fetchone()[0]
	db_cur_trg=dbcon_trg.cursor()
	db_cur_trg.execute(Count_query_Trg)
	sql_Data_trg=db_cur_trg.fetchone()[0]
	db_cur_src.close()
	db_cur_trg.close()
	if sql_data_src == 0:
		print(Fore.RED +'\nValidation Error : ',Table_schema,".",Table_name," was found empty.")
		return -1
	elif sql_data_src>sql_Data_trg:
		return sql_data_src
	else:
		return sql_Data_trg

def FetchMinValue(dbcon_src, Table_name_src,source_schema,dbcon_trg, Table_name_trg,target_schema,KeyColumns):
	column_list=""
	column_list_order=""
	for item in KeyColumns:
		column_list = column_list + item + ","
		column_list_order = column_list_order + item + " ASC,"
	column_list=column_list[:-1]
	column_list_order=column_list_order[:-1]
	Count_query_Src="SELECT " + column_list +  " FROM "+source_schema+"."+Table_name_src + " ORDER BY " + column_list_order + " LIMIT 1";
	Count_query_Trg="SELECT " + column_list +  " FROM "+target_schema+"."+Table_name_trg + " ORDER BY " + column_list_order + " LIMIT 1";
	sql_data_src= pd.read_sql_query(Count_query_Src ,dbcon_src,parse_dates=True)
	sql_data_trg= pd.read_sql_query(Count_query_Trg ,dbcon_trg,parse_dates=True)
	return pd.concat([sql_data_src,sql_data_trg],ignore_index=True).sort_values(sorted(sql_data_src),ascending=True).head(1)

def FetchMaxValue(dbcon_src, Table_name_src,source_schema,dbcon_trg, Table_name_trg,target_schema,KeyColumns):
	column_list=""
	column_list_order=""
	for item in KeyColumns:
		column_list = column_list + item + ","
		column_list_order = column_list_order + item + " DESC,"
	column_list=column_list[:-1]
	column_list_order=column_list_order[:-1]
	Count_query_Src="SELECT " + column_list +  " FROM "+source_schema+"."+Table_name_src + " ORDER BY " + column_list_order + " LIMIT 1";
	Count_query_Trg="SELECT " + column_list +  " FROM "+target_schema+"."+Table_name_trg + " ORDER BY " + column_list_order + " LIMIT 1";
	sql_data_src= pd.read_sql_query(Count_query_Src ,dbcon_src,parse_dates=True)
	sql_data_trg= pd.read_sql_query(Count_query_Trg ,dbcon_trg,parse_dates=True)
	return pd.concat([sql_data_src,sql_data_trg],ignore_index=True).sort_values(sorted(sql_data_src),ascending=False).head(1)
