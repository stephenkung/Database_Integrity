## team7, homework, by Liming Gong.
import psycopg2
import sys
import re
import sys
import os
import argparse
 
parser = argparse.ArgumentParser(description='Database Integrity check')
parser.add_argument('cmd',  default='',type=str, help="input schema file")

def parse_input(infile):
	table_list = []
	pk_list = []  #list of list, each element is a list of pks
	fk_list = []  #list of list of map, each element is a list of fks, each fk contains table_name:keyname 
	fk_self_list = [] #list of list, each element is a list of fk names in its table
	with open(infile,'r') as ff:
		for line in ff:
			temp_pk_list = []
			temp_fk_list = []
			temp_fk_self_list = []
			tbname_match = re.compile('^(\w+)(\s+)')
			pk_match = re.compile('(\(|,)(\s*)(\w+)(\s*)\(pk\)')
			fk_match = re.compile('fk:(\s*)(\w+)\.(\w+)(\s*)\)')
			fk_self_match = re.compile('(,\s*)(\w+)(\s*)\(fk')
			for match in tbname_match.finditer(line):
				#print("Table name match:",match.group(1))
				table_list.append(match.group(1))
			for match in pk_match.finditer(line):
				#print("PK match:",match.group(3))
				temp_pk_list.append(match.group(3))
			for match in fk_match.finditer(line):
				#print("FK match, table name:",match.group(2),",PK name:",match.group(3))
				temp_fk_list.append({match.group(2):match.group(3)})
			for match in fk_self_match.finditer(line):
				#print("FK self match, column name:",match.group(2))
				temp_fk_self_list.append(match.group(2))
			pk_list.append(temp_pk_list)
			fk_list.append(temp_fk_list)
			fk_self_list.append(temp_fk_self_list)
	assert(len(table_list)==len(pk_list))
	assert(len(fk_list)==len(pk_list))
	assert(len(fk_list)==len(fk_self_list))
	return table_list, pk_list, fk_list, fk_self_list
			
#check a given key attribute exist or not
def sanity_check(table_list, pk_list, fk_self_list, cur, outfile):
	tb_real_columns=[]
	ff=open(outfile,"w")
	for i in range(len(table_list)):
		sql = "select column_name from information_schema.columns where table_name="+"\'"+table_list[i].lower()+"\'"+";\n"
		#print("sanity check:",sql)
		cur.execute(sql)
		ff.write(sql)
		rows = cur.fetchall()
		rows = [list(i) for i in rows]
		rows = [x for sublist in rows for x in sublist]
		print("available columus:", rows)
		pk_list[i] = [x for x in pk_list[i] if x.lower() in rows]
		fk_self_list[i] = [x for x in fk_self_list[i] if x.lower() in rows]
	ff.close()
	return pk_list, fk_self_list

def gen_and_run_sql_for_pk_check(table_list, pk_list, outfile, cur):
	pk_err_list = [0]*len(table_list)
	with open(outfile,'a') as ff: 
		#ff.write("--checking PK integrity \n")
		for i in range(len(table_list)):
			table_name = table_list[i]
			pk_name_list = pk_list[i]

			#begin check null, need to check return number
			null_check_query = "select count(*) from " + table_name + "\n	where ( "
			for k in range(len(pk_name_list)):
				if k!=0: null_check_query += " or "
				null_check_query += pk_name_list[k]
				null_check_query += " is null "     
			null_check_query += "); \n"
			ff.write(null_check_query)	
			pk_err_list[i] += execute_sql(cur, null_check_query, "check_null")
			
			
			#begin check duplication, need to add up all return numbers of each row
			dup_check_query = "select count(*) from " + table_name + "\n	group by "
			for k in range(len(pk_name_list)):
				if k!=0: dup_check_query += ","
				dup_check_query += pk_name_list[k]
			dup_check_query += " having count(*) >1; \n"
			ff.write(dup_check_query)	
			pk_err_list[i] += execute_sql(cur, dup_check_query, "check_dup")
			print("For PK check, table ",i+1, " violation is ",pk_err_list[i]) 
	return pk_err_list




def gen_and_run_sql_for_fk_check(table_list, pk_list, fk_list, fk_self_list, outfile, cur):
	with open(outfile,'a') as ff: 
		#ff.write("--checking FK integrity \n")
		fk_err_list = [0]*(len(table_list))
		for i in range(len(table_list)):
			for j in range(len(fk_list[i])):
				##check if foreign key is the pk of another table
				fk = fk_list[i][j]  #fk is a dictonary with 1 element
				k, v = list(fk.keys()), list(fk.values()) #k is table name, v is attribute
				#print(k,v)
				tb_index = table_list.index(k[0])
				if(v[0] in pk_list[tb_index]):  #print("FK:",k[0],",",v[0]," exist!")
					##generate sql,check PK existance, need to check the return number
					fk_check_sql = "select count(*) from "+ table_list[i] + "\n" 
					fk_check_sql += "	left join "+k[0]+" on "+table_list[i]+"."+ fk_self_list[i][j]+"="+k[0]+"."+v[0] +"\n"
					fk_check_sql += "	where "+k[0]+"."+v[0]+" is null; \n"
					ff.write(fk_check_sql)   				
					fk_err_list[i] += execute_sql(cur, fk_check_sql, "check_null")
					
					##check FK->PK's duplication
					fk_pkdup_sql = "select count(*) from "+ table_list[i] + "\n"
					fk_pkdup_sql += "	where "+ fk_self_list[i][j] + " in ( \n"
					fk_pkdup_sql += "		select "+v[0]+" from "+k[0]+" group by "+v[0]+" having count(*)>1); \n"
					ff.write(fk_pkdup_sql)   				
					fk_err_list[i] += execute_sql(cur, fk_pkdup_sql, "check_null")
				else: 
					print("Error, FK:",k[0],',',v[0],'is not a valid PK in anothe table.')
					#FIXME need update, just ignore this foreign key
			print("For FK check, table ",i+1, " violation is ",fk_err_list[i]) 
	return fk_err_list

##excute sql query, return the cnt number added together
def execute_sql(cur, sql, check):
	cur.execute(sql)
	rows = cur.fetchall()
	total_num=0
	for r in rows:
		if check=="check_null":
			total_num += r[0]
		elif check=="check_dup":
			total_num += r[0]-1 if r[0]>=1 else 0
		else:
			print("Error, error check")
		#print("row value:",r[0])
	#print("total number:",total_num)
	return total_num		

def truncate(n, decimals=0):
    multiplier = 10 ** decimals
    return int(n * multiplier) / multiplier



def get_table_size(cur, tb_name):
	sql = "select count(*) from "+tb_name+";"
	size = execute_sql(cur, sql, "check_null")
	return size 



def computer_metric(tb_size, pk_err, fk_err, fk_self_list, threshold):
	pk_err_rate = [(x*100.0)/y for x, y in zip(pk_err, tb_size)]
	num_of_fk = [(1 if len(x)==0 else len(x))  for x in fk_self_list] #can't be 0 because it is the divisor
	fk_err_rate = [(x*100.0)/(y*z) for x, y, z in zip(fk_err, tb_size, num_of_fk)]
	ok_col = ["Y"]* len(pk_err_rate)
	#with open(outfile, "w") as ff:
	for i in range(len(tb_size)):
		if(pk_err_rate[i] >threshold*100 or fk_err_rate[i]>threshold*100): ok_col[i]="N"
		pk_err_rate[i] = truncate(pk_err_rate[i],1)
		fk_err_rate[i] = truncate(fk_err_rate[i],1)
		print("table ",i, "pk err rate:", pk_err_rate[i])
		print("table ",i, "fk err rate:", fk_err_rate[i])
		print("table ",i, "is OK:", ok_col[i])
	return pk_err_rate, fk_err_rate, ok_col


def write_database(table_list, pk_err, fk_err, ok, cur, outfile):
	ff = open(outfile,'a')
	sql = "drop table QM;\n"
	cur.execute(sql)
	ff.write(sql)
	sql = "create table QM(tableName varchar(50),entityError float(4), referentialError float(4), OK varchar(1) );\n"
	cur.execute(sql)
	ff.write(sql)
	for i in range(len(table_list)):
		sql = "insert into QM values (" + "\'"+table_list[i]+"\'" +","+str(pk_err[i])+","+str(fk_err[i])+","+ "\'" +ok[i] +"\'" +");\n"
		print("write database:",sql)
		cur.execute(sql)
		ff.write(sql)
	print("Done!!")
	ff.close()
	
	 	


def main():
	args = parser.parse_args()
	if args.cmd=="":
		print("Error:please specify the input schema file!") 
	else:
		cmd = args.cmd.replace(";","=").replace(" ","").split("=")
		if len(cmd)>=3:
			infile = cmd[-3]
			err_thr = float(cmd[-1])	
			print("input file is:",infile, ",err threshold is:", err_thr)
		else:
			infile = cmd[-1]
			err_thr = 0.01
			print("input file is:",infile, ",err threshold is:", err_thr)
	#Define our connection string
	#conn_string = "host='localhost' dbname='postgres' user='postgres' password='password'"
	conn_string = "host='/tmp/' dbname='team7' user='team7' password='team7'"
 
	# print the connection string we will use to connect
	print ("Connecting to database")
 
	# get a connection, if a connect cannot be made an exception will be raised here
	conn = psycopg2.connect(conn_string)
 
	# conn.cursor will return a cursor object, you can use this cursor to perform queries
	cursor = conn.cursor()
	print ("Connected!\n")
	
	out_sql_file = "refint.sql"
	table_list, pk_list, fk_list, fk_self_list = parse_input(infile)
	pk_list, fk_self_list = sanity_check(table_list, pk_list, fk_self_list, cursor, out_sql_file)
	pk_err_list = gen_and_run_sql_for_pk_check(table_list, pk_list, out_sql_file, cursor)
	fk_err_list = gen_and_run_sql_for_fk_check(table_list, pk_list, fk_list, fk_self_list, out_sql_file, cursor)
	
	tb_size_list = []
	for tb in table_list:
		tb_size = get_table_size(cursor,tb)
		print("size of ",tb, "is ", tb_size)
		tb_size_list.append(tb_size)

	pk_err_list, fk_err_list, ok_list = computer_metric(tb_size_list, pk_err_list, fk_err_list, fk_self_list, err_thr)
	write_database(table_list, pk_err_list, fk_err_list, ok_list, cursor, out_sql_file)
	conn.commit()
	cursor.close()
	conn.close()
	print("Program Finish!")
 
if __name__ == "__main__":
	main()
