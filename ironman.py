from avengers.pepperpotts import *

"""
=====Package Abbreviation====
FC	:	Freight Cumulative
FT	:	Freight Transacted
SV	:	Services Verified or Registered
ST	:	Services Transacted
SVT	:	Services Voucher Transaction
SWT	:	Services Wallet Transaction
"""

#Variable declaration
tdate 	= dt.date.today().strftime('%Y%m%d')
df		= pd.DataFrame()

#Package initialization message
print(Fore.WHITE+"We have lots of functions in Ironman package:\n 1. index_month & index_qtr for Month & Quarter Index\n 2. str_month & str_qtr for Month & Quarter Name\n 3. month_order_details & qtr_order_details for order level data\n 4. cohorts_dump for cohorst data\n"+Fore.RESET)

def info():
	"""This will give you a breif about this package."""
	print(Fore.WHITE+"We have lots of functions in this package:\n 1. index_month & index_qtr for Month & Quarter Index\n 2. str_month & str_qtr for Month & Quarter Name\n 3. month_order_details & qtr_order_details for order level data\n 4. cohorts_dump for cohorst data\n"+Fore.RESET)

def list_month():
	"""This will give you the list of months."""
	month = ['01','02','03','04','05','06','07','08','09','10','11','12']
	for y in range(2015,2021):
		for m in month:
			if y == 2015 and int(m) <11:
				pass
			else:
				yield(str(y)+m)

def str_month(month_index):
	"""This will return month by passing the month index."""
	return list(list_month())[month_index]

def index_month(month_string):
	"""This will give you the index of month for ex. 50 for 202001"""
	return list(list_month()).index(month_string)

def list_qtr():
	"""This will give you the list of months."""
	qtr = ['Q1','Q2','Q3','Q4']
	for y in range(2015,2025):
		for q in qtr:
			if y == 2015 and q !='Q4':
				pass
			else:
				yield(str(y)+"-"+q)

def str_qtr(qtr_index):
	"""This will return quarter by passing the quarter index."""
	return list(list_qtr())[qtr_index]

def index_qtr(qtr_string):
	"""This will give you the index of quarter for ex. 1 for 2015-12"""
	return list(list_qtr()).index(qtr_string)

def month_order_details(from_month='201501',to_month='201501'):
	"""This will return a dataFrame and give you order level dump (filter on month)."""
	fm = index_month(from_month)
	tm = index_month(to_month)

	statement = "SELECT * FROM zlog.order_details WHERE month BETWEEN "+str(fm)+" AND "+str(tm)
	dataframe = pd.read_sql(statement,con=con_zinka)
	return dataframe

def qtr_order_details(from_qtr='2015-Q1',to_qtr='2015-Q1'):
	"""This will return a dataFrame and give you order level dump (filter on quarter)."""
	fq = index_qtr(from_qtr)
	tq = index_qtr(to_qtr)

	statement = "SELECT * FROM zlog.order_details WHERE qtr BETWEEN "+str(fq)+" AND "+str(tq)
	dataframe = pd.read_sql(statement,con=con_zinka)
	return dataframe

def getPResults(query):
	"""Pass the quey in this function and it will return results in dataFrame from Presto DB"""
	df = pd.read_sql_query(query, con_presto)
	return df

def getZResults(query):
	"""Pass the quey in this function and it will return results in dataFrame from local DB"""
	df = pd.read_sql_query(query, con_zinka)
	return df

def upload2Sql(dataFrame, dbName, tableName, mode='fail'):
	"""Using this function you can upload the data to mysql server """

	sqlEngine	   = create_engine('mysql+pymysql://py.user:py@1234@172.18.8.91/'+dbName, pool_recycle=3600)
	dbConnection	= sqlEngine.connect()

	try:
		frame		   = dataFrame.to_sql(tableName, dbConnection, if_exists=mode);
	except ValueError as vx:
		print(vx)
	except Exception as ex:   
		print(ex)
	else:
		print("Table %s created successfully."%tableName);   
	finally:
		dbConnection.close()

def common_sp_FC_SV():
	"""This function will return string with common fleet owner or supply partner count b/w services verified and freight transacted"""
	
	df1 = getZResults(qry_sponplatformF)
	df2 = getPResults(qry_sponplatformS)
	df2['sp_number']= df2.sp_number.astype(str)
	df2['month_name']=202001
	df3 = df1.merge(df2, how='left', on='sp_number')
	df1.to_csv('SPonPlatform-Freight.csv', index=False)
	df2.to_csv('SPonPlatform-Service.csv', index=False)
	df3.to_csv('CommonSP on Platform.csv', index=False)
	
	return "Common SPs in Services (verified) & Freight (Transacted cumulative):  "+str(df1.count()[0] - df3.isna().sum()[1])

def common_sp_FC_ST():
	pass

def cohorts_dump(cohort_type,qtr_or_month):
	"""This function will return a dataFrame, you'll have to pass cohort_type (shipper,carrier,cluster, sme or unit) & qtr_or_month (m or q)."""

	if qtr_or_month.lower() == 'm':

		if cohort_type.lower() == 'shipper' or cohort_type.lower() == 'customer':
			try:
				pass
				#HELLO()
				print(Fore.BLUE+"Your monthly cohorts dump for "+cohort_type.lower()+"s is ready !!!"+Fore.RESET)
			except Exception as e:
				print(Fore.RED+"Error occured: "+str(e)+Fore.RESET)
			finally:
				pass

		elif cohort_type.lower() == 'carrier' or cohort_type.lower() == 'fleet_owner':
			print("Your monthly cohorts dump for "+cohort_type.lower()+"s is ready !!!"+Fore.RESET)

		elif cohort_type.lower() == 'cluster':
			print("Your monthly cohorts dump for "+cohort_type.lower()+"s is ready !!!"+Fore.RESET)

		elif cohort_type.lower() == 'sme':
			print("Your monthly cohorts dump for "+cohort_type.lower()+"s is ready !!!"+Fore.RESET)

		else:
			print(Fore.YELLOW+"Please enter correct cohorts type (ie. shipper, carrier, cluster, unit or sme) !!!"+Fore.RESET)

	elif qtr_or_month.lower() == 'q':

		if cohort_type.lower() == 'shipper' or cohort_type.lower() == 'customer':
			print("Your quaterly cohorts dump for "+cohort_type.lower()+"s is ready !!!"+Fore.RESET)

		elif cohort_type.lower() == 'carrier' or cohort_type.lower() == 'fleet_owner':
			print("Your quaterly cohorts dump for "+cohort_type.lower()+"s is ready !!!"+Fore.RESET)

		elif cohort_type.lower() == 'cluster':
			print("Your quaterly cohorts dump for "+cohort_type.lower()+"s is ready !!!"+Fore.RESET)

		elif cohort_type.lower() == 'sme':
			print("Your quaterly cohorts dump for "+cohort_type.lower()+"s is ready !!!"+Fore.RESET)

		else:
			print(Fore.YELLOW+"Please enter correct cohorts type (ie. shipper, carrier, cluster, unit or sme) !!!"+Fore.RESET)
	
	else:
		print(Fore.YELLOW+"Please enter correct cohorts type (ie. shipper, carrier, cluster, unit or sme) or/and correct type q or m for quartely and monthly cohorts respectively !!!"+Fore.RESET)


