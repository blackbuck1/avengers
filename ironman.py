import mysql.connector
import pandas as pd
import datetime as dt
from pyhive import presto
from colorama import init,Fore, Back, Style 


#Initialization for colorama
init() 

#Variable declaration
tdate 	= dt.date.today().strftime('%Y%m%d')
df		= pd.DataFrame()

#Package initialization message
print(Fore.WHITE+"We have lots of functions in Ironman package:\n 1. index_month & index_qtr for Month & Quarter Index\n 2. str_month & str_qtr for Month & Quarter Name\n 3. month_order_details & qtr_order_details for order level data\n 4. cohorts_dump for cohorst data\n"+Fore.RESET)

def info():
	print(Fore.WHITE+"We have lots of functions in this package:\n 1. index_month & index_qtr for Month & Quarter Index\n 2. str_month & str_qtr for Month & Quarter Name\n 3. month_order_details & qtr_order_details for order level data\n 4. cohorts_dump for cohorst data\n"+Fore.RESET)

#Sqlconnector for DbConnection
con_zinka  = mysql.connector.connect(user='public', password='zinka@321', host='192.168.1.91', database='zlog')
con_presto = presto.connect(host='192.168.1.1', port=8080)

#Queries for data maipulation
qry_trucksonplatformS	= "SELECT DISTINCT truck_no FROM divum.blackbuck.fleetApp_truck WHERE is_verified = 1"
qry_trucksonplatformF	= "SELECT DISTINCT registration_number FROM od_snapshot WHERE registration_number <> ''"

qry_truckstransactingS	= """
SELECT DATE_FORMAT(updated_at+ INTERVAL '330' MINUTE, '%Y%m') AS month_name,
trucks AS registration_number
FROM
(
SELECT updated_at,
truck_no
FROM services_payment.blackbuck.wallet_wallettransactionhistory
WHERE status IN ('FasTag Recharge',
'FastageRecharge Failure',
'HPCL-Card-Recharge',
'HPCLCardless',
'Reliance-Card-Recharge',
'RelianceCardless',
'Card-Recharge',
'Card-Pull',
'Card-less Pull')
AND truck_no IS NOT NULL
UNION ALL SELECT a.created_at AS updated_at,
b.truck_no AS truck_no
FROM divum.blackbuck.fleetApp_gpsrequest a
INNER JOIN divum.blackbuck.fleetApp_truck b ON a.truck_id=b.id
WHERE status_id = 4
UNION ALL SELECT a.created_on AS updated_at,
c.truck_no AS truck_no
FROM divum.blackbuck.voucher_transaction a
LEFT JOIN divum.blackbuck.voucher b ON a.voucher_id = b.id
LEFT JOIN divum.blackbuck.fleetApp_truck c ON b.truck_id = c.id
WHERE a.type = 'DEBIT'
AND b.mode IN ('CASH', 'ACCOUNT') 
) a
GROUP BY 1,2
"""

qry_truckstransactingS	= qry_truckstransactingS.replace("\n", " ")
qry_truckstransactingF	= "SELECT month_name, registration_number FROM od_snapshot WHERE registration_number <> ''"

def list_month():
    month = ['01','02','03','04','05','06','07','08','09','10','11','12']
    for y in range(2015,2021):
        for m in month:
            if y == 2015 and int(m) <11:
                pass
            else:
                yield(str(y)+m)

def str_month(month_index):
	return list(list_month())[month_index]

def index_month(month_string):
	return list(list_month()).index(month_string)

def list_qtr():
    yield('')
    qtr = ['Q1','Q2','Q3','Q4']
    for y in range(2015,2025):
        for q in qtr:
            if y == 2015 and q !='Q4':
                pass
            else:
                yield(str(y)+"-"+q)

def str_qtr(qtr_index):
	return list(list_qtr())[qtr_index]

def index_qtr(qtr_string):
	return list(list_qtr()).index(qtr_string)

def month_order_details(from_month='201501',to_month='201501'):
	fm = index_month(from_month)
	tm = index_month(to_month)

	statement = "SELECT * FROM zlog.order_details WHERE month BETWEEN "+str(fm)+" AND "+str(tm)
	dataframe = pd.read_sql(statement,con=con_zinka)
	return dataframe

def qtr_order_details(from_qtr='2015-Q1',to_qtr='2015-Q1'):
	fq = index_qtr(from_qtr)
	tq = index_qtr(to_qtr)

	statement = "SELECT * FROM zlog.order_details WHERE qtr BETWEEN "+str(fq)+" AND "+str(tq)
	dataframe = pd.read_sql(statement,con=con_zinka)
	return dataframe

def cohorts_dump(cohort_type,qtr_or_month):
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
			print(Fore.YELLOW+"Please enter correct cohorts type (ie. shipper, carrier, cluster or sme) !!!"+Fore.RESET)

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
			print(Fore.YELLOW+"Please enter correct cohorts type (ie. shipper, carrier, cluster or sme) !!!"+Fore.RESET)
	
	else:
		print(Fore.YELLOW+"Please enter correct cohorts type (ie. shipper, carrier, cluster or sme) or/and correct type q or m for quartely and monthly cohorts respectively !!!"+Fore.RESET)


