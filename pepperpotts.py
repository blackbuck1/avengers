import mysql.connector
import pandas as pd
from pyhive import presto
from colorama import init, Fore, Back, Style 

#Initialization for colorama
init() 

print(Fore.YELLOW+"===========Package: PepperPotts is imported================="+Fore.RESET)

#Sqlconnector for DbConnection
con_zinka  = mysql.connector.connect(user='public',  password='zinka@321', host='172.18.8.91', database='zlog')
con_zinka  = mysql.connector.connect(user='py.user', password='py@1234',   host='172.18.8.91', database='services')
con_presto = presto.connect(host='54.254.250.251', port=8080)

#Queries for data maipulation
qry_trucksonplatformS	= "SELECT DISTINCT truck_no FROM divum.blackbuck.fleetApp_truck WHERE is_verified = 1"
qry_trucksonplatformF	= "SELECT DISTINCT registration_number FROM od_snapshot WHERE registration_number <> ''"
qry_truckstransactedS	= """
							SELECT DATE_FORMAT(updated_at+ INTERVAL '330' MINUTE, '%Y%m') AS month_name,
							truck_no AS registration_number
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
							AND DATE_FORMAT(updated_at+ INTERVAL '330' MINUTE, '%Y%m') = '202002'
							UNION ALL
							SELECT a.created_at AS updated_at, b.truck_no AS truck_no FROM divum.blackbuck.fleetApp_gpsrequest a INNER JOIN divum.blackbuck.fleetApp_truck b ON a.truck_id=b.id 
							WHERE status_id = 4 AND DATE_FORMAT(a.created_at+ INTERVAL '330' MINUTE, '%Y%m') = '202002'
							UNION ALL 
							SELECT a.created_on AS updated_at, c.truck_no AS truck_no FROM divum.blackbuck.voucher_transaction a LEFT JOIN divum.blackbuck.voucher b ON a.voucher_id = b.id LEFT JOIN divum.blackbuck.fleetApp_truck c ON b.truck_id = c.id
							WHERE a.type = 'DEBIT'
							AND DATE_FORMAT(a.created_on+ INTERVAL '330' MINUTE, '%Y%m') = '202002'
							AND b.mode IN ('CASH', 'ACCOUNT') 
							) a
							GROUP BY 1,2
						  """
qry_truckstransactedF	= "SELECT month_name, registration_number FROM od_snapshot WHERE registration_number <> ''"
qry_sponplatformS		= "SELECT DISTINCT phone_no AS sp_number FROM divum.blackbuck.fleetApp_fleetowner WHERE is_verified = 1 AND CAST(DATE_FORMAT(app_installed_on+ INTERVAL '330' MINUTE, '%Y%m') AS INT) <= 202001"
qry_sponplatformF		= "SELECT DISTINCT actual_sp_number AS sp_number FROM od_snapshot"
qry_allsponplatformS	= "SELECT user_id, phone_no FROM divum.blackbuck.fleetApp_fleetowner"
qry_sptransactedSWT		= """
							SELECT user_id,
							amount,
							date_format(updated_at + interval '5' HOUR + interval '30' MINUTE, '%Y%m') AS month_name,
							status
							FROM services_payment.blackbuck.wallet_wallettransactionhistory
							WHERE status IN 
							('HPCL-Card-Recharge',
							'HPCLCardless',
							'Reliance-Card-Recharge',
							'RelianceCardless',
							'Card-Recharge',
							'Card-Pull',
							'Card-less Pull',
							'FasTag Recharge',
							'FastageRecharge Failure'
							)
							AND date_format(updated_at + interval '5' HOUR + interval '30' MINUTE, '%Y%m') = '202001'
						  """.replace("\n", " ").replace("\t", "")
qry_sptransactedSVT		= """
							SELECT c.user_id AS user_id,
							a.amount AS amount,
							date_format(a.created_on + interval '5' HOUR + interval '30' MINUTE, '%Y%m') AS month_name,
							'Cash' AS status
							FROM divum.blackbuck.voucher_transaction a
							LEFT JOIN divum.blackbuck.voucher b
							ON a.voucher_id = b.id
							LEFT JOIN divum.blackbuck.fleetApp_fleetowner c
							ON b.fleet_owner_id = c.id
							WHERE a.type = 'DEBIT' 
							AND b.mode IN ('CASH', 'ACCOUNT') 
							AND date_format(a.created_on + interval '5' HOUR + interval '30' MINUTE, '%Y%m') = '202001'
						  """.replace("\n", " ").replace("\t", "")

'''Above mentioned (qry_sptransactedSWT,qry_sptransactedSVT & qry_allsponplatformS) will be used in case if qry_sptransactedS does not execute !!!'''
qry_sptransactedS		= """
							SELECT phone_no AS sp_number,
							dt AS month_name,
							SUM(fuel_recharge) AS fuel_recharge,
							SUM(fastag_recharge) AS fastag_recharge
							FROM
							(
								SELECT * FROM
								(
									SELECT user_id,
									date_format(updated_at + interval '5' HOUR + interval '30' MINUTE, '%Y%m') AS dt,
									coalesce(sum(CASE WHEN status IN ('HPCL-Card-Recharge', 'HPCLCardless', 'Reliance-Card-Recharge', 'RelianceCardless', 'Card-Recharge', 'Cash') THEN amount END), 0) - coalesce(sum(CASE WHEN status IN ('Card-Pull', 'Card-less Pull') THEN amount END), 0) AS fuel_recharge,
									coalesce(sum(CASE WHEN status IN ('FasTag Recharge') THEN amount END), 0) - coalesce(sum(CASE WHEN status IN ('FastageRecharge Failure') THEN amount END), 0) AS fastag_recharge
									FROM
									(
										SELECT user_id,
										amount,
										updated_at,
										status
										FROM services_payment.blackbuck.wallet_wallettransactionhistory
										WHERE status IN 
										(
											'HPCL-Card-Recharge',
											'HPCLCardless',
											'Reliance-Card-Recharge',
											'RelianceCardless',
											'Card-Recharge',
											'Card-Pull',
											'Card-less Pull',
											'FasTag Recharge',
											'FastageRecharge Failure'
										)
										AND date_format(updated_at + interval '5' HOUR + interval '30' MINUTE, '%Y%m') = '202001'
										UNION ALL
										SELECT c.user_id AS user_id,
										a.amount AS amount,
										a.created_on AS updated_at,
										'Cash' AS status
										FROM divum.blackbuck.voucher_transaction a
										LEFT JOIN divum.blackbuck.voucher b
										ON a.voucher_id = b.id
										LEFT JOIN divum.blackbuck.fleetApp_fleetowner c
										ON b.fleet_owner_id = c.id
										WHERE a.type = 'DEBIT' AND b.mode IN ('CASH', 'ACCOUNT') AND date_format(a.created_on + interval '5' HOUR + interval '30' MINUTE, '%Y%m') = '202001'
									) a
									GROUP BY 1, 2
								) a
								INNER JOIN
								(
									SELECT user_id, phone_no FROM divum.blackbuck.fleetApp_fleetowner
								) b 
								ON a.user_id = b.user_id 
							) f
							GROUP BY 1,2
						 """.replace("\n", " ").replace("\t", "")
qry_spinternalfuelS		= "SELECT sp_number, month AS month_name, SUM(gmv) AS fuel_recharge  FROM services.internal_fuel  WHERE month = 202001  GROUP BY 1,2"
qry_allsponplatformF	= "SELECT username AS sp_number, Concat(first_name, ' ', last_name) AS sp_name FROM bb.zinka.auth_user GROUP BY 1,2"

qry_truckstransactedS	= """
							SELECT DISTINCT truck_no AS registration_number
							FROM 
							( 
								SELECT updated_at,
								truck_no
								FROM services_payment.blackbuck.wallet_wallettransactionhistory
								WHERE status IN (
									'FasTag Recharge',
									'FastageRecharge Failure',
									'HPCL-Card-Recharge', 
									'HPCLCardless',
									'Reliance-Card-Recharge', 
									'RelianceCardless',
									'Card-Recharge', 
									'Card-Pull', 
									'Card-less Pull'
								) 
								AND truck_no IS NOT NULL
								AND date_format(updated_at + interval '5' HOUR + interval '30' MINUTE, '%Y%m') = '202001'
							UNION ALL 
								SELECT a.created_at AS updated_at ,
								b.truck_no AS truck_no
								FROM divum.blackbuck.fleetapp_gpsrequest a
								INNER JOIN divum.blackbuck.fleetapp_truck b
								ON a.truck_id=b.id 
								WHERE status_id = 4
								AND date_format(a.created_at + interval '5' HOUR + interval '30' MINUTE, '%Y%m') = '202001'
							UNION ALL 
								SELECT a.created_on AS updated_at,
								c.truck_no AS truck_no
								FROM divum.blackbuck.voucher_transaction a
								LEFT JOIN divum.blackbuck.voucher b
								ON a.voucher_id = b.id 
								LEFT JOIN divum.blackbuck.fleetapp_truck c
								ON b.truck_id = c.id 
								WHERE a.type = 'DEBIT' 
								AND b.mode IN ('CASH', 'ACCOUNT')
								AND date_format(a.created_on + interval '5' HOUR + interval '30' MINUTE, '%Y%m') = '202001'
							) a
						  """.replace("\n", " ").replace("\t", "")
