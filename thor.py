import pandas as pd
import datetime as dt
from colorama import init,Fore, Back, Style 

#Initialization for colorama
init() 

#Variable declaration
tdate = dt.date.today().strftime('%Y%m%d')

#Package initialization message
print(Fore.WHITE+"Thor package will help you to get metrics for services data, these are some functions available in this package:\n 1. fastag_journey for fastag journey over the period following these parameter (df, ls_onboard_month, ls_journey_month) & will return a DataFrame.\n 2. str_month & str_qtr for Month & Quarter Name\n 3. month_order_details & qtr_order_details for order level data\n 4. cohorts_dump for cohorst data\n"+Fore.RESET)

#data = pd.read_csv('fastag_journey.csv')

onboard = [201808,201809,201810,201811,201812,201901,201902,201903,201904,201905,201906,201907,201908,201909,201910,201911,201912]
journey = [201901,201902,201903,201904,201905,201906,201907,201908,201909,201910,201911,201912]

def fastagJourneyCalc(df, onboard_month,journey_month):
    """This function will give you a dictionary output on basis of onboard & joury month you have given."""
    sp_onboard = 0
    fastag_count = 0
    df = df.fillna(0)
    
    #fastag_calculation
    sp_onboard    = df[df['fastag_onboard']==onboard_month].groupby(['fastag_onboard']).size()
    fastag_count  = df[(df['fastag_onboard']==onboard_month) & (df['check_fastag_'+str(journey_month)]>0)].groupby(['fastag_onboard']).size()
    fastag_jmCont = df[(df['fastag_onboard']==onboard_month) & (df['fastag_'+str(journey_month)]>0)].groupby(['fastag_onboard']).size()
    fastag_gmvFM  = df[(df['fastag_onboard']==onboard_month) & (df['fastag_'+str(journey_month)]>0)].groupby(['fastag_onboard'])['fastag_'+str(onboard_month)].sum()
    fastag_gmvJM  = df[(df['fastag_onboard']==onboard_month) & (df['fastag_'+str(journey_month)]>0)].groupby(['fastag_onboard'])['fastag_'+str(journey_month)].sum()
    fastag_avg    = df[(df['fastag_onboard']==onboard_month) & (df['check_fastag_'+str(journey_month)]>0) & (df['fastag_'+str(onboard_month)]>0)].groupby(['fastag_onboard'])['fastag_'+str(onboard_month)].mean()
    fastag_avg_mx = df[(df['fastag_onboard']==onboard_month) & (df['check_fastag_'+str(journey_month)]>0) & (df['max_fastag_'+str(journey_month)]>0)].groupby(['fastag_onboard'])['max_fastag_'+str(journey_month)].mean()
    
    #fuel_calculation
    fuel_count     = df[(df['fastag_onboard']==onboard_month) & (df['max_fuel_'+str(journey_month)]>0)].groupby(['fastag_onboard'])['max_fuel_'+str(journey_month)].size()
    fuel_avg_mx    = df[(df['fastag_onboard']==onboard_month) & (df['max_fuel_'+str(journey_month)]>0)].groupby(['fastag_onboard'])['max_fuel_'+str(journey_month)].mean()
    fuel_sum_mx    = df[(df['fastag_onboard']==onboard_month) & (df['max_fuel_'+str(journey_month)]>0)].groupby(['fastag_onboard'])['max_fuel_'+str(journey_month)].sum()
    
    #gps_calculation
    try:
        gps_count  = df[(df['fastag_onboard']==onboard_month) & (df['max_gps_'+str(journey_month)]>0)].groupby(['fastag_onboard'])['max_gps_'+str(journey_month)].size()
        gps_count  = gps_count.values[0]
        gps_avg_mx = df[(df['fastag_onboard']==onboard_month) & (df['max_gps_'+str(journey_month)]>0)].groupby(['fastag_onboard'])['max_gps_'+str(journey_month)].mean()
        gps_avg_mx = gps_avg_mx.values[0]
        gps_sum_mx = df[(df['fastag_onboard']==onboard_month) & (df['max_gps_'+str(journey_month)]>0)].groupby(['fastag_onboard'])['max_gps_'+str(journey_month)].sum()
        gps_sum_mx = gps_sum_mx.values[0]
    except:
        gps_count  = 0
        gps_avg_mx = 0
        gps_sum_mx = 0
        
    
    #tyre_calculation
    try:
        tyre_count  = df[(df['fastag_onboard']==onboard_month) & (df['max_tyre_'+str(journey_month)]>0)].groupby(['fastag_onboard'])['max_tyre_'+str(journey_month)].size()
        tyre_count  = tyre_count.values[0]
        tyre_avg_mx = df[(df['fastag_onboard']==onboard_month) & (df['max_tyre_'+str(journey_month)]>0)].groupby(['fastag_onboard'])['max_tyre_'+str(journey_month)].mean()
        tyre_avg_mx = tyre_avg_mx.values[0]
        tyre_sum_mx = df[(df['fastag_onboard']==onboard_month) & (df['max_tyre_'+str(journey_month)]>0)].groupby(['fastag_onboard'])['max_tyre_'+str(journey_month)].sum()
        tyre_sum_mx = tyre_sum_mx.values[0]
    except:
        tyre_count  = 0
        tyre_avg_mx = 0
        tyre_sum_mx = 0
        
    return {
    "Fastag Onboard"						:onboard_month,
    "Journey Month"							:journey_month,
    "# of SPs"								:sp_onboard.values[0],
    "# of SPs (higher GMV)"					:fastag_count.values[0],
    "# of SPs (Jth Month)"					:fastag_jmCont.values[0],
    "First Month GMV (Jth Month User's)"	:fastag_gmvFM.values[0],
    "Journey Month GMV (Jth Month User's)"	:fastag_gmvJM.values[0],
    "Fastag Journey AvgMaxGMV"				:fastag_avg.values[0],
    "Fastag Journey SumMaxGMV"				:fastag_avg_mx.values[0],
    "Fuel Journey Count"					:fuel_count.values[0],
    "Fuel Journey AvgMaxGMV"				:fuel_avg_mx.values[0],
    "Fuel Journey SumMaxGMV"				:fuel_sum_mx.values[0],        
    "GPS Journey Count"						:gps_count,
    "GPS Journey AvgMaxGMV"					:gps_avg_mx,
    "GPS Journey SumMaxGMV"					:gps_sum_mx,
    "Tyre Journey Count"					:tyre_count,
    "Tyre Journey AvgMaxGMV"				:tyre_avg_mx,
    "Tyre Journey SumMaxGMV"				:tyre_sum_mx
    }   

def fastag_journey(df, ls_onboard, ls_journey):
	"""This will return a dataframe with all the metrics define in func:fastagJourneyCalc passing the imput parameter (df, ls_onboard, ls_journey) """
	results = []
	df = df.fillna(0)

	for om in ls_onboard:
	    for jm in ls_journey:
	        if om < jm:
	            #print("Journey created for ("+str(om)+", "+str(jm)+")")
	            ls = list(fastagJourneyCalc(df,om,jm).values())
	            results.append(ls)
	        else:
	            pass 

	columns   = list(fastagJourneyCalc(df,ls_onboard[0],ls_journey[0]).keys())
	journeydf = pd.DataFrame(results,columns = columns)
	print("Data created for "+str(ls_onboard[0])+" to "str(ls_journey[0]))
	return journeydf

def get_FJSummary(df):
	"""This will return summary of Fastag Journey"""
	col3			= df.loc[0:,'Fastag Onboard':'# of SPs (Jth Month)']
	higher_gmv_sp	= df['# of SPs (higher GMV)']/df['# of SPs']
	nth_month_sp	= df['# of SPs (Jth Month)']/df['# of SPs']
	col6			= df.loc[0:,['Fuel Journey Count','GPS Journey Count','Tyre Journey Count']]
	gmv_growth		= ((df['Journey Month GMV (Jth Month User\'s)']/df['# of SPs (Jth Month)']) - (df['First Month GMV (Jth Month User\'s)']/df['# of SPs (Jth Month)'])) / (df['First Month GMV (Jth Month User\'s)']/df['# of SPs (Jth Month)']) 
	fuel_sp			= df['Fuel Journey Count']/df['# of SPs']
	gps_sp			= df['GPS Journey Count']/df['# of SPs']
	tyre_sp			= df['Tyre Journey Count']/df['# of SPs']
	col7			= (df['First Month GMV (Jth Month User\'s)']/df['# of SPs (Jth Month)'])
	col8			= (df['Journey Month GMV (Jth Month User\'s)']/df['# of SPs (Jth Month)'])
	col9			= df.loc[0:,['Fuel Journey AvgMaxGMV','GPS Journey AvgMaxGMV','Tyre Journey AvgMaxGMV']]

	#concate all the metrics
	df				= pd.concat([col3,col6,higher_gmv_sp,nth_month_sp,gmv_growth,fuel_sp,gps_sp,tyre_sp,col7,col8,col9], axis=1)
	df.columns		= ['Fastag Onboard','Journey Month','# of SPs',	'# of SPs (higher GMV)','# of SPs (Jth Month)' ,'# of SP (Fuel)','# of SP (GPS)','# of SP (Tyre)',
    					'SP% (Higher GMV)', 'SP% (Jth Month)','GMV growth%', 'SP% (Fuel)', 'SP% (GPS)', 'SP% (Tyre)', 
    					'AvgGMV First Month (Fastag)', 'AvgGMV Journey Month (Fastag)', 'AvgGMV (Fuel)', 'AvgGMV (GPS)', 'AvgGMV (Tyre)'
    				  ]
	return df
