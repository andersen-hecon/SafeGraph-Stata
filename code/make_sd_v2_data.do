clear all
cd "d:\data\safegraph\sg\sd\v2"
local fs: dir "." files "2020-??-??*.dta"
di `"`fs'"'

clear

local astr ""
local j=0
foreach f of local fs {
	local j=`j'+1
	
	di "File `j': `f'"
	tempfile t`j'
	
	use `f', clear
	drop bucketed_distance_traveled median_dwell_at_bucketed_distanc at_home_by_each_hour destination_cbgs 
	qui {
		gen date=date(substr(date_range_start,1,strpos(date_range_start,"T")-1),"YMD")
		format date %td

		drop date_range_start date_range_end

		gen running_errands_device_count=device_count-(completely_home_device_count+part_time_work_behavior_devices+full_time_work_behavior_devices)

		gen not_working_device_count=completely_home_device_count+running_errands_device_count

		compress running_errands_device_count not_working_device_count

		noisily di "Breaking up home time"
		gen bucketed_home_time_1h		=real(regexs(1)) if regexm(bucketed_home_dwell_time,`"<60":([0-9]*)"'')
		gen bucketed_home_time_1h_6h	=real(regexs(1)) if regexm(bucketed_home_dwell_time,`"61-360":([0-9]*)"'')
		gen bucketed_home_time_6h_12h	=real(regexs(1)) if regexm(bucketed_home_dwell_time,`"361-720":([0-9]*)"'')
		gen bucketed_home_time_12h_18h	=real(regexs(1)) if regexm(bucketed_home_dwell_time,`"721-1080":([0-9]*)"'')
		gen bucketed_home_time_18h		=real(regexs(1)) if regexm(bucketed_home_dwell_time,`">1080":([0-9]*)"'')

		drop bucketed_home_dwell_time 

		noisily di "Breaking up percent home time"
		gen bucketed_home_pct_0_25		=real(regexs(1)) if regexm(bucketed_percentage_time_home,`"0-25":([0-9]*)"'')
		gen bucketed_home_pct_25_50		=real(regexs(1)) if regexm(bucketed_percentage_time_home,`"26-50":([0-9]*)"'')
		gen bucketed_home_pct_50_75		=real(regexs(1)) if regexm(bucketed_percentage_time_home,`"51-75":([0-9]*)"'')
		gen bucketed_home_pct_75_100	=real(regexs(1)) if regexm(bucketed_percentage_time_home,`"76-100":([0-9]*)"'')
		gen bucketed_home_pct_gt_100	=real(regexs(1)) if regexm(bucketed_percentage_time_home,`">100":([0-9]*)"'')

		drop bucketed_percentage_time_home
		
		noisily di "Breaking up away from home time"
		gen bucketed_away_time_0_20m	=real(regexs(1)) if regexm(bucketed_away_from_home_time,`"<20":([0-9]*)"'')
		gen bucketed_away_time_21_45m	=real(regexs(1)) if regexm(bucketed_away_from_home_time,`"21-45":([0-9]*)"'')
		gen bucketed_away_time_46_60m	=real(regexs(1)) if regexm(bucketed_away_from_home_time,`"46-60":([0-9]*)"'')
		gen bucketed_away_time_1_2h		=real(regexs(1)) if regexm(bucketed_away_from_home_time,`"61-120":([0-9]*)"'')
		gen bucketed_away_time_2_3h		=real(regexs(1)) if regexm(bucketed_away_from_home_time,`"121-180":([0-9]*)"'')
		gen bucketed_away_time_3_4h		=real(regexs(1)) if regexm(bucketed_away_from_home_time,`"181-240":([0-9]*)"'')
		gen bucketed_away_time_4_5h		=real(regexs(1)) if regexm(bucketed_away_from_home_time,`"241-300":([0-9]*)"'')
		gen bucketed_away_time_5_6h		=real(regexs(1)) if regexm(bucketed_away_from_home_time,`"301-360":([0-9]*)"'')
		gen bucketed_away_time_6_7h		=real(regexs(1)) if regexm(bucketed_away_from_home_time,`"361-420":([0-9]*)"'')
		gen bucketed_away_time_7_8h		=real(regexs(1)) if regexm(bucketed_away_from_home_time,`"421-480":([0-9]*)"'')
		gen bucketed_away_time_8_9h		=real(regexs(1)) if regexm(bucketed_away_from_home_time,`"481-540":([0-9]*)"'')
		gen bucketed_away_time_9_10h	=real(regexs(1)) if regexm(bucketed_away_from_home_time,`"541-600":([0-9]*)"'')
		gen bucketed_away_time_10_11h	=real(regexs(1)) if regexm(bucketed_away_from_home_time,`"601-660":([0-9]*)"'')
		gen bucketed_away_time_11_12h	=real(regexs(1)) if regexm(bucketed_away_from_home_time,`"661-720":([0-9]*)"'')
		gen bucketed_away_time_12_14h	=real(regexs(1)) if regexm(bucketed_away_from_home_time,`"721-840":([0-9]*)"'')
		gen bucketed_away_time_14_16h	=real(regexs(1)) if regexm(bucketed_away_from_home_time,`"841-960":([0-9]*)"'')
		gen bucketed_away_time_16_18h	=real(regexs(1)) if regexm(bucketed_away_from_home_time,`"961-1080":([0-9]*)"'')
		gen bucketed_away_time_18_20h	=real(regexs(1)) if regexm(bucketed_away_from_home_time,`"1081-1200":([0-9]*)"'')
		gen bucketed_away_time_20_22h	=real(regexs(1)) if regexm(bucketed_away_from_home_time,`"1201-1320":([0-9]*)"'')
		gen bucketed_away_time_22_24h	=real(regexs(1)) if regexm(bucketed_away_from_home_time,`"1321-1440":([0-9]*)"'')
		
		drop bucketed_away_from_home_time
		
		foreach v of varlist bucketed_* {
			replace `v'=0 if mi(`v')
		}
		
		gen county_fips=real(substr(strofreal(origin_census_block_group,"%012.0f"),1,5))
		compress

		save `t`j''
	}
	
	local astr: list astr | t`j'
	
}

clear
append using `astr'
summ date
local s=`r(min)'
tempfile add
save `add'

*use "social_distancing_panel_v2", clear
*drop if date>=`s'
*append using `add'

fillin origin_census_block_group date

foreach v of varlist 	device_count completely_home_device_count part_time_work_behavior_devices ///
						full_time_work_behavior_devices delivery_behavior_devices candidate_device_count ///
						running_errands_device_count not_working_device_count ///
						bucketed_home_time* bucketed_away_time* {
		replace `v'=0 if mi(`v') & _fillin==1
}

replace county_fips=real(substr(strofreal(origin_census_block_group,"%012.0f"),1,5)) if _fillin==1 & mi(county_fips)

drop _fillin


compress

save "social_distancing_panel_v2", replace
