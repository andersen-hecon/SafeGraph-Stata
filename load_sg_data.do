cd "d:\data\safegraph\sg"

local astr ""
forvalues i=1/5 {
    import delimited using "./CoreApr2020Release-CORE_POI-2020_03-2020-04-07/core_poi-part`i'.csv", clear

	drop iso_country_code phone_number open_hours category_tags
	
	statastates, abbreviation(region)
	keep if _merge==3
	drop _merge region state_name
	
	tempfile f`i'
	save `f`i''
	
	local astr: list astr | f`i'
}


clear

append using `astr'

merge 1:1 safegraph_place_id using "safegraph_place_id", keepusing(long_id)

gsort mi_l long_id safegraph_place_id

gen byte mi_l=mi(long_id)

summ long_id
bys mi_l (safegraph_place_id) : replace long_id=`r(max)'+_n if mi_l==1

drop mi_l

encode top_category, gen(category) label(category)

// now deal with unmatched (i.e. removed) POIs
preserve
	keep if _merge==2
	keep long_id
	merge 1:1 long_id using "safegraph_place_id", keep(matched) nogen
	tempfile add
	save `add'
restore

drop if _merge==2
append using `add'

compress

compress

save "safegraph_place_id", replace

foreach s in jan19 feb19 mar19 apr19 may19 jun19 jul19 aug19 sept19 oct19 nov19 dec19 jan20 feb20 mar20 {
	tempfile f1 f2 f3

	forvalues i=1/3 {
	  	import delimited using "./monthly/`s'/patterns-part`i'.csv", clear
		
		drop location_name street_address city region postal_code brands distance_from_home median_dwell bucketed_dwell_times related_same_day_brand related_same_month_brand popularity_by_hour popularity_by_day device_type iso_country_code visitor_work_cbgs  raw_visit_counts date_range_end 
		
		merge 1:1 safegraph_place_id using safegraph_place_id, keep(matched) nogen keepusing(safegraph_place_id long_id category)
		
		drop safegraph_place_id 

		replace date_range_start =date_range_start /(24*60*60)+date("1/1/1970","MDY")
		compress
		format date* %td

		replace visits_by_day=subinstr(subinstr(visits_by_day,"[","",1),"]","",1)
		split visits_by_day, parse(",") gen(visits) destring
		drop visits_by_day

		compress
		
		save `f`i'', replace
	}
	
	clear
	
	append using `f1' `f2' `f3'
	save "./`s'_patterns", replace
}