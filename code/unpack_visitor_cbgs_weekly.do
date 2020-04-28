cd "d:\data\safegraph\sg"
python:

from sfi import Data, Frame
import pandas as pd
import json

def unpack_visitors():
	patterns_df =pd.DataFrame(data=Data.get("long_id visitor_home_cbgs",selectvar="select"),columns=["long_id","visitor_home_cbgs"])

	# convert jsons to dicts
	patterns_df = patterns_df.dropna(subset = ['visitor_home_cbgs'])
	patterns_df['visitor_home_cbgs_dict'] = [json.loads(cbg_json) for cbg_json in patterns_df.visitor_home_cbgs]

	# extract each key:value inside each visitor_home_cbg dict (2 nested loops) 
	all_sgpid_cbg_data = [] # each cbg data point will be one element in this list
	for index, row in patterns_df.iterrows():
	  this_sgpid_cbg_data = [ {'long_id' : row['long_id'], 'visitor_home_cbgs' : key, 'visitor_count' : value} for key,value in row['visitor_home_cbgs_dict'].items() ]
	  
	  # concat the lists
	  all_sgpid_cbg_data = all_sgpid_cbg_data + this_sgpid_cbg_data

	home_cbg_data_df = pd.DataFrame(all_sgpid_cbg_data)

	# note: home_cbg_data_df has 3 columns: safegraph_place_id, visitor_count, visitor_home_cbg

	# sort the result:
	home_cbg_data_df = home_cbg_data_df.sort_values(by=['long_id', 'visitor_count'], ascending = False)

	fr_results=Frame.connect("unpacked")

	n0=fr_results.getObsTotal()
	nn=home_cbg_data_df.shape[0]
	n1=n0+nn

	add_locs=range(n0,n1)
	fr_results.addObs(nn)

	fr_results.store(("long_id", "visitor_count", "visitor_home_cbgs"),add_locs,home_cbg_data_df.values)
end

clear
foreach load in "03-01" "03-08" "03-15" "03-22" "03-29" "04-05" "04-12" {
	import delimited using "./weekly/2020-`load'-weekly-patterns.csv", clear

	merge 1:1 safegraph_place_id using  "safegraph_place_id", keep(matched) nogen keepusing(long_id)

	cap frame drop unpacked
	frame create unpacked double long_id long visitor_count str12 visitor_home_cbgs

	gen byte select=0
	count
	local k=50000
	local N=`r(N)'
	local N0=`k'*floor(`N'/`k')
	forvalues n=1(`k')`N0' {
		local n1=`n'
		local n2=min(`N',`n'+`k'-1)

		qui replace select=1 in `n1'/`n2'
		
		di "Doing `n1' to `n2'"
		
		python: unpack_visitors()
		qui replace select=0
	}

	frame change unpacked
	compress
	save "weekly_`: di subinstr("`load'","-","_",.)'_cbgs", replace
}		

