cd "D:\Data\safegraph\sg"

local months: dir "./sd/v1/2020" dirs "*" 
local fs=""
foreach month of local months {
	local files: dir "./sd/v1/2020/`month'" dirs "*"
	foreach file of local files {
		shell "c:\program files\7-zip\7z" x ".\sd\v1\2020\\`month'\\`file'\\2020-`month'-`file'-social-distancing.csv.gz" -o".\sd" -y
		local nfs="2020-`month'-`file'-social-distancing.csv"
		local fs: list fs | nfs
	}
}

foreach f of local fs {
		local g=subinstr("`f'",".csv","",1)
		import delimited using "./sd/v1/`f'", clear
		save ./sd/`g', replace
		cap rm "./sd/v1/`f'"
}
