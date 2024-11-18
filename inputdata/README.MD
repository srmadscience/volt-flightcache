# Where we got the data #

````
https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FGJ&QO_fu146_anzr=b0-gvzr
````

# How we created flights.txt #

````
awk -F, '{ print $6, ",", $10, ",", $11 , ",", $15, ",", $25, ",", $32 }'  < On_Time_Reporting_Carrier_On_Time_Performance_\(1987_present\)_2023_1.csv  > flights.txt
````
