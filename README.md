In this notebook we gather the data from Star Wars movies using SWAPI and a characters data set provided by Dennis Bakhuis (scraped from Wookieepedia). 
After some cleaning in Pandas final dataframes are send to Azure Blob Storage with connection string provided bu the user. 
No csv's are stored locally. Python script contains the same code.

Note: In most numeric fields "unknown" values were replaced with "0" which cause inacurate/false results on charts or in calculations.
