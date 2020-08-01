# delayedflights


## discussion about what the task is

The paper is not a real use case, and things like taking the weather data from actual arrival time is some kind of data leakage.
To have comparable results, we will do the same as the paper.


## Reading the weather data

### Small optim: filter the weather data by keeping only the one with airport present the flights data

### Mapping the wban to airport

Two almost equivalent solutions:
* read the stations files on the fly
* preprocess the stations files and save the mapping in a file

Alternatives:
* use data from https://www.ncdc.noaa.gov/data-access/land-based-station-data/station-metadata
* use data from http://www.weather2000.com/1st_order_wbans.txt
-> with less consistency & coverage than using the station files


## Labels

All the 4 definitions of the labels (D1 to D4) have been implemented.


## Negative subsampling


## Joining the dataset

### Alternatives

#### group weather data per hour x airport, then for each comination hour x airport join flights with weather data
- 12x2 joins -> slow?
+ should scale?

#### group weather data per airport, then for each airport recompute the join "in memory"
+ one group by only -> fast?
- everything related to one airport in memory -> don't scale?

#### group weather data per day x airport, join with flight data on day x airport, pick the weather data the one with the closest ts
+ one group by and one join only -> fast?
+ should scale?

### implementation

How to get the timestamp? Should we handle time zones?
How to aggregate weather data when several are available for one hour?


## Preprocessing

### Weather type

The string is composed of several 2 characters substrings, each representing one type of weather ('RA': rain, 'SN': snow), with an optionnal character prefix ('-': light intensity, blank: Moderate intensity, '+' Heavy intensity)
https://en.wikipedia.org/wiki/METAR#METAR_WX_codes
The data is data is splitted into several columns (one for each possible type of weather), with values equal to 0 if the it's absent, and the values of 1, 2, or 3 if present (respectively with a '-' prefix, no prefix, '+' prefix)

### Sky condition

The string is composed of several 6 characters substrings, each representing a type of cloud coverage at a given altitude.
The 3 first characters represents the cloud coverage percentage (in oktas) (FEW: "Few" 1–2 oktas, SCT: "Scattered" 3–4 oktas, etc).
The 3 last characters represents the altitude.
https://en.wikipedia.org/wiki/METAR#Cloud_reporting
The data is data is splitted into several columns (one for each possible coverage percentage), with values equal to the min altitude reported for this coverage, or the max altitude (999) if not reported.
