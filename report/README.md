# Predicting flight delays
Victor Guerra <vm.guerramoran@criteo.com>
Stephane Le Roy <s.leroy@criteo.com>

## Introduction
In this report, we present the work done to implement a Machine Learning pipeline that trains a model that predicts delays in US Flights.
We take as reference a paper published at beginning of 2016 [Using Scalable Data Mining for Predicting Flight Delays](https://www.researchgate.net/publication/292539590_Using_Scalable_Data_Mining_for_Predicting_Flight_Delays) by Belcastro, Marozzo, Talia and Trunfio.

In order to have results comparable with the paper, we used the same dataset, both in term of date range, from 2009 to 2013, and in term of feature used.
The results described below were obtained using the following setup (unless otherwise specified):
- D2 as a label (delayed flights affected by extreme weather, plus the ones for which NAS delay is greater than or equal to the delay threshold)
- 60 min for the delay threshold
- 12 hours of weather data

In this report positives will define as delayed flights and negatives as on-time flights.

## Implementation
### Reading datasets and preprocessing

#### Weather data
Reading the data on disk was not a significant part of the running time of the job, so the data was kept as a csv (not a more efficient format like parquet), without prefiltering only the relevant columns.

One small optimization was to filter the weather data by keeping only the one with airport present the flights data. Since only a small part of the weather stations are close to an airport, so the other are irrelevant for the task.

The mapping of wban to airport was obtained using station files.
This was the approach giving the best coverage and consistency compared to other considered approaches:
- using data from https://www.ncdc.noaa.gov/data-access/land-based-station-data/station-metadata
- using data from http://www.weather2000.com/1st_order_wbans.txt

#### Flight data
Again reading the data on disk was not a significant part of the running time of the job, so the data was kept as a csv (not a more efficient format like parquet), without prefiltering only the relevant columns.

TODO explain computeNextDayFactor

TODO explain monotonically_increasing_id

### Negative subsampling
The ratio for the negative subsampling was computed on the fly to ensure the balance of positives and negatives.

This step was done as soon as possible since it greatly reduces the size of the dataset, especially for a 60 min threshold.

### Joining flights and weather data

Several alternatives have been tested for the join of the flights and weather data:

#### Alternatives
##### Group everything by airport
By grouping everything per airport (origin airport first, then doing the same for the destination airport) and doing the joins in memory, it was much faster in theory.
It requires only two steps of communication (one group by origin then destination airports), and the join in memory was pretty fast
(sorting the flights and weather data list by timestamp in O(log(N)), then iterating through the two lists using a "two pointers" approach in O(N)).
The issue with this method is that for airports with big traffic, and with multiple years of data, memory limit can be reached in theory.
Moreover the traffic per airport is not well balanced, so the computation time for the tasks would have been unbalanced too.

##### Multiple joins
This approach (as well as the ones described below) requires to complete/deduplicate the weather data in order to have exactly one weather data per hour/airport.
After, one joins is done for each of the hours of weather data.
This scales well in practice, but the it's quite expansive to do all these joins (24 joins for the setup with 12 hours of weather data).

##### Joining per airport
The next step was then to join all the flights and weather data per airport, and filtering out the irrelevant weather data using the timestamps in the join conditions.
This is quite straightforward to do that in with spark dataframe, but the issue is that the implementation is not efficient, and it's very likely that for each flight, all the weather data of the time range was join, and was filtered afterward, which is quite inefficient.

##### Joining per airport x day
By joining the flight with the weather data by airport x day, the join is very efficient, and the irrelevant weather data could be filtered afterward.
Since many flights have to be join with weather from the previous day, the weather data was duplicated before the join (on row for the current day and one row for the day before).
Days were used, but any duration could have been used, as long as the duration were bigger than the number of hours of weather data to join per flight + the duration of the longest flight.
This approach is fast, balanced, and scales well, so this was the one used in the end.

#### Implementation details
The approach for the join operation requires first to complete/deduplicate the weather data in order to have exactly one weather data per hour/airport.
This is done by grouping all weather data per airport, then, in memory, sorting them in O(log(N)), and iterating over all required hours to output the last available weather data (in O(N)).

Time zones were not handled explicitly in the code since no comparison of timestamp from two different time zone were done.

### Transformation pipeline

### Model training

### Metrics and Validation
The model are evaluated using the following metrics:
- F-score
- Precision
- Recall
- Accuracy
- Confusion matrix
- Area under the ROC

All metrics except Area under the ROC require a threshold to be chosen.
It was selected using the value maximizing the F-score.
Since the number of negatives and positives were balanced, it was equivalent to selecting the value maximizing the accuracy.

Since the area under the ROC is usually less noisy than the other metrics (and has the nice property of being invariant to the positives/negatives ratio, although irrelevant for our case), it was the metric used for the model selection.

TODO explain the train / validation / test split

## Feature engineering

### Flight data features

#### Origin and destination airports

The origin and destination airports of the flights are categorical features, fed to the model using a string indexer.
Since the cardinality is quite high, some airports might have relatively few flights, the feature is too sparse, and the model might not have enough data to predict delays on these airport.
To mitigate this issue the approach of using counters to encode the airports has been used.
The counter used was the number of flights per airports.
By using this counter as a feature column the model can learn and generalize well from low traffic airports.
Other counters than number of flights could have been used, including the labels, (this approach, -label encoding-, can sometime be very effective, but requires extra care to avoid leaking labels).

#### Timestamp data

The time of the flight can a very important feature for a delay prediction.
Since the model that is used in the end is Gradient Boosted Decision Trees, the timestamp value in seconds can be used directly by the model without much preprocessing, no bucketization or scaling/normalization required (directly handled by the model).
One important piece of information that is not extracted easily by the GBDT is the periodicity (yearly, weekly, daily periodicities). For this purpose other features has been built, DayOfYear, SecondOfDay and DayOfWeek.

### Weather data features

#### Weather type

The string is composed of several 2 characters substrings, each representing one type of weather ('RA': rain, 'SN': snow), with an optionnal character prefix ('-': light intensity, blank: Moderate intensity, '+' Heavy intensity)
https://en.wikipedia.org/wiki/METAR#METAR_WX_codes
The data is data is split into several columns (one for each possible type of weather), with values equal to 0 if the it's absent, and the values of 1, 2, or 3 if present (respectively with a '-' prefix, no prefix, '+' prefix)

#### Sky condition

The string is composed of several 6 characters substrings, each representing a type of cloud coverage at a given altitude.
The 3 first characters represents the cloud coverage percentage (in oktas) (FEW: "Few" 1–2 oktas, SCT: "Scattered" 3–4 oktas, etc).
The 3 last characters represents the altitude.
https://en.wikipedia.org/wiki/METAR#Cloud_reporting
The data is split into several columns (one for each possible coverage percentage), with values equal to the min altitude reported for this coverage, or the max altitude (999) if not reported.

#### Visibility, Wind Speed, Wind Direction, Humidity and Pressure

Since GBDT models are quite good at handling real values like these features, those were used directly without scaling/normalization or other preprocessing.
The wind direction seemed to be an important feature for the model (according to the results of feature importance), which is not surprising: a plane has to face the wind to land, so this is a strong factor for flight delays.
The values of wind direction are cyclic (a value of 359° is close to 0°) an attempt to encode it into the features has been done (by decomposing it into a 2D vector), but without significant improvements.

## Results

### Job performance

TODO running time

TODO explain about spark parameters / partitioning / etc

TODO garmadon screenshot

### Comparison with previous works

We can compare the results we obtained with the from the paper which this project is based on **[Belcastro 2016]** and previous works (**[Rebollo and Balakrishnan 2014]** and **[FlightCaster 2009]**).

|                | our work (training) | our work   | **[Belcastro 2016]** | **[Rebollo and Balakrishnan 2014]** | **[FlightCaster 2009]** |
| -------------- | ------------------- | ---------- | -------------------- |------------------------------------ | ----------------------- |
| Area under ROC | 0.997               | 0.940      |                      |                                     |                         |
| F-score        | 0.975               | 0.878      |                      |                                     |                         |
| precision      | 0.976               | 0.871      |                      |                                     | 0.85                    |
| recall         | 0.974               | 0.886      | 0.869                | 0.764                               | 0.60                    |
| Accuracy       | 0.975               | 0.877      | 0.858                | 0.810                               |                         |

We slightly improved the results with our implementation (by around 2% on the reported metrics).
All the reported statistics on the datasets (total number of flight, number of delayed flights, etc) were the same than the ones reported in the paper, so the improvement might be attributed to feature engineering or the model itself.

### Feature importance analysis

## Experiments

### Nb weather hours

TODO do the experiment + put the results

### Other target labels and delay thresholds

Here are the results for the different way to define the target labels (D1 to D4) and different values of delay threshold.

The definition for the target labels are the ones defined in the paper:
- D1 contains delayed flights due only to extreme weather or NAS, or a combination of them.
- D2 includes delayed flights affected by extreme weather, plus those ones for which NAS delay is greater than or equal to the delay threshold.
- D3 includes delayed flights affected by extreme weather or NAS, even if not exclusively.
- D4 contains all delayed flights.

For different threshold delays and target labels we obtained the same numbers of positives than the ones from the papers:

| label | delay | delayed tuples | delayed tuples **[Belcastro 2016]** |
| ----- | ----- | -------------- | ----------------------------------- |
| D1    | 15    | 1.32M          | 1.3M                                |
| D2    | 15    | 2.14M          | 2.1M                                |
| D3    | 15    | 3.41M          | 3.4M                                |
| D4    | 15    | 5.79M          | 5.8M                                |
| D1    | 60    | 257k           | 257k                                |
| D2    | 60    | 435k           | 433k                                |
| D3    | 60    | 953k           | 950k                                |
| D4    | 60    | 1.67M          | 1.7M                                |

The results are also very close to the ones obtain in the paper:

| label | delay | precision | recall | accuracy |
| ----- | ----- | --------- | ------ | -------- |
| D1    | 15    | 0.695     | 0.815  | 0.729    |
| D2    | 15    | 0.720     | 0.812  | 0.752    |
| D3    | 15    | 0.651     | 0.838  | 0.696    |
| D4    | 15    | 0.571     | 0.829  | 0.620    |
| D1    | 60    | 0.854     | 0.882  | 0.865    |
| D2    | 60    | 0.871     | 0.886  | 0.877    |
| D3    | 60    | 0.745     | 0.815  | 0.770    |
| D4    | 60    | 0.655     | 0.804  | 0.693    |

| label | delay | precision | recall | accuracy |
| ----- | ----- | --------- | ------ | -------- |
| D2    | 15    | 0.     | 0.  | 0.    |
| D2    | 30    | 0.     | 0.  | 0.    |
| D2    | 45    | 0.     | 0.  | 0.    |
| D2    | 60    | 0.871     | 0.886  | 0.877    |
| D2    | 90    | 0.     | 0.  | 0.    |

TODO do the experiment + put the results

### model accuracy with more/less data (one month to 5 years)

TODO do the experiment + put the results

## Other ideas of improvements

### Weather data aggregation, maximum, average, difference, etc

The weather data for the origin and destination airports are aggregated as lists of up to 12 structures containing the columns described above.
Other aggregation schemes could be considered.
For example maximum and/or average over all/some hours could be relevant for many weather data columns (if weather is bad enough to cause delay for an hour, the delay can accumulate and cause delay several hours later).
Computing the differences from one hour to another for some weather data columns could also be relevant in some case, like wind direction, when the wind changing direction abruptly/frequently can change the flight plan for the landing.

### Cross features

One drawback of GBDT is that they can't easily learn decision boundaries that involve more than one feature.
The effect is that the learnt trees create some "stairs" pattern which is a bad use of the model capacity.
To avoid this, crossed features could be used, especially for features showing up in the feature importance analysis, since a "stairs" pattern mean a lot of splits for the two features.
