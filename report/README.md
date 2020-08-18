# Predicting flight delays

Victor Guerra <vm.guerramoran@criteo.com>
Stephane Le Roy<s.leroy@criteo.com>

## Introduction

In this report, we present the work done to implement a Machine Learning pipeline that trains a model that predicts delays in US Flights.
We take as reference a paper published at beginning of 2016 [Using Scalable Data Mining for Predicting Flight Delays](https://www.researchgate.net/publication/292539590_Using_Scalable_Data_Mining_for_Predicting_Flight_Delays) by Belcastro, Marozzo, Talia and Trunfio.

## Implementation

### Reading datasets and preprocessing
### Joins
### Transformation pipeline
### Model

## Feature engineering

### Flight data features

#### Origin and destination airports

The origin and destination airports of the flights are a categorical features, fed to the model using a string indexer.
Since the cardinality is quite high, some airports might have relatively few flights, so another way to encode the feature was implemented by using the number of flights per airports as a feature.
This way the model can learn something from the airport, even on low traffic ones.

#### Timestamp data

The time of the flight can a very important feature for a delay prediction.
Since the model that is used in the end is Gradient Boosted Decision Trees, the timestamp value in seconds can be used directly by the model without much preprocessing, no bucketization or scaling/normalization required (directly handled by the model).
One important piece of information that is not extracted easily by the GBDT is the periodicity (yearly, weekly, daily periodicities). For this purpose other features has been built, DayOfYear, SecondOfDay and DayOfWeek.

### Weather data features

#### Weather type

The string is composed of several 2 characters substrings, each representing one type of weather ('RA': rain, 'SN': snow), with an optionnal character prefix ('-': light intensity, blank: Moderate intensity, '+' Heavy intensity)
https://en.wikipedia.org/wiki/METAR#METAR_WX_codes
The data is data is splitted into several columns (one for each possible type of weather), with values equal to 0 if the it's absent, and the values of 1, 2, or 3 if present (respectively with a '-' prefix, no prefix, '+' prefix)

#### Sky condition

The string is composed of several 6 characters substrings, each representing a type of cloud coverage at a given altitude.
The 3 first characters represents the cloud coverage percentage (in oktas) (FEW: "Few" 1–2 oktas, SCT: "Scattered" 3–4 oktas, etc).
The 3 last characters represents the altitude.
https://en.wikipedia.org/wiki/METAR#Cloud_reporting
The data is data is splitted into several columns (one for each possible coverage percentage), with values equal to the min altitude reported for this coverage, or the max altitude (999) if not reported.

#### Visibility, Wind Speed, Wind Direction, Humidity and Pressure

Since GBDT models are quite good at handling real values like these features, those were used directly without scaling/normalization or other preprocessing.
It could be argued that the cyclic nature of the Wind Direction (359° is close to 0°) could be encoded, but the gain would be too limited to be worthwhile.

## Final results

## Other experiments

## Conclusion
