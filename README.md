# weather

Weather Assignment

Author: Suprabhat

Run

To compile and test the application:

Create the jar file if using gradle follow below steps
Step 1 - Open terminal/CD prompt
Step 2 - copy the project from GitHub to ur folder
Step 3 - go to cd prompt and go to the directory where you copied the project
Step 4 - run - gradle clean assemble -- this will create a jar file in build folder, go to builder and then to lib and copy that jar
Step 5 - if Hadoop is installed please run below command
/bin/spark-submit --class weatther.app.weather --master local <jar file name>

Else if gradle is not installed 
Please download the jar file from the project in the GitHub.
And directly run the jar file using below command
/bin/spark-submit --class weatther.app.weather --master local base-engine-1.0-SNAPSHOT
In this case jar file name is base-engine-1.0-SNAPSHOT.jar

Else 
Copy the code directly to eclipse, then right click on the project, go to the property and select scala compiler and change the scala installation to 2.10 bundle
Scala version is 2.10
And then run the application using run as scala application.


Requirements

Create a toy simulation of the environment (taking into account things like atmosphere, topography, geography, oceanography, or similar) that evolves over time. Then take measurements at various locations and times, and have your program emit that data, as in the following:

Location	Position	   Local time	       Condition	Temperature	Pressure	Humidity
Sydney	        -33.86,151.21,39   2015-12-23 16:02:12	Rain	           +12.5	1010.3	 	  97
Melbourne	-37.83,144.98,7	   2015-12-25 02:30:55	Snow		    -5.3	998.4		  55
Adelaide	-34.92,138.62,48.  2016-01-04 23:05:37	Sunny		   +39.4	1114.1.		  12

Obviously you can’t give it to us as a table (ok, yes, you could feed us markdown, but let’s not do that?) so instead submit your data to us in the following format:

    Sydney|-33.86,151.21,39|2015-12-23T05:02:12Z|Rain|+12.5|1004.3|97
    Melbourne|-37.83,144.98,7|2015-12-24T15:30:55Z|Snow|-5.3|998.4|55
    Adelaide|-34.92,138.62,48|2016-01-03T12:35:37Z|Sunny|+39.4|1114.1|12
where

Location is an optional label describing one or more positions,

Position is a comma-separated triple containing latitude, longitude, and elevation in metres above sea level,

Local time is a date time,

Conditions is either Snow, Rain, Sunny,

Temperature is in °C,

Pressure is in hPa, and

Relative humidity is a %.

Your toy weather simulation should report data from a reasonable number of positions; 10±. The weather simulation will be used for games and does not need to be meteorogically accurate, it just needs to be emit weather data that looks plausible to a layperson.

So far we have assumed that our game takes place on Earth, leading to the use of latitude and longitude for co-ordinates and earth-like conditions. If you choose to assume that the game takes place elsewhere, please document any corresponding changes to the output format.

Constraints and Assumptions:-

Data of temperature, pressure and humidity for a particular day for different time intervals(Local date and Time) extracted from Bureau of meteorology department.

Latitude, longitude of different locations from GeoHack sources.

Altitude is elevation gathered from different internet sources

Local Time is directly refers in the form of the standard needed.


Astronomic metrics

Latitude:

Latitude is the angular position in the North-South axis of Earth's surface. The closer you are to a Tropic in the Summer, the greater is the temperature. The closer you to a Pole, the colder is the weather.

It goes from 90° at the North Pole, to -90° at the South Pole, being the Equator the 0° mark.

Longitude:
Longitude is a geographic coordinate that specifies the east-west position of a point on the Earth's surface. It is an angular measurement, usually expressed in degrees and denoted by the Greek letter lambda (λ).

The longitude of other places is measured as the angle east or west from the Prime Meridian, ranging from 0° at the Prime Meridian to +180° eastward and −180° westward. 


Atmospheric metrics

Altitude
We call Altitude as the elevation from the sea level.

Humidity
Here humidity is termed as relative humidity. 

Air pressure

Air pressure is the weight of atmosphere on a specific point and we will use hPa (hectopascals) as unit. One Pascal is equivalent to one Newton over a square meter[2.3].

The standard air pressure at sea level is 1013.25 hPa[2.2] and we'e going to use this value as reference in our calculations.



Data Acquisition Process:-

Temperature = is Average temperature of Min temperature and Max temperature

Altitude  = Elevation from sea level

Relative humidity = average of relative humidity for a particular day extracted from bureau of meteorology

Pressure = average of pressure for a particular day extracted from bureau of meteorology

Data Preparation :-

Create data frame having details of different locations and their geography details.

11 different data frames having details of atmospheric details for a day in different interval of times.

11 different data frames having date time details.

Data Cleaning:-

Joined different data frames to get the desired output. Cleaned unnecessary data column.

Data Modelling :-

Prepared UDF for concatenation of geographic details , calculating average temperature and condition deverivation. Performed featured engineering and prediction logic


Conditions -
//IF HUMIDITY IS MORE THAN 60% it was A RAINY DAY 
//TEMPERATURE IS LESS THAN OR EQUAL TO 0 DEGREE IT Was a SNOW
//TEMPERATURE MORE THAN 0 AND HUMIDITY LESS THAN 60 IT was A SUNNY DAY

References:

Wikipedia for Latitude and Longitude and elevation details

Temperature, Pressure and humidity from Bureau of Meteorology.

Geohack 

