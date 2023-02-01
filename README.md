# to4-consumption-anomalies

## Instructions
Detailed usage instructions can be found in the Consumption Anomalies Tool User Guide.docx

## Background
The Consumption Anomalies tool is designed to detect anomalous activity in consumption quantities of stock data. The tool was originally developed and used in Zambia. The solution was later refactored to be used on generalized stock data in ordered to be used by a broader set of countries. A simple user interface was added so users can run the tool from start to finish without needing to interact directly with the code. 

Using historical consumption data, the tool detects anomalies in consumption at the facility and product level and flags these combinations to the user for review.

### Anomaly Detection Method

The Consumption Anomaly Detection tool uses a method called Statistical Process Control (SPC) to detect anomalies. SPC is a standard practice in the manufacturing industry. It detects anomalies in data by calculating a moving range, with an upper and lower bound based on points of data over time. 

This method is useful because it accounts for some reasonable increases and decreases in values over time, which will not be flagged as anomalies, while being able to detect “unexpected” deviations in the data. For the application of facility consumption data, this means that a facility can may have a gradual decrease or increase in consumption of a product over time without it being flagged as an anomaly. 

### Tool overview

The tool has been designed to work on any country's data that meets certain data requirements. The user inputs dataset-specific information which map to required arguments for the tool

#### Data requirements
Excel or csv data containing product, facility, consumption, date 

####
Input - excel or csv
User interface - python gui using pysimplegui
Analysis - python script
Ouput - csv tables
Visualization - excel dashboard, csv output connected via Power Query

## To run
pip install requirements.txt

python consumption_analysis_gui.py
