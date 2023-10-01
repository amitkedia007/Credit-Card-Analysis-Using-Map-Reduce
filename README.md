# My Contributions to the project:

## Design & Implementation:
Conceptualized and designed a two-stage MapReduce algorithm to analyze credit card spending patterns.
Utilized two mappers and two reducers, interconnected to answer the research question regarding average spending by city and the top 3 cities with the highest average spending.
Processed data from the "CreditCard2.txt" file, ensuring seamless data flow from Mapper-1 to Reducer-2.

## Data Processing:
Extracted and highlighted relevant features, specifically 'City' and 'Amount', from the credit card spending dataset for analysis.
Designed the flow and logic for both Job-1 and Job-2, ensuring accurate data processing and aggregation.

## Algorithm Development:
Developed Mapper-1 to filter and extract relevant data, emitting key-value pairs based on city names and associated transaction amounts.
Implemented Reducer-1 to compute the average transaction amount for each city.
Designed Mapper-2 to prepare data for Reducer-2, which identified the top 3 cities with the highest average spending using a TreeMap data structure.

## Code Implementation:
Authored and tested the code for both mappers and reducers, ensuring accurate data processing and result generation.
Utilized the Hadoop framework for MapReduce operations, extending relevant classes and overriding methods for custom functionality.

## Results & Evaluation:
Orchestrated the entire MapReduce program using the driver class, ensuring sequential execution and data flow.
Successfully answered the research question by calculating the average amount spent by each city and identifying the top 3 cities with the highest average spending.
Presented results with clear visual representations and detailed explanations.

## Conclusion:
Demonstrated the power and efficiency of the Hadoop MapReduce framework in processing large datasets and extracting valuable insights.
Highlighted the significance of high-performance computational infrastructure in data analytics.

### For the detailed analysis report check the Report in the repository 
