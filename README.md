# Phonepe-Pulse-Data-Visualization-and-Exploration

The project is about the extract the data from Phonepe repository and process it to obtain insights and information that can be visualized in a user-friendly manner. 

**ABOUT PHONEPE:**

PhonePe is an Indian digital payments and financial services company headquartered in Bengaluru, Karnataka, India. PhonePe was founded in December 2015, by Sameer Nigam, Rahul Chari and Burzin Engineer .The PhonePe app, based on the Unified Payments Interface (UPI), went live in August 2016.

The PhonePe app is available in 11 Indian languages. Using PhonePe, users can send and receive money, recharge mobile, DTH, data cards, make utility payments, pay at shops, invest in tax saving funds, liquid funds, buy insurance, mutual funds, and digital gold.

PhonePe is accepted as a payment option by over 3.5 crore offline and online merchant outlets, constituting 99% of pin codes in the country.

**Phonepe Pulse:**

PhonePe Pulse is a feature offered by the Indian digital payments platform called PhonePe.

PhonePe Pulse provides users with insights and trends related to their digital transactions and usage patterns on the PhonePe app. It offers personalized analytics, including spending patterns, transaction history, and popular merchants among PhonePe users.

This feature aims to help users track their expenses, understand their financial behavior, and make informed decisions.

**Phonepe Pulse Data Visualisation:**

Data visualization refers to the graphical representation of data using charts, graphs, and other visual elements to facilitate understanding and analysis

These visualizations are designed to present information in a visually appealing and easily digestible format, enabling users to quickly grasp trends, patterns, and insights from their transaction history.**

**Problem Statement:**

The Phonepe pulse Github repository contains a large amount of data related to various metrics and statistics.The goal is to extract this data and process it to obtain insights and information that can be visualized in a user-friendly manner.

**Approach:**

**1. Data extraction:**

Clone the Github using scripting to fetch the data from the Phonepe pulse Github repository and store it in a suitable format such as CSV or JSON.

**2. Data transformation:**

Use a scripting language such as Python, along with libraries such as Pandas, to manipulate and pre-process the data.
This may include cleaning the data, handling missing values, and transforming the data into a format suitable for analysis and visualization.

**3. Database insertion:**

Use the "mysql-connector-python" library in Python to connect to a MySQL database and insert the transformed data using SQL commands.

**4. Dashboard creation:**

Use the Streamlit and Plotly libraries in Python to create an interactive and visually appealing dashboard.
Plotly's built-in plots would display the data on different plots. Streamlit can be used to create a user-friendly interface with multiple dropdown options for users to select different facts and figures to display. 
* An interactive dashboard reports are generated and filtered through a Tableau.
* View the dashboard using the link mentioned below.
* Tableau link: https://public.tableau.com/app/profile/karthikeyan.m2180/vizzes

**5. Data retrieval:**

Use the "mysql-connector-python" library to connect to the MySQL database and fetch the data into a Pandas dataframe.
Use the data in the dataframe to update the dashboard dynamically.

**6. Deployment:**

Ensuring the solution is secure, efficient, and user-friendly.
Testing the solution thoroughly and deploy the dashboard publicly, making it accessible to users.
Technologies:
Github Cloning
Python
Pandas
MySQL
mysql-connector-python
Streamlit
Plotly

