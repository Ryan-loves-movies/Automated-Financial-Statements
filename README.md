<p align="center">
  <img style="-webkit-user-select: none; display: block; margin: auto; padding: env(safe-area-inset-top) env(safe-area-inset-right) env(safe-area-inset-bottom)   env(safe-area-inset-left); cursor: zoom-in;" src="https://mpng.subpng.com/20180610/kvb/kisspng-computer-icons-report-clip-art-fatality-analysis-reporting-system-5b1daa0cac57b9.3848326315286707327059.jpg" height = "256px" width = "256px">
  <h1 align="center">AutoFin</h1>
  <h3 align="center">Python script that automates the retrieval of financial statements in most 10-Ks of companies into an excel sheet</h3>
  <p align="center">
    <a href="https://lxml.de/index.html">
	    <img src="https://img.shields.io/badge/built%20with-lxml-green.svg" />
    </a>
    <a href="https://www.python.org/">
    	<img src="https://img.shields.io/badge/built%20with-Python3-red.svg" />
    </a>
  </p>
</p>

# Overview
This is meant to be a simple project of automating the retrieval of financial statements (Balance Sheet Statemenets, Statements of Operations and Statements of Cash Flows) directly from the 10-Ks of companies.
**Table of contents**
- [Prerequisites](#prerequisites)
- [Implementation of program](#implementation-of-program)
- [How it works](#how-it-works)
- [Why I built this](#why-I-built-this)

# Prerequisites
- Python3
  * xlwings
  * asyncio
  * aiohttp
  * requests
  * datetime
  * time
  * lxml
  * pandas
  * numpy
  * pathlib
- xlwings
- Microsoft Excel

## Installing Prerequisites

### Installing Python3
Python3 can be installed via brew
```
brew install python3
```

### Installing python3 prerequisites
Open the terminal from the zip file
```
$ python3 -m pip install -r requirements.txt
```

# Implementation of program

## Downloading
### Zip file
You can simply download the [zip file](https://github.com/Ryan-loves-movies/Automated-Financial-Statements/archive/refs/heads/master.zip) 

### Using git
Execute the following in a directory you want 

```
git clone https://github.com/Ryan-loves-movies/Automated-Financial-Statements.git
```

## Using the program as intended
- Open the financial_statements.xlsm file or [create your own](#creating-your-own-.xlsm-file)
- Open the 'Balance Sheets config' sheet or any of the config sheets
  * Configure which companies you want to scrape - Ticker symbols only
  * Configure which forms to scrape - '10-K' or '10-Q'

Example configuration sheet in excel

|tickers|forms|
|:---|:---|
|TSLA|10-K|
|RBLX|10-Q|
- Enable the developer tab in excel 
  * Under 'Options' -> 'Ribbon' -> Check 'Developer' tab, 
  * Or for mac 'Preferences' -> 'Ribbon' -> Check 'Develper' tab
- Open Visual Basic under the 'Developer' tab
- Insert a new 'module' and insert the code for xlwings:

Example Code:
```
Sub YourFuncName()
    RunPython "import balance_sheet_updater; balance_sheet_updater.main()"
End Sub
```
There are 3 programs to choose to import from - balance_sheet_updater, income_statements_updater, cash_flow_updater

- Make sure you have the sheet with the right name - 'Balance Sheets', 'Income Statements', 'Cash Flows'
- Save, run the macro and go and have a coffee to wait for the program to do its thing

### Creating your own .xlsm file
- In your own file, the same names for the sheets still have to be adhered to (unless you fork and configure the program yourself)
- Under 'Tools' -> 'References' -> Check 'xlwings'
- The rest is the same as configuring the .xlsm file that is already in the zip file



# How it works
There are 3 main steps to the program
- Configuration for which companies to retrieve and whether to retrieve annual and/or quarterly data
  * Retrieve data from excel sheet with pandas.read_excel() and read sheet with specified name into table that can be parsed
- Retrieval of the data (from sec.gov and data.sec.gov)
  * requests.get() is used to get and parse data from data.sec.gov to convert the ticker symbols to CIK
  * asyncio is used to asynchronously request data from data.sec.gov initially to get all filings data for the CIK
  * Data is filtered to just find 10-K and 10-Q
  * For each form, if the form ends in .htm, a request is made to 'sec.gov/Archive/edgar/data/(CIK)/(asc-number)/FilingSummary.xml' to see whether the page exists
  * If site exists, find the specified table and parse from the given link directly
  * If it doesn't exist, go straight to the 10-Q or 10-K filing and search for the table there

There are 3 methods used to find the table in the form
|Method|Speed|
|:---|:---|
|Find the hyperlink in the document that links to the page of the table to extract the table]|Fastest|
|Find the text that only exists in that financial statement and extract the table]|Mid - Quite Fast|
|Find the header that only exists before financial statement and extract the table]|Slowest|

- Updating of the data (in excel)
  * The lists of lists that contain the tables are merged together and converted to a pandas dataframe
  * xlwings is used to update the dataframe into the specified sheets



# Why I built this
After going on a journey looking through companies' financial statements and annual reports, one thing stood out to me -- Not all Financial Statements report the same metrics. 
In other words, oftentimes the data retrieved from tools (yfinance, ...) retrieve the data and process it in their own way to come up with the few same metrics for each financial statement. As far as I am an ameteur in this field, I believe the method of derivation of the metrics are as important as the metrics themselves. As such, I decided to embark on this project to retrieve data on my own from the companies' 10-Ks so that any abnormalities with the data given can be easily noticed and one would be able to find out what the abnormality means, as well as deal with it in their own common sense way.
