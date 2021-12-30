import pathlib
script_path = str(pathlib.Path(__file__).parent.resolve())
import sys
sys.path.append(script_path)
from processor_classes.user_agents import headers
from processor_classes.scraper import scraper
from processor_classes.ratelimiter import ratelimiter

import time
from lxml import etree
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from pprint import pprint
import asyncio
import aiohttp
import pathlib


class processor():
    """
    Asynchronous requesting of data
    Note: All asynchronous requsting is rate limited to sec's rate limit - 10 req/s
    """
    def __init__(self, ticker_form_list):
        """

        Parameters
        ----------
        ticker_form_list: Raw output from retriver(...).retrieve_data(...)
                          i.e. [['TSLA', '10-K'],
                                ['RBLX', '10-Q'],
                                ...]

        """
        self.ticker_form_list = ticker_form_list
        self.ticker_list = []
        for i in ticker_form_list:
            self.ticker_list.append(i[0])
        self.data = []

    def get_json_cik(self):
        """

        Returns
        -------
        Dictionary of ticker symbols of list containing cik, json cik, and form type

        E.g. {'AAPL': ['320193', 'CIK0000320193.json', '10-Q'], 'MSFT': ['789019', 'CIK0000789019.json', '10-K']}

        """
        header = headers().header()
        data = requests.get('https://www.sec.gov/files/company_tickers_exchange.json', headers = header).json()
        ticker_to_cik = {}
        for i in data['data']:
            ticker_to_cik[i[2]] = i[0]

        self.full_urls = {}
        for i in self.ticker_form_list:
            ticker = i[0]
            form = i[1]
            cik = str(ticker_to_cik.get(ticker))
            json_cik = 'CIK' + cik.zfill(10) + '.json'
            self.full_urls[ticker] = [cik, json_cik, form]
        # print(self.full_urls)
        return self.full_urls

    def get_form_links(self, dictionary_cik, financial_statement):
        """

        Parameters
        ----------
        dictionary_cik: Dictionary returned from processor(ticker_form_list).get_json_cik()
            E.g. {'AAPL': ['320193', 'CIK0000320193.json', '10-Q'], 'MSFT': ['789019', 'CIK0000789019.json', '10-K']}
        financial_statement: Report wanted -- Only 'balance sheets', 'statement of operations', 'statement of cash flows' are accepted

        Returns a dictionary of ticker symbols that give list of
        reportDate,
        link,
        boolean to signify if the url should be parsed as html or txt and
        either a link to the table directly or a boolean False indicating it doesn't exist

        E.g.
        {'AAPL': [['2021-09-25',
                    '320193/000032019321000105/aapl-20210925.htm'
                    True,
                    'http://sec.gov/Archives/data/edgar/320193/000032019321000105/R2.htm'], ...
                ]
        'MSFT': [['2021-06-30',
                    '789019/000156459021039151/msft-10k_20210630.htm',
                    True,
                    False], ...
                ]
        }
        -------

        """

        def get_filing_sum_link(url_and_fulldoc, table):
            """
            Function to determine if FilingSummary.xml index exits and if it does,
            append the link to the table directly instead
            Else, filingsummary=False

            Parameters
            ----------
            url: '(CIK)/(acs-number)/FilingSummary.xml'
            header: Header for requests
            table: Report wanted --> 'balance sheets', ...

            Returns the exact full link to retrieve the table wanted --> Balance Sheet if the link is valid
            Else, return False
            -------

            """
            full_url = url_and_fulldoc[0]
            fulldoc = url_and_fulldoc[1]
            reports = fulldoc.xpath('.//Report')

            # If reports is not an empty list
            if not len(reports):
                return False

            if table == 'balance sheets':
                # Define list to append to for checking
                make_requests = []
                for report in reports:
                    name = report.find('./ShortName').text.lower().strip(' ').strip('(unaudited)').strip(' ').rstrip('s')

                    if name == 'consolidated balance sheet' or name == 'consolidated balance sheet statement' or name == 'condensed consolidated balance sheet' or name == 'consolidated condensed balance sheet':
                        try:
                            html_url = report.find('./HtmlFileName').text
                            return full_url.replace('FilingSummary.xml', html_url)
                        except AttributeError:
                            xml_url = report.find('./XmlFileName').text
                            return full_url.replace('FilingSummary.xml', xml_url)
                    elif 'balance sheet' in name:
                        try:
                            html_url = report.find('./HtmlFileName').text
                            make_requests.append([full_url.replace('FilingSummary.xml', html_url), 'html'])
                        except AttributeError:
                            xml_url = report.find('./XmlFileName').text
                            make_requests.append([full_url.replace('FilingSummary.xml', xml_url), 'xml'])
                # If 'balance sheet' is actually in the names
                if make_requests:
                    # Asynchronously make requsts and get result
                    async def get(url, session, transform):
                        async with session.get(url=url[0]) as response:
                            resp = await response.read()
                            if url[1] == 'html':
                                table_str = resp.decode('utf-8')
                            else:
                                dom = etree.fromstring(resp, parser=etree.XMLParser())
                                newdoc = transform(dom)
                                table_str = etree.tostring(newdoc.xpath('.//table[1]')[0])
                            if ('asset' in table_str) and ('liabilities' in table_str) and ('equity' in table_str):
                                return url[0]
                            else:
                                return 'Null'
                    async def main_get(urls):
                        # xml schema provided by edgar to decrypt xml
                        xsl_filename = f'{pathlib.Path().resolve()}/InstanceReport.xslt'
                        xslt = etree.parse(xsl_filename, parser=etree.XMLParser())
                        transform = etree.XSLT(xslt)

                        async with aiohttp.ClientSession(headers=header) as session:
                            ret = await asyncio.gather(*[get(url, session, transform) for url in urls])
                            ret = [i for i in ret if i != 'Null']
                            if ret:
                                return ret[0]
                            else:
                                return False
                    filingsummary = asyncio.run(main_get(make_requests))
                    return filingsummary
                else:
                    return False
            elif table == 'statement of operations':
                # Define list to append to for checking
                make_requests = []
                for report in reports:
                    name = report.find('./ShortName').text.lower().strip(' ').strip('(unaudited)').strip(' ').rstrip('s')
                    names = ['consolidated statement of income',
                              'consolidated statements of income',
                              'condensed consolidated statement of income',
                              'condensed consolidated statements of income',
                              'consolidated condensed statement of income',
                              'consolidated condensed statements of income',

                              'consolidated statement of operation',
                              'consolidated statements of operation',
                              'condensed consolidated statement of operation',
                              'condensed consolidated statements of operation',
                              'consolidated condensed statement of operation',
                              'consolidated condensed statements of operation']
                    if name in names:
                        try:
                            html_url = report.find('./HtmlFileName').text
                            return full_url.replace('FilingSummary.xml', html_url)
                        except AttributeError:
                            xml_url = report.find('./XmlFileName').text
                            return full_url.replace('FilingSummary.xml', xml_url)
                    elif 'balance sheet' in name:
                        try:
                            html_url = report.find('./HtmlFileName').text
                            make_requests.append([full_url.replace('FilingSummary.xml', html_url), 'html'])
                        except AttributeError:
                            xml_url = report.find('./XmlFileName').text
                            make_requests.append([full_url.replace('FilingSummary.xml', xml_url), 'xml'])
                # If 'balance sheet' is actually in the names
                if make_requests:
                    # Asynchronously make requsts and get result
                    async def get(url, session, transform):
                        async with session.get(url=url[0]) as response:
                            resp = await response.read()
                            if url[1] == 'html':
                                table_str = resp.decode('utf-8')
                            else:
                                dom = etree.fromstring(resp, parser=etree.XMLParser())
                                newdoc = transform(dom)
                                table_str = etree.tostring(newdoc.xpath('.//table[1]')[0])
                            if ('net income' in table_str) and ('operating income' in table_str or 'income from operation' in table_str) and ('revenue' in table_str or 'sales' in table_str):
                                return url[0]
                            else:
                                return 'Null'

                    async def main_get(urls):
                        # xml schema provided by edgar to decrypt xml
                        xsl_filename = f'{pathlib.Path().resolve()}/InstanceReport.xslt'
                        xslt = etree.parse(xsl_filename, parser=etree.XMLParser())
                        transform = etree.XSLT(xslt)

                        async with aiohttp.ClientSession(headers=header) as session:
                            ret = await asyncio.gather(*[get(url, session, transform) for url in urls])
                            ret = [i for i in ret if i != 'Null']
                            if ret:
                                return ret[0]
                            else:
                                return False

                    filingsummary = asyncio.run(main_get(make_requests))
                    return filingsummary
                else:
                    return False
            elif table == 'statement of cash flows':
                # Define list to append to for checking
                make_requests = []
                for report in reports:
                    name = report.find('./ShortName').text.lower().strip(' ').strip('(unaudited)').strip(' ').rstrip('s')
                    names = ['consolidated statement of cash flow',
                             'consolidated statements of cash flow',
                             'condensed consolidated statement of cash flow',
                             'condensed consolidated statements of cash flow',
                             'consolidated condensed statement of cash flow',
                             'consolidated condensed statements of cash flow']
                    if name in names:
                        try:
                            html_url = report.find('./HtmlFileName').text
                            return full_url.replace('FilingSummary.xml', html_url)
                        except AttributeError:
                            xml_url = report.find('./XmlFileName').text
                            return full_url.replace('FilingSummary.xml', xml_url)
                    elif 'balance sheet' in name:
                        try:
                            html_url = report.find('./HtmlFileName').text
                            make_requests.append([full_url.replace('FilingSummary.xml', html_url), 'html'])
                        except AttributeError:
                            xml_url = report.find('./XmlFileName').text
                            make_requests.append([full_url.replace('FilingSummary.xml', xml_url), 'xml'])
                # If 'balance sheet' is actually in the names
                if make_requests:
                    # Asynchronously make requsts and get result
                    async def get(url, session, transform):
                        async with session.get(url=url[0]) as response:
                            resp = await response.read()
                            if url[1] == 'html':
                                table_str = resp.decode('utf-8')
                            else:
                                dom = etree.fromstring(resp, parser=etree.XMLParser())
                                newdoc = transform(dom)
                                table_str = etree.tostring(newdoc.xpath('.//table[1]')[0])
                            if ('operating activities' in table_str) and ('financing activities' in table_str) and ('investing activities' in table_str):
                                return url[0]
                            else:
                                return 'Null'

                    async def main_get(urls):
                        # xml schema provided by edgar to decrypt xml
                        xsl_filename = f'{pathlib.Path().resolve()}/InstanceReport.xslt'
                        xslt = etree.parse(xsl_filename, parser=etree.XMLParser())
                        transform = etree.XSLT(xslt)

                        async with aiohttp.ClientSession(headers=header) as session:
                            ret = await asyncio.gather(*[get(url, session, transform) for url in urls])
                            ret = [i for i in ret if i != 'Null']
                            if ret:
                                return ret[0]
                            else:
                                return False

                    filingsummary = asyncio.run(main_get(make_requests))
                    return filingsummary
                else:
                    return False
            elif table == 'whole report':
                return 'Null'

        def test_against(fin_dict, ticker_symbol, form, form_name, accnum, reportdate, primdoc, cik):
            """
            Repetitive function that does the tests

            Parameters
            ----------
            fin_dict: Dictionary to update
            ticker_symbol: Ticker symbol in the dictionary to access and update the value of ['forms']
            form: form name given by the .json request
            form_name: '10-K' or '10-Q' -- Results are filtered based on whether the name of the form is equal to this
            accnum: list of accession numbers given by the .json request
            reportdate: list of report dates given by the .json request
            primdoc: list of primaryDocument given by the .json request

            Returns an updated fin_dict with the fin_dict[ticker_symbol] updated to:
            [reportDate, url, html_bool, filingsummary link (if it exists else False)]
            [['2021-11-12', '789019/000156459021039151/msft-10k_20210630.htm', True, '789019/000156459021039151/R2.htm'], ...]

            -------

            """
            if form == form_name:
                extension = primdoc[num]
                # if primdoc string is not empty and the extension is .htm or .html
                if extension and (extension[-4:] != '.txt'):
                    base_middle_url = f"{cik}/{''.join(accnum[num].split('-'))}"
                    form_url = f'{base_middle_url}/{extension}'
                    html_bool = True
                    filingsummary_url = f'{base_middle_url}/FilingSummary.xml'
                else:
                    form_url = f"{cik}/{accnum[num]}.txt"
                    html_bool = False
                    filingsummary_url = False
                # update fin_dict
                fin_dict[ticker_symbol].append([reportdate[num], form_url, html_bool, filingsummary_url])
            return fin_dict

        header = headers().header()
        base_filings_url = 'https://data.sec.gov/submissions/'

        # Defining async function to request asynchronously
        async def get(url, session, list_to_update, ticker):
            async with await session.get(url) as response:
                try:
                    resp = await response.json()

                except ContentTypeError as e:
                    print("Unable to get url {} due to {}.".format(url, e.__class__))
                    print(f"Probably exceeded request rate of sec to get request response of {response.status} and got your ip banned for the next 10 minutes\nSorry about that! :(")
                    return 'Null'
                now = time.monotonic() - session.START
                print(f"{now:.5f}s: got {url} with {response.status}")
                list_to_update.pop(1)
                list_to_update.insert(1, resp)
                list_to_update.append(ticker)
                return list_to_update
        async def main_get(urls):
            """

            Parameters
            ----------
            urls: {'AAPL': ['320193', 'CIK0000320193.json', '10-Q'], 'MSFT': ['789019', 'CIK0000789019.json', '10-K']}

            Returns dictionary {'AAPL': ['320193', {ActualResponse}, '10-Q'], 'MSFT': ['789019', {ActualResponse}, '10-K']}
            -------

            """
            base_filings_url = 'https://data.sec.gov/submissions/'
            async with aiohttp.ClientSession(headers=header) as session:
                session = ratelimiter(session)
                ret = await asyncio.gather(*[get((base_filings_url + urls.get(url)[1]), session, urls.get(url), url) for url in urls])
                new_dict = {i[3]:i[:-1] for i in ret}
                return new_dict
        # End of Defining of async function

        dictionary_of_resp = asyncio.run(main_get(dictionary_cik))

        fin_dict = {}
        for ticker_symbol in dictionary_of_resp:
            parameter_list = dictionary_cik[ticker_symbol]
            cik = parameter_list[0]
            resp = parameter_list[1]
            if resp == 'Null':
                print('bad response for this ticker probably because of the IP ban\nSorry! Gonna have to try again in about 10 minutes!')
                continue
            form_to_fetch = parameter_list[2]

            # Define list for info to be added to
            fin_dict[ticker_symbol] = []

            # Recent Filings
            accnum = resp['filings']['recent']['accessionNumber']
            reportdate = resp['filings']['recent']['reportDate']
            forms = resp['filings']['recent']['form']
            primdoc = resp['filings']['recent']['primaryDocument']

            # Fetch older filings
            older_filings = resp['filings']['files']

            # If older_filings is not empty
            if older_filings:
                # Compile a list of all the urls for the older filings
                urls = [base_filings_url+filing['name'] for filing in older_filings]

                # Defining of async function
                async def get(url, session):
                    try:
                        async with await session.get(url) as response:
                            try:
                                resp = await response.json()
                            except ContentTypeError as e:
                                print("Unable to get url {} due to {}.".format(url, e.__class__))
                                print(f"Probably exceeded request rate of sec to get request response of {response.status} and got your ip banned for the next 10 minutes\nSorry about that! :(")
                                return 'Null', 'Null', 'Null', 'Null'
                            now = time.monotonic() - session.START
                            print(f"{now:.5f}s: got {url} with {response.status}")
                            # print("Successfully got url {} with resp of length {}.".format(url, len(resp)))
                            accnum = resp['accessionNumber']
                            reportdate = resp['reportDate']
                            forms = resp['form']
                            primdoc = resp['primaryDocument']
                            return accnum, reportdate, forms, primdoc
                    except Exception as e:
                        print("Unable to get url {} due to {}.".format(url, e.__class__))
                async def main_get(urls, old_accnum, old_reportdate, old_forms, old_primdoc):
                    async with aiohttp.ClientSession(headers=header) as session:
                        session = ratelimiter(session)
                        ret = await asyncio.gather(*[get(url, session) for url in urls])
                        for resp in ret:
                            old_accnum.append(resp[0])
                            old_reportdate.append(resp[1])
                            old_forms.append(resp[2])
                            old_primdoc.append(resp[3])
                        return accnum, reportdate, forms, primdoc
                # Async function defined
                accnum, reportdate, forms, primdoc = asyncio.run(main_get(urls, accnum, reportdate, forms, primdoc))

            fin_dict[ticker_symbol] = [accnum, reportdate, forms, primdoc, form_to_fetch, cik]

        for ticker_symbol in fin_dict:
            accnum = fin_dict[ticker_symbol][0]
            reportdate = fin_dict[ticker_symbol][1]
            forms = fin_dict[ticker_symbol][2]
            primdoc = fin_dict[ticker_symbol][3]
            form_to_fetch = fin_dict[ticker_symbol][4]
            cik = fin_dict[ticker_symbol][5]

            fin_dict[ticker_symbol] = []
            inside_start = time.perf_counter()
            # Get and compile all the links
            if form_to_fetch == '10-Q':
                for num, form in enumerate(forms):
                    fin_dict = test_against(fin_dict=fin_dict, ticker_symbol=ticker_symbol, form=form, form_name='10-K', accnum=accnum, reportdate=reportdate, primdoc=primdoc, cik=cik)
                    fin_dict = test_against(fin_dict=fin_dict, ticker_symbol=ticker_symbol, form=form, form_name='10-Q', accnum=accnum, reportdate=reportdate, primdoc=primdoc, cik=cik)
                # Sort the forms by date
                all_forms = fin_dict.get(ticker_symbol)
                fin_dict[ticker_symbol] = sorted(all_forms, key=lambda x: datetime.strptime(x[0], '%Y-%m-%d'), reverse=True)
            else:
                for num, form in enumerate(forms):
                    fin_dict = test_against(fin_dict=fin_dict, ticker_symbol=ticker_symbol, form=form,form_name='10-K', accnum=accnum, reportdate=reportdate, primdoc=primdoc, cik=cik)

            # Defining of async function
            async def get(url, session, list_to_update):
                # if list_to_update[3] is False
                if not list_to_update[3]:
                    return list_to_update
                async with await session.get(url) as response:
                    resp = await response.read()
                    fulldoc = etree.fromstring(resp, parser=etree.XMLParser())
                    list_to_update.pop(3)
                    list_to_update.append([url, fulldoc])
                    return list_to_update
            async def main_get(urls):
                base_data_url = 'https://www.sec.gov/Archives/edgar/data/'
                async with aiohttp.ClientSession(headers=header) as session:
                    session = ratelimiter(session)
                    ret = await asyncio.gather(*[get(url=f'{base_data_url}{url[3]}', session=session, list_to_update=url) for url in urls])
                    return ret
            # End of async function
            fin_dict[ticker_symbol] = asyncio.run(main_get(fin_dict.get(ticker_symbol)))

            list_of_lists = fin_dict.get(ticker_symbol)
            for list_to_update in list_of_lists:
                if list_to_update[3] is not False:
                    list_to_update[3] = get_filing_sum_link(list_to_update[3], table=financial_statement)
            fin_dict[ticker_symbol] = list_of_lists

        return fin_dict

    def download(self, forms_dict, table_type = 'balance_sheet_tables'):
        """

        Parameters
        ----------
        forms_dict: The direct output of self.get_form_links()
                    (i.e. {'AAPL': [['2021-09-25',
                                    '320193/000032019321000105/aapl-20210925.htm'
                                    True,
                                    'http://sec.gov/Archives/data/edgar/320193/000032019321000105/R2.htm'], ...
                                ]
                        'MSFT': [['2021-06-30',
                                    '789019/000156459021039151/msft-10k_20210630.htm',
                                    True,
                                    False], ...
                                ]
                        }

        Returns a similar output but with the tables in place of the url and boolean
                    (i.e. {'AAPL': [
                                  [['Current assets:',None,None],
                                  ...],
                                  ...
                                  ],

                           'MSFT': [
                                  [['Current assets:',None,None],
                                  ...]
                                  ],

                           ...
                          }
                    )
        -------

        """
        header = headers().header()
        # Asyncio to get the network requests as fast as possible
        # Changed code to return content of GET response and store it in
        for ticker_symbol in forms_dict:
            all_forms = forms_dict.get(ticker_symbol)

            # Defining the async functions
            async def get(url, session):
                base_data_url = 'https://www.sec.gov/Archives/edgar/data/'
                date = url[0]
                actual_url = base_data_url + url[1]
                html_bool = url[2]
                filingsummary = url[3]
                # If filingsummary contains something
                if filingsummary:
                    # If .xml file, it should be noted and saved in filingsummary
                    if filingsummary[-4:] == '.xml':
                        async with await session.get(filingsummary) as response:
                            resp = await response.read()
                            return [date, resp, html_bool, 'xml']
                    # Should be a .htm file then, that is fine
                    else:
                        async with await session.get(filingsummary) as response:
                            resp = await response.read()
                            return [date, resp, html_bool, 'html']
                else:
                    async with await session.get(actual_url) as response:
                        resp = await response.read()
                        return [date, resp, html_bool, filingsummary]
            async def main_get(urls):
                async with aiohttp.ClientSession(headers = header) as session:
                    session = ratelimiter(session)
                    ret = await asyncio.gather(*[get(url, session) for url in urls])
                    return ret
            # End of defining of async functions

            # Add responses for all files
            forms_dict[ticker_symbol] = asyncio.run(main_get(all_forms))

            # Simple function to run concurrently
            def scrape(resp_list):
                date = resp_list[0]
                resp = resp_list[1]
                html_bool = resp_list[2]
                filingsummary = resp_list[3]
                res = scraper()
                table = eval(f'res.get_{table_type}(resp={resp}, html_bool={html_bool}, filingsummary="{filingsummary}")')
                return table

            print('Now scraping')
            start = time.perf_counter()
            with ThreadPoolExecutor() as e:
                res = [e.submit(scrape, i) for i in forms_dict.get(ticker_symbol)]
                forms_dict[ticker_symbol] = [i.result() for i in res]
            print(f'concurrent.futures module took {time.perf_counter() - start}s to scrape everything\n')

        return forms_dict

if __name__=='__main__':
    tryout = processor([['AAPL', '10-Q'], ['MSFT', '10-K']])
    list = tryout.get_json_cik()
    links = tryout.get_form_links(list)
    pprint(links)
    pprint(tryout.download(links))