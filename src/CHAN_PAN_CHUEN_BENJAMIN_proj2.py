import pandas as pd
import requests
from bs4 import BeautifulSoup
import argparse


def request_url(url):
    '''
        request and get content from the given url
    '''
    try:
        content = requests.get(url)
        content.raise_for_status()
    except requests.exceptions.HTTPError as e: #check for error 4XX/5XX
        print(e)
    else:
        #print(f'{content.url} was successfully retrieved with status code {content.status_code}')
        return content
        #print(content)


def make_soup(base_url, url='', tab=''):
    '''
        BeautifulSoup
    '''
    content = request_url(f'{base_url}{url}{tab}')
    soup = BeautifulSoup(content.content, 'lxml')
    return soup


def df_list_to_csv(df_list, filename):
    '''
        store df_list(single dataframe/ list of dataframes) as csv
    '''
    if not isinstance(df_list, list):
        df_list = [df_list]
    result = pd.concat(df_list)
    result.to_csv(f'{filename}.csv', index=False)


def team_url(url, grade):
    '''
        generate a list of urls for all 2019 men's teams
    '''
    soup = make_soup(url)
    #print(soup)

    main_table = soup.find('ul', {"class": "fivb-pools__list"})
    if main_table is not None:
        #print(main_table)
        li = list()
        links = main_table.find_all('a')
        if grade:
            links = links[0:3] # set a limit to parse 3 countries/webpages only when the grade flag is activated
        for link in links:
            li.append(link.get('href'))
        return li


def html_to_df(base_url, url='', tab='', id=None, table_no=0):
    '''
        scrape tables and store it as a list of DataFrames
    '''
    soup = make_soup(base_url, url, tab)
    #print(soup)
    if id is not None:
        start = soup.find('div', {"id": id})
        table = start.find_all('table')
    else:
        table = soup.find_all('table')

    return pd.read_html(str(table))[table_no]


def add_country(df, url, column):
    '''
        add column: country to a DataFrame
    '''
    data = url.rsplit('/', 1)[-1]
    df[f'{column}'] = data
    return df


def vnl_table(team_url_list, filename, tab='', id=None, table_no=0, add_column_country=False):
    '''
        url to csv
    '''
    df_list = list()
    for url in team_url_list:
        df = html_to_df('https://www.volleyball.world', url, tab, id, table_no)
        if add_column_country:
            add_country(df, url, 'Country')
        df_list.append(df)
    df_list_to_csv(df_list, filename)


def remove_total_rows(filename):
    '''
        remove total rows if the table contains them
    '''
    df = csv_to_df(filename)
    df_new = df[~df.iloc[:, 0].str.contains('total', case=False)]
    df_list_to_csv(df_new, filename)


def json_to_df(base_url, codes, grade, suffix=''):
    '''
        read json from a link directly and store it as a dataframe
    '''
    df_list = list()
    if grade:
        codes = codes[:3] # set a limit to 3 api calls only when the grade flag is activated

    for code in codes:
        df_list.append(pd.read_json(f'{base_url}{code}{suffix}'))
    return df_list


def generate_country_code_list(df_list):
    '''
        generate a list of country codes to access all countries' URL
    '''
    li = df_list[0]['key'].tolist()
    for i in range(len(li)):
        li[i] = f'/{li[i]}'
    return li


def generate_vnl_schedule_city_dict(url):
    '''
        generate a dictionary with keys: arena city and match number
    '''
    content = request_url(url).json()
    d = dict()
    for json in content['Matches']:
        if json['Gender'] == 'Men' and json['PoolRoundName'] == 'Preliminary Round':
            d.setdefault('MatchNumber', []).append(json['MatchNumber'])
            d.setdefault('City', []).append(json['Location']['City'])
            #d['City'] = li2.append(json['Location']['City'])
            #print(json['MatchNumber'])
    return d


def dict_to_df(d):
    '''
        turn a dictionary to dataframe
    '''
    return pd.DataFrame.from_dict(d)


def remove_leading_zeros(df, column):
    '''
        remove leading zeros in a column to synchronize keys in different tables for merging
    '''
    df[column] = df[column].str.lstrip('0')
    return df


def merge_table(left, right, key_left, key_right):
    '''
        merge 2 CSVs with a common key
    '''
    df_left = csv_to_df(left)
    df_right = csv_to_df(right)
    df_left[key_left] = df_left[key_left].astype(str)
    df_right[key_right] = df_right[key_right].astype(str)
    #print(df_left)
    return pd.merge(df_left, df_right, how='left', left_on=[key_left], right_on=[key_right])


def generate_city_code_dict(base_url, codes, suffix, api_key):
    '''
        generate a dictionary with key: cities and value: url
        do this so the dictionary key may act as a common key for merging
    '''
    #df_list = list()
    d = dict()
    for code in codes:
        url = f'{base_url}/{code}{suffix}&auth={api_key}'
        url = url.replace(' ', '%20')
        #print(url)
        #df_list.append(request_url(url).json())
        d.setdefault(code, request_url(url).json())
    return d


def generate_geocode_dict(city_longlat_df_dict):
    '''
        generate a dictionary with key:, arena city, longitude, latitude
    '''
    d = dict()
    for key, value in city_longlat_df_dict.items():
        d.setdefault('City', []).append(key)
        d.setdefault('addresst', []).append(value['standard']['addresst'])
        d.setdefault('City_api', []).append(value['standard']['city'])
        d.setdefault('longt', []).append(value['longt'])
        d.setdefault('latt', []).append(value['latt'])
    return d



def csv_to_df(filename, header=1):
    '''
        turn a csv to dataframe
    '''
    try:
        header_rows_list = list(range(header))
    except TypeError:
        print('Please use integers to indicate the number of header rows')

    try:
        return pd.read_csv(f'{filename}.csv', header=header_rows_list)
    except IndexError:
        print('Index out of range')

def scrape_holidays(grade):
    '''
       scrape public holidays dataset online from scratch and store it as csv
    '''
    publicHolidays_availableCountries_df_list = json_to_df('https://date.nager.at/Api/v2', ['/AvailableCountries'], grade)
    df_list_to_csv(publicHolidays_availableCountries_df_list, 'publicHolidays_availableCountries')
    country_codes_list = generate_country_code_list(publicHolidays_availableCountries_df_list)
    publicHolidays_df_list = json_to_df('https://date.nager.at/Api/v2/PublicHolidays/2019', country_codes_list, grade)
    df_list_to_csv(publicHolidays_df_list, 'public_holidays') #if grade is Ture, make 3 API calls to grab 3 countries data only


def scrape_vnl(grade, best_players_rank_dict):
    '''
        scrape vnl datasets online from scratch and store them as csv
    '''
    #if grade is True, team_url would only generate 3 team URLs, so only 3 teams/webpages would be scraped for each dataset
    team_url_list = team_url('https://www.volleyball.world/en/vnl/2019/men/teams', grade)
    vnl_table(team_url_list, 'team_roster', '/team_roster', add_column_country=True)
    for key, value in best_players_rank_dict.items(): #scrape best_xxx stats with fewer codes
        vnl_table(team_url_list, key, '/facts_and_figures', value, add_column_country=True)
        remove_total_rows(key)
    vnl_table(['/en/vnl/2019/men/resultsandranking/round1'], 'results', table_no=1)
    # the 2 tables in the previous url has the same identical tags.
    # The only way I could thought of is to locate the table I want by index

    vnl_schedule_city_dict = generate_vnl_schedule_city_dict('https://www.volleyball.world/en/vnl/2019/api/volley/matches/0/en/user')
    vnl_schedule_city_df = dict_to_df(vnl_schedule_city_dict)
    vnl_schedule_city_df = remove_leading_zeros(vnl_schedule_city_df, 'MatchNumber')
    df_list_to_csv(vnl_schedule_city_df, 'vnl_schedule_city')

    men_result_with_city = merge_table('Results', 'vnl_schedule_city', 'Number', 'MatchNumber')
    df_list_to_csv(men_result_with_city, 'men_result_with_city')
    return vnl_schedule_city_df


def scrape_geocode(vnl_schedule_city_df, grade):
    '''
        scrape geocode dataset online from scratch and store it as csv
        require vnl_schedule_city_df
    '''
    city_codes = list(set(vnl_schedule_city_df['City'].tolist()))
    if grade:
        city_codes = city_codes[0:3] #scrape 3 times only if grade is True
        #print(city_codes)
    # There are usage limits for this api. If running without the --grade flag,
    # do not run the next line of code for more than a few times a day with the same api key.
    # If running with the --grade flag, it is okay to run for more than 10 times.
    # register for a new api key for free if you would like to rerun the code more.
    city_longlat_df_dict = generate_city_code_dict('https://geocode.xyz', city_codes, '?geoit=json',
                                                   '142885146321154e15898865x106483') #<--api key
    geocode_dict = generate_geocode_dict(city_longlat_df_dict)
    geocode_dict_df = dict_to_df(geocode_dict)
    df_list_to_csv(geocode_dict_df, 'geocode')


def main():
    '''
        when invoked, choose to obtain data remotely or locally
        if remotely, scrape online store in csv
        read csv from directory then stick it into our data model and display them
    '''
    parser = argparse.ArgumentParser(description="This is our final project: Volleyball Nations League")

    parser.add_argument("--source", choices=["remote", "local"], required=True, type=str,
                        # A help string to print out when you use -h or --help
                        help="Choose source from local or remote")

    parser.add_argument("--grade", choices=['yes', 'no'], required=False, type=str,
                        help="--grade flag is an optional parameter. "
                        "If you decide to use --grade, you may choose to enter 'yes' or 'no'. "
                        "Enter 'yes' if you would like to grab a maximum of 3 of each data source. "
                        "Enter 'no' or simply do not use the --grade flag if you want to grab the whole data source.")

    args = parser.parse_args()
    source = args.source
    grade = args.grade

    if grade == 'yes':
        grade = True
    else:
        grade = False

    #create the following dictionary to conveniently scrape the best_xxx statistics with fewer lines of codes
    best_players_rank_dict = {
        'best_scorers': 'scorers',
        'best_attackers': 'spikers',
        'best_blockers': 'blockers',
        'best_servers': 'servers',
        'best_setters': 'setters',
        'best_diggers': 'diggers',
        'best_receivers': 'receivers',
    }

    if source == "remote": #scrape data online
        scrape_holidays(grade) #data source 1
        vnl_schedule_city_df = scrape_vnl(grade, best_players_rank_dict) #data source 2
        scrape_geocode(vnl_schedule_city_df, grade) #data source 3

    #grab the data (either locally or remotely) and stick it into our data model - DataFrames

    # data source 1
    #publicHolidays_availableCountries_df = csv_to_df('publicHolidays_availableCountries') #for future merging use
    public_holidays_df = csv_to_df('public_holidays') #public_holidays table

    # data source 2
    team_roster_df = csv_to_df('team_roster') #VNL table 1
    best_scorers_df = csv_to_df('best_scorers') #VNL table 2
    best_attackers_df = csv_to_df('best_attackers') #VNL table 3
    best_blockers_df = csv_to_df('best_blockers') #VNL table 4
    best_servers_df = csv_to_df('best_servers') #VNL table 5
    best_setters_df = csv_to_df('best_setters') #VNL table 6
    best_diggers_df = csv_to_df('best_diggers') #VNL table 7
    best_receivers_df = csv_to_df('best_receivers') #VNL table 8
    men_result_with_city_df = csv_to_df('men_result_with_city', 2) #VNL table 9

    # data source 3
    geocode_df = csv_to_df('geocode') #geocode table

    #display dataframes to graders
    #print(publicHolidays_availableCountries_df)
    print(public_holidays_df)
    print(team_roster_df)
    print(best_scorers_df)
    print(best_attackers_df)
    print(best_blockers_df)
    print(best_servers_df)
    print(best_setters_df)
    print(best_diggers_df)
    print(best_receivers_df)
    print(men_result_with_city_df)
    print(geocode_df)


if __name__ == "__main__":
    main()
