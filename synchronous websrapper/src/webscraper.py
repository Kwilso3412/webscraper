### Import needed libraries 
import pandas as pd 
import requests
from bs4 import BeautifulSoup
import time
from random import randint
import re

## Create a header
## Uses a custom user agent, to get this just google "User agent"
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'}


# because of how good reads best of page is set up and there are some incorrectly linked items we have to approach it differently 
# Web scrape the url
def get_url (url_list, url):
    r = requests.get(url, headers=headers)
    if r.status_code == 200:
        print(f"we're in {r.status_code}")
        # Process the successful request
    elif r.status_code in [403, 429, 503]:
        print(f'we failed {r.status_code}')

    url_soup = BeautifulSoup(r.text, 'html.parser')
    url_grab = url_soup.find_all('div', {'class':'u-paddingBottomMedium mediumText'})

    for item in url_grab:
        url_setup = item.find_all('a',{'rel':'nofollow noopener'})
        #this will give you all of the items
        for url in url_setup[4:]:
            # gets the last page
            url_received= url.get('href')
            url_list.append(
                url_received
            )
    return url_list

url_list = []

# inital grab 
url_1 = 'https://www.goodreads.com/list/best_of_year/2024?id=196307.Best_Books_of_2024'
# get the url
url_list = get_url(url_list, url_1)
print('Grabbed url 1')
# grabs 1900 - 1940 
url_2 = 'https://www.goodreads.com/list/show/20833.Best_books_1900'
url_list = get_url(url_list, url_2)
print('Grabbed url 2')
# 1940 grab
url_3 = 'https://www.goodreads.com/list/show/19413'
url_list = get_url(url_list, url_3)
print('Grabbed url 3')

# adds 2013 and 2016 to the list
# this were incorrectly linked as 2003 and 2006 respectably
url_list.extend(
            ["https://www.goodreads.com/list/best_of_year/2013?id=27345.Best_Books_Published_in_2013",
            "https://www.goodreads.com/list/best_of_year/2016?id=95160.Best_Books_of_2016"]
        )
print('URL grabber Succeeded, getting the id')

# sets up main data to be thrown in the get_books function 
df_for_function= pd.DataFrame(url_list, columns=['url'])

# drop the duplicates from the data set
df_for_function.drop_duplicates(inplace=True)

# gets the id for unique sites
def extract_id_from_url(url):
    match = re.search(r'list/show/(\d+)(\.\S+)?', url)
    return match.group(1) if match else None

# Apply the function to the 'url' column and store the result in a new 'id' column
df_for_function['id'] = df_for_function['url'].apply(extract_id_from_url)
print("id's are in the list now one more step grab the numbers")

last_page_number_list = []
# we add in the link from the previous list to for the url
def get_page_number (url_list):
    scrapped_last_page_list = []
    # Scrapes through the website to find the last page
    for url in url_list:
        r = requests.get(url, headers=headers)
        if r.status_code == 200:
            print(f"we're in {r.status_code}")
        # Process the successful request
        elif r.status_code in [403, 429, 503]:
            print(f'we failed {r.status_code}')
        soup = BeautifulSoup(r.text, 'html.parser')
        page_number_scraper = soup.find('div', {'class':'pagination'})
        # gets the last page
        if page_number_scraper:
            next_page = page_number_scraper.find('a',{'class':'next_page'})
            if next_page:
                last_page_grabber = next_page.find_previous_sibling('a')
                scrapped_last_page = last_page_grabber.text.strip()
                print(f'Scrapped {url} amount of pages {scrapped_last_page}')
        else:
            scrapped_last_page = 1
            print(f'Scrapped {url} amount of pages {scrapped_last_page}')
            
        scrapped_last_page_list.append(
            scrapped_last_page
        )
        # Pacing control
        if len(scrapped_last_page_list) % 30 == 0:  # Pause after every 100 items
            pause = randint(15, 30)
            print(f"Processed {len(scrapped_last_page_list)} items, taking a pause of {pause} seconds...")
            time.sleep(pause)
    return scrapped_last_page_list

# runs function and store in a list
last_page_number_list = get_page_number(df_for_function['url'])
# convert all the numbers into int
for i in range(len(last_page_number_list)):
    last_page_number_list[i] = int(last_page_number_list[i])
df_for_function['last_number'] = last_page_number_list

print("")

pause = randint(15,30)
print(f"lets take a pause of {pause} seconds before we grab the headers...")
time.sleep(pause) 

def get_header (url):
    r = requests.get(url, headers=headers)
    if r.status_code == 200:
        print(f"we're in {r.status_code} {url}")
    # Process the successful request
    elif r.status_code in [403, 429, 503]:
        print(f'we failed {r.status_code}{url}')
    # sets up everything to so you can parse through the data
    header_soup = BeautifulSoup(r.text, 'html.parser')
    # gets the item and stores it in a variable
    find_header = header_soup.find_all('h1', {'class':'gr-h1 gr-h1--serif'})
    # Pacing control
    pause_time = randint(1,3)
    time.sleep(pause_time)

    for header_found in find_header:
        return header_found.text.strip()

header_list = []

df_for_function['header'] = df_for_function['url'].apply(get_header)

df_for_function.to_csv("dataframe_going_in.csv")
print("your inputs for the get book function are gathered check the output")


pause = randint(60,120)
print(f"We have all the puzzle pieces now lets take a {pause} seconds...")
print('Otherwise we will get kicked out')
time.sleep(pause) 


def getBook(id, uploaded_url, page, header):
    # initialize a list for the dataset
    get_best_books_list = []
    # Gets the URL
    url= uploaded_url
    current_header = None
    # if the url does not equal 1 it will change teh url and fetch the new results 
    if page != 1:
        if "list/best_of_year/" in uploaded_url:
        # grabs year and header 
            year = uploaded_url.split('/')[-1][:4]
            current_header = uploaded_url.split('?id=')[-1].split('&')[0]
            url = f'https://www.goodreads.com/list/best_of_year/{year}?id=183940.{current_header}&page={page}'
            print(f'I created header: {current_header}')
            print(f"We're at page: {page}")
        elif "list/show/" in uploaded_url:
            current_header = uploaded_url.split('.')[1]
            url = f'https://www.goodreads.com/list/show/{id}.Best_Books_1944?page={page}'
            print(f'I created header: {url}')
            print(f"We're at page: {page}")

    r = requests.get(url, headers=headers)
    soup = BeautifulSoup(r.text, 'html.parser')
    books = soup.find_all('tr', {'itemtype':'http://schema.org/Book'})

    print(f"This link was used {url}")
    ## Find the items 
    ## for this example you'll want to get the text of all the items
    for item in books:
        # first puts it into a dataframe
        rank = item.find('td',{'class':'number'}).text.strip()
        title = item.find('a',{'class':'bookTitle'}).text.strip()
        author_name = item.find('span',{'itemprop':'author'}).text.strip()
        # after this line you will get and error where 'â€”'appears instead of'-'
        avg_rating_and_number_of_ratings = item.find('span',{'class':'minirating'}).text.strip()
        score = item.find('a',{'href':'#'}).text.strip()
        # We need to skip the first one and then grab the second one
        all_href_tags = item.find_all('a', {'href': '#'})
        people_voted_tag = all_href_tags[1]
        people_voted = people_voted_tag.text.strip()

        print(f'Scrapped book at {url}')
        # stores teh value in the list
        get_best_books_list.append({
            'header': header,
            'site_page_number': page,
            'rank': rank,
            'title': title,
            'author_name': author_name,
            'average_rating_and_number_of_Ratings':avg_rating_and_number_of_ratings,
            'score': score,  # You would need to extract this similar to 'people_voted'
            'people_voted': people_voted,
            'url_id' : id
        })
        # Pacing control
        if len(get_best_books_list) % 1000 == 0:
            pause_time = randint(30, 80)
            print(f"Pausing for {pause_time} seconds after processing {len(get_best_books_list)} items.")
            time.sleep(pause_time)
    return get_best_books_list

# we need to enumerate the URL_list and the page number so we only pass the number and not the list
# Otherwise we will directly adding one to the list and python doesn't support that

best_books_list = []

for i, row in df_for_function.iterrows():
    current_last_number = row['last_number']
    for page in range(1, current_last_number + 1):
        # Call getBook with the appropriate id, url, and page
        book_scrapping = getBook(row['id'], row['url'], page, row['header'])
        best_books_list.extend(book_scrapping)

best_books_df= pd.DataFrame(best_books_list)
# Saves the book into a data frame
try:
    best_books_df.to_csv('best_books.csv')
    print("CSV file has been saved successfully.")
except IOError as e:
    print(f"Error saving CSV file: {e}")

print("we got all the books you can find it in your directory")