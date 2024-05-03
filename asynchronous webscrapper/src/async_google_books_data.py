import asyncio
import aiohttp
from aiohttp import ClientTimeout
from bs4 import BeautifulSoup
import asyncio
import pandas as pd
import time
from random import randint

# main book class
class BookFinder:
    def __init__(self):
        # sets up the base url to be transformed
        self.base_url = "https://www.googleapis.com/books/v1/volumes"
'''
The get_books_info function will create the url to receive information from google apis resources. It will then grab the information and
then move to the next function to put the information into a data frame. The function will ensure it does not wait longer than 60 seconds 
and time out with an error message. 
'''
async def get_books_info(self, title,retries=10, wait=2, retry_count=5):
        # transforms the base url to be used for gathering the book details
        # example url: https://www.googleapis.com/books/v1/volumes?q=intitle:"To%20Kill%20a%20Mockingbird"&maxResults=10
        params = {
            'q': f'intitle:"{title}"',
            'maxResults': 10  # Adjust the number of results as needed
        }
        # this will make a request to get the information and move it to next function. 
        try:
            timeout = ClientTimeout(total=60)  # Adjust the timeout value based on your requirements
            async with aiohttp.ClientSession(timeout=timeout) as session:
                for attempt in range(retries):
                    async with session.get(self.base_url, params=params) as response:
                        # if it can successfully grab the information and move to the next function
                        if response.status == 200:
                            book_data = await response.json()
                            return await self.find_book_details(book_data, title)
                        # if the rate limit error 429 is received we back off and then retry again
                        elif response.status == 429:
                            print(f"Rate limit exceeded, retrying in {wait} seconds...")
                            await asyncio.sleep(wait)
                            wait *= 3  # Exponential backoff
                        # if we get a the service temporarily unavailable error 503 then we will take a random pause and then continue
                        elif response.status_code == 503:
                            pause = randint(30, 50)
                            for attempt in range(retry_count):
                                print(f"Attempt {attempt + 1} failed with status 503. Retrying after {pause} seconds...")
                                time.sleep(pause)
                        else:
                        # if we get an error not listed above we will return the failed to retrieve so we can review it later 
                        # it will also print the error status and the error message for further debugging
                            error_message = await response.text()
                            print(f"Failed to retrieve data for {title}: {response.status} - {error_message}")
                            return pd.DataFrame()
                #if we reach our max retired we will ultimately break 
                print("Max retries exceeded.")
                return pd.DataFrame()
        
        except asyncio.TimeoutError:
            print("The request timed out. Trying again...")
            return await self.get_books_info(title, params) 

'''
This next function will take the json received form the previous function and it will removed all the json tag and
and place it in a data frame that is ready to use. 
'''
async def find_book_details(self, book_data, input_title):
        first_not_for_sale_book = None
        # this will get the items from the book
        books = book_data.get('items', [])
        # then it will cycle through the book and grab all the information while cleaning it to get rid of the json syntax
        for book in books:
            volume_info = book.get('volumeInfo', {})
            sale_info = book.get('saleInfo', {})
            saleability = sale_info.get('saleability', '')
            details = {
                'good_reads_title': input_title,  # Include the input title in the details
                'google_title': volume_info.get('title', 'Unknown Title'),
                'ISBN_10': None,
                'ISBN_13': None,
                'pageCount': volume_info.get('pageCount', None),
                'mainCategory': volume_info.get('mainCategory', None),
                'categories': ', '.join(volume_info.get('categories', [])),
                'language': volume_info.get('language', 'Unknown'),
                'country': sale_info.get('country', 'Unknown'),
                'saleability': saleability,
                'listPrice': sale_info.get('listPrice', {}).get('amount'),
                'currencyCode': sale_info.get('listPrice', {}).get('currencyCode'),
                'retailPrice': sale_info.get('retailPrice', {}).get('amount'),
                'retailCurrencyCode': sale_info.get('retailPrice', {}).get('currencyCode')
            }
            identifiers = volume_info.get('industryIdentifiers', [])
            for identifier in identifiers:
                if identifier['type'] == 'ISBN_10':
                    details['ISBN_10'] = identifier['identifier']
                elif identifier['type'] == 'ISBN_13':
                    details['ISBN_13'] = identifier['identifier']
            # Check if book is for sale and return the first instance
            if saleability == 'FOR_SALE':
                return pd.DataFrame([details], columns=details.keys())
            # Store the first not for sale book details if it has not already been stored
            if not first_not_for_sale_book and saleability == 'NOT_FOR_SALE':
                first_not_for_sale_book = details
        # If no for-sale book found but a not-for-sale book was stored, return it
        if first_not_for_sale_book:
            return pd.DataFrame([first_not_for_sale_book], columns=first_not_for_sale_book.keys())
        # Return an empty DataFrame if no suitable book is found
        return pd.DataFrame()


# Asynchronous usage
async def main():
    book_finder = BookFinder()
    #read in teh data and get the titles needed
    best_books_df = pd.read_csv("best_books.csv")
    book_titles = best_books_df["title"].tolist()
    all_details = []
    # will iterate through the book titles in the data frame and apply it to the function in BookFinder() class
    for title in book_titles:
        book_details_df = await book_finder.get_books_info(title)
        if not book_details_df.empty:
            # if can successfully grab the book details 
            print(f"Details retrieved for {title}")
            all_details.append(book_details_df)
        else:
            # if we are unable to grab the book details
            print(f"No details found for {title}")
    # will concat all the book information and then save it as a csv
    if all_details:
        full_details_df = pd.concat(all_details, ignore_index=True)
        full_details_df.to_csv('google_book_details.csv', index=False)
        print("Data saved to 'google_book_details.csv'.")
    else:
        print("No book details found to save.")

# Run the async main function
asyncio.run(main())