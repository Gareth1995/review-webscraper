import os
from playwright.async_api import async_playwright
import asyncio
from bs4 import BeautifulSoup
from transformers import pipeline
import re
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()
POSTGRES_URI = os.getenv('TEMBO_URI')
HUGGINGFACE_TOKEN = os.getenv('HUGGING_FACE_TOKEN')

from huggingface_hub import login
login(token=HUGGINGFACE_TOKEN)

classifier = pipeline("text-classification", model="j-hartmann/emotion-english-distilroberta-base", return_all_scores=True)

os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'

async def scrape_hotel_reviews(url_link):

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            context = await browser.new_context()
            page = await context.new_page()

            reviews_html = []
            review_text = []
            reviewer_name = []
            reviewer_country = []
            review_sentiment = []
            review_date = []

            ################### Scraping for Booking.com ###################
            search_url = (url_link)
            await page.goto(search_url)

            await page.wait_for_timeout(3000)

            # click on see reviews button
            await page.locator('[data-testid="fr-read-all-reviews"]').click()
            print('Clicked on see reviews button')
            
            # // Locate the last <li> element within the <ol> under the <div> with class = ab95b25344
            last_review_page_num = await page.locator('div.ab95b25344 > ol > li:last-child').text_content()
            print('Last review page number:', last_review_page_num)

            for i in range(1, int(last_review_page_num) + 1):
            # for i in range(1, 10):
                
                print(f'navigating to page {i}')
                # click button where aria-label = i
                await page.locator(f'button[aria-label=" {i}"]').click()
                await page.wait_for_timeout(3000)
                print('Navigation successful')

                # get all review cards
                reviews = page.locator('[aria-label="Review card"]')
                print(f'reviews on page {i} obtained')

                # get count of review statements
                count = await reviews.count()
                print('Number of reviews:', count)

                # get text content of each review statement
                for j in range(count):
                    # Get the HTML content of each review div
                    div_html = await reviews.nth(j).evaluate("(element) => element.outerHTML")
                    if div_html:
                        reviews_html.append(div_html)

            print('Total number of reviews:', len(reviews_html)) 

        # Parse the HTML with BeautifulSoup
        for review in reviews_html:
            soup = BeautifulSoup(review, 'html.parser')

            # Get the nationality, name and review date of each reviewer
            divs = soup.find_all('div', class_="b817090550 c44c37515e")
            # Extract the alt text from the img tag within each div
            for div in divs:
                # get reviewer country
                img_tag = div.find('div', class_="abf093bdfe f45d8e4c32").find('img')
                if img_tag and 'alt' in img_tag.attrs:  # Ensure the img tag and 'alt' attribute exist
                    alt_text = img_tag['alt']
                    reviewer_country.append(alt_text)
                else:
                    print('no alt tag found')
                
                # get reviewer name
                rev_name = div.find('div', class_ = "a3332d346a e6208ee469").get_text()
                reviewer_name.append(rev_name)

                # get date of review
                rev_date = div.find('span', class_ = 'abf093bdfe d88f1120c1').get_text()
                review_date.append(rev_date)

            # get review text from every review card
            # Find all div elements with the specified class
            divs = soup.find_all('div', class_="c624d7469d a0e60936ad a3214e5942")

            # Loop through each div and extract text from span tags
            for div in divs:
                # Extract positive review text
                positive_div = div.find('div', {'data-testid': 'review-positive-text'})
                positive_text = positive_div.get_text(strip=True) if positive_div else ""

                # Extract negative review text
                negative_div = div.find('div', {'data-testid': 'review-negative-text'})
                negative_text = negative_div.get_text(strip=True) if negative_div else ""

                # Concatenate positive and negative reviews
                combined_review = f"{positive_text} {negative_text}".strip()
                
                # Append to the reviews list if there's any text
                if combined_review:

                    def convert_to_win1252(text):
                        try:
                            return text.encode('utf-8').decode('windows-1252')
                        except UnicodeEncodeError:
                            return text.encode('utf-8', 'ignore').decode('windows-1252')  # Ignore unconvertible characters

                    # calculate combined review sentiment 
                    text_emotion_pred = classifier(combined_review)

                    if text_emotion_pred:
                        # Find the label with the highest score
                        max_emotion = max(text_emotion_pred[0], key=lambda x: x['score'])

                        # Get the label with the highest score
                        label_with_highest_score = max_emotion['label']

                        combined_review = convert_to_win1252(combined_review)
                        review_text.append(combined_review)
                        review_sentiment.append(label_with_highest_score)
                

        # pad nas to review list
        review_text = review_text + [None] * (len(reviews_html) - len(review_text)) # NAs to review texts for missing text
        review_sentiment = review_sentiment + [None] * (len(reviews_html) - len(review_sentiment)) # NAs to review sentiments for missing text   

        print('Number of countries:', len(reviewer_country))
        # print(reviewer_country)

        print('Number of reviews:', len(review_text))
        # print(review_text)

        print('Number of named reviewers:', len(reviewer_name))
        # print(reviewer_name)

        print('Number of review timestamps:', len(review_date))
        # print(review_date)

        print('Number of review sentiments:', len(review_sentiment))
        # print(review_sentiment)

        # Create a DataFrame from the lists
        review_df = pd.DataFrame({
            'hotel_id': 2,
            'source_id': 1,
            'review_text': review_text,
            'reviewer_name': reviewer_name,
            'review_date': review_date,
            'sentiment': review_sentiment,
            'country': reviewer_country,
            
        })

        await browser.close()

        return review_df

    except Exception as e:
        print(f"An error occurred: {e}")
        return {"error": str(e)}

def load_to_postgres(df):
    # Create a connection string (replace with your actual database details)
    db_url = POSTGRES_URI

    # Create an SQLAlchemy engine
    engine = create_engine(db_url)

    # Write the DataFrame to a PostgreSQL table
    df.to_sql('reviews', engine, if_exists='replace', index=False)


review_df = asyncio.run(scrape_hotel_reviews("https://www.booking.com/hotel/za/kwantu-guesthouses-cape-town.html?aid=304142&label=gen173nr-1FCAEoggI46AdIM1gEaPsBiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKwgd68BsACAdICJDg3NWYxYmY0LTBjNDktNGRiYy04Y2Q1LWUxOTAxZTY0MjgxONgCBeACAQ&sid=989dc5e594027c7ff3b4d7505cacb436&dest_id=-1217214&dest_type=city&dist=0&group_adults=2&group_children=0&hapos=1&hpos=1&no_rooms=1&req_adults=2&req_children=0&room1=A%2CA&sb_price_type=total&sr_order=popularity&srepoch=1737982144&srpvid=15855a1d9db00417&type=total&ucfs=1&"))
# review_df = asyncio.run(scrape_hotel_reviews("https://www.booking.com/hotel/za/bantry-bay-suite-hotel-cape-town.html?aid=304142&label=gen173nr-1FCAEoggI46AdIM1gEaPsBiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuALBgf28BsACAdICJDlhNDU1ZjQ1LWRiNmMtNGM0OC1iMDgxLWViNWY1NDZiYjYwNdgCBeACAQ&sid=989dc5e594027c7ff3b4d7505cacb436&dest_id=-1217214&dest_type=city&dist=0&group_adults=2&group_children=0&hapos=4&hpos=4&no_rooms=1&req_adults=2&req_children=0&room1=A%2CA&sb_price_type=total&sr_order=popularity&srepoch=1738499906&srpvid=b15f58da98a20577&type=total&ucfs=1&#tab-main"))
print(review_df)
print('Loading dataframe into database')
load_to_postgres(review_df)
print('Dataframe loaded to database')
