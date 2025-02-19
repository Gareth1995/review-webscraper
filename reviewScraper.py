import os
from playwright.async_api import async_playwright
import asyncio
from anthropic import Anthropic
# from transformers import pipeline
import re
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
POSTGRES_URI = os.getenv('TEMBO_URI')
# HUGGINGFACE_TOKEN = os.getenv('HUGGING_FACE_TOKEN')
# MAX_TOKENS = 512  # Adjust based on sentiment model's limit
ANTHROPIC_API_KEY = os.getenv('ANTHROPIC_API_KEY')

# from huggingface_hub import login
# login(token=HUGGINGFACE_TOKEN)

# classifier = pipeline("text-classification", model="j-hartmann/emotion-english-distilroberta-base", return_all_scores=True)

# os.environ['TF_ENABLE_ONEDNN_OPTS'] = '0'

async def get_neg_pos_review(card):
    # Extract positive review
    positive_review_locator = card.locator('[data-testid="review-positive-text"] span')
    positive_review_count = await positive_review_locator.count()

    if positive_review_count > 1:
        positive_review = await positive_review_locator.nth(1).text_content()  # Get the visible text
    elif positive_review_count == 1:
        positive_review = await positive_review_locator.first.text_content()
    else:
        positive_review = None  # If no positive review exists

    # Extract negative review (same approach)
    negative_review_locator = card.locator('[data-testid="review-negative-text"] span')
    negative_review_count = await negative_review_locator.count()

    if negative_review_count > 1:
        negative_review = await negative_review_locator.nth(1).text_content()
    elif negative_review_count == 1:
        negative_review = await negative_review_locator.first.text_content()
    else:
        negative_review = None  # If no negative review exists

    return positive_review, negative_review

async def get_score(card):
    score_locator = card.locator('.a3b8729ab1.d86cee9b25')

    # Get the full text and extract the last occurrence of the score
    full_text = await score_locator.text_content()
    
    # Extract the numeric score (usually at the end of the text)
    score = full_text.strip().split()[-1]  # This gets the last part (e.g., '8.0')

    return score

async def get_reviewer_name(card):
    name_locator = card.locator('.a3332d346a.e6208ee469')
    
    if await name_locator.count() > 0:  # Check if locator exists
        reviewer_name = await name_locator.text_content()
        return reviewer_name.strip()  # Store cleaned text
    else:
        return None  # Insert None if locator is missing

async def get_review_date(card):
    date_locator = card.locator('[data-testid="review-stay-date"]')
    
    if await date_locator.count() > 0:  # Check if locator exists
        stay_date = await date_locator.text_content()
        return stay_date.strip()  # Store cleaned text
    else:
        return None  # Insert None if locator is missing

async def get_country(card):
    country_locator = card.locator('span.afac1f68d9.a1ad95c055')

    if await country_locator.count() > 0:  # Check if locator exists
        country = await country_locator.text_content()
        return country.strip()  # Store cleaned text
    else:
        return None  # Insert None if locator is missing
    
async def get_apartment_type(card):
    room_locator = card.locator('span[data-testid="review-room-name"]')

    if await room_locator.count() > 0:  # Check if locator exists
        room_name = await room_locator.text_content()
        return room_name.strip()  # Store cleaned text
    else:
        return None  # Insert None if locator is missing
    
async def get_length_of_stay(card):
    nights_locator = card.locator('span[data-testid="review-num-nights"]')

    if await nights_locator.count() > 0:  # Check if locator exists
        num_nights_text = await nights_locator.text_content()
        match = re.search(r'(\d+)', num_nights_text)  # Extract the number using regex
        num_nights = int(match.group(1)) if match else None  # Convert to int if found
    else:
        num_nights = None  # Insert None if locator is missing

    return num_nights

async def get_group_type(card):
    traveler_locator = card.locator('span[data-testid="review-traveler-type"]')

    if await traveler_locator.count() > 0:  # Check if locator exists
        traveler_type = await traveler_locator.text_content()
    else:
        traveler_type = None  # Insert None if locator is missing

    return traveler_type

async def get_partner_reply(card):

    # Look for the "Continue reading" button inside the review card
    continue_reading_button = card.locator('[data-testid="review-pr-toggle"]')

    # Check if the button is visible
    if await continue_reading_button.is_visible():
        # Click to reveal the reply
        await continue_reading_button.click()

        # Wait for the reply text to appear, refine the locator to target the reply specifically
        reply_locator = card.locator('.a53cbfa6de.b5726afd0b span').nth(1)  # Use .nth(1) to target the second <span> (partner's reply)

        await reply_locator.wait_for(timeout=2000)  # Wait for the reply to appear

        # Get the reply text
        reply_text = await reply_locator.text_content()

        if reply_text:
            return reply_text.strip()  # Return the reply text if found
        else:
            return None
    else:
        return None
    
def query_claude(content: str, query: str) -> str:
    """
    Send a query to Claude API about specific content and get the response.
    
    Args:
        content (str): The text content to analyze or reference
        query (str): The question or instruction for Claude about the content
    
    Returns:
        str: Claude's response
        
    Raises:
        Exception: If API call fails or authentication error occurs
    """
    try:
        # Initialize Anthropic client
        client = Anthropic(api_key=ANTHROPIC_API_KEY)
        
        # Construct the message
        system_prompt = "You are Claude, an AI assistant. Please analyze the following content and answer the query about it."
        full_prompt = f"{system_prompt}\n\nContent:\n{content}\n\nQuery:\n{query}"
        
        # Call Claude API
        message = client.messages.create(
            model="claude-3-opus-20240229",
            max_tokens=1000,
            messages=[
                {
                    "role": "user",
                    "content": full_prompt
                }
            ]
        )
        
        # Extract and return Claude's response
        return message.content[0].text
        
    except Exception as e:
        print(f"Error querying Claude API: {e}")
        raise

async def scrape_hotel_reviews(url_link, hotel_id, source_id, filename):

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            context = await browser.new_context()
            page = await context.new_page()

            positive_review_text_array = []
            negative_review_text_array = []
            review_rating = []
            reviewer_names = []
            reviewer_country = []
            review_sentiment = []
            review_dates = []
            apartment_type = []
            num_nights_stay = []
            group_type = []
            review_feedback = []

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

            # for i in range(1, int(last_review_page_num) + 1):
            for i in range(1, 2):
                
                print(f'navigating to page {i}')
                # click button where aria-label = i
                await page.locator(f'button[aria-label=" {i}"]').click()
                await page.wait_for_timeout(3000)
                print('Navigation successful')

                # Get all review cards
                review_cards = await page.locator('[aria-label="Review card"]').all()
                total_reviews = len(review_cards)
                print(f'Found {total_reviews} review cards on page {i}')

                for card in review_cards:
                    
                    ################################################################
                    ##################### REVIEW TEXT SCRAPING #####################
                    ################################################################
                    positive_review, negative_review = await get_neg_pos_review(card)

                    # Store the values
                    positive_review_text_array.append(positive_review)
                    negative_review_text_array.append(negative_review)

                    ################################################################
                    ##################### REVIEW RATING SCRAPE #####################
                    ################################################################
                    score = await get_score(card)
                    review_rating.append(score)

                    ################################################################
                    ##################### REVIEWER NAME SCRAPE #####################
                    ################################################################
                    rev_name = await get_reviewer_name(card)
                    reviewer_names.append(rev_name)

                    ################################################################
                    ##################### REVIEWER DATE SCRAPE #####################
                    ################################################################
                    rev_date = await get_review_date(card)
                    review_dates.append(rev_date)

                    ###################################################################
                    ##################### REVIEWER COUNTRY SCRAPE #####################
                    ###################################################################
                    rev_country = await get_country(card)
                    reviewer_country.append(rev_country)

                    ##########################################################################
                    ##################### REVIEWER APARTMENT TYPE SCRAPE #####################
                    ##########################################################################
                    rev_apartment_type = await get_apartment_type(card)
                    apartment_type.append(rev_apartment_type)

                    ###################################################################
                    ##################### REVIEWER LENGTH OF STAY #####################
                    ###################################################################
                    rev_length_stay = await get_length_of_stay(card)
                    num_nights_stay.append(rev_length_stay)

                    ###################################################################
                    ##################### REVIEWER GROUP TYPE #########################
                    ###################################################################
                    rev_group_type = await get_group_type(card)
                    group_type.append(rev_group_type)

                    ###################################################################
                    ##################### FEEDBACK TO REVIEWER ########################
                    ###################################################################
                    partner_reply = await get_partner_reply(card)
                    review_feedback.append(partner_reply)

                    ###################################################################
                    ##################### REVIEWER SENTIMENT ##########################
                    ###################################################################
                    sample_content = "Positive: " + str(positive_review) + " negative: " + str(negative_review)
                    sample_query = "tell me which sentiment fits this review best: anger, disgust, fear, joy, neutral, sadness, surprise GIVE ME ONLY THE SENTIMENT. NO OTHER WORDS."
                    
                    try:
                        response = query_claude(sample_content, sample_query)
                        print("Claude's response:", response)
                        review_sentiment.append(response)
                        
                    except Exception as e:
                        print(f"Failed to get response: {e}")

                print(f'Extracted {len(review_sentiment)} reviewer feedbacks so far')
                
                                
                # print(f'Extracted {len(positive_review_text_array)} positive reviews so far')
                # print(f'Extracted {len(negative_review_text_array)} negative reviews so far')
                # print(f'Extracted {len(review_rating)} review ratings so far')
                # print(f'Extracted {len(reviewer_names)} review names so far')
                # print(f'Extracted {len(review_dates)} review dates so far')
                # print(f'Extracted {len(reviewer_country)} reviewer countries so far')
                # print(f'Extracted {len(apartment_type)} reviewer apartment types so far')
                # print(f'Extracted {len(num_nights_stay)} reviewer stay length so far')
                # print(f'Extracted {len(group_type)} reviewer group types so far')
                # print(f'Extracted {len(review_feedback)} reviewer feedbacks so far')
                
                
            # print('All positive reviews:', positive_review_text_array)
            # print('All negative reviews:', negative_review_text_array)
            # print('All review ratings:', review_rating)
            # print('All review names:', reviewer_names)
            # print('All review dates:', review_dates)
            # print('All reviewer countries:', reviewer_country)
            # print('All reviewer apartment types:', apartment_type)
            # print('All reviewer stay lengths:', num_nights_stay)
            # print('All reviewer group types:', group_type)
            # print('All reviewer feedbacks:', review_feedback)
            print('All reviewer sentiments:', review_sentiment)
            # print(f'Total positive reviews: {len(positive_review_text_array)}')
            # print(f'Total negative reviews: {len(negative_review_text_array)}')         


        await browser.close()

        # save data as csv
        # print('saving review data to csv')
        # filename = 'output/'+filename
        # review_df.to_csv(filename, index=False)

        # return review_df

    except Exception as e:
        print(f"An error occurred: {e}")
        return {"error": str(e)}

# MAY CHANGE THIS TO APPENED LOAD CSV TO POSTGRES
def load_to_postgres(df):
    # Create a connection string (replace with your actual database details)
    db_url = POSTGRES_URI

    # Create an SQLAlchemy engine
    engine = create_engine(db_url)

    # Convert the DataFrame to a list of dictionaries (one dictionary per row)
    data = df.to_dict(orient='records')

    # Construct the insert statement with "ON CONFLICT DO NOTHING"
    insert_query = text("""
            INSERT INTO reviews (hotel_id, source_id, review_text, review_rating, reviewer_name, review_date, sentiment, country)
            VALUES (:hotel_id, :source_id, :review_text, :review_rating, :reviewer_name, :review_date, :sentiment, :country)
            ON CONFLICT (hotel_id, source_id, reviewer_name, review_text) DO NOTHING;
        """)
    
    # Execute the query for each row in the DataFrame
    with engine.connect() as conn:
        for row in data:
            # print(f"Inserting row: {row}")  # Log the row being inserted
            conn.execute(insert_query, row)
            # print(f"{row} successfully inserted")
        
        conn.commit()

    print("Data loaded successfully, duplicate reviews ignored.")


# pulling user reviews from booking.com for Kwantu Guesthouse 1
# review_df = asyncio.run(scrape_hotel_reviews("https://www.booking.com/hotel/za/kwantu-guesthouses-cape-town.html?aid=304142&label=gen173nr-1FCAEoggI46AdIM1gEaPsBiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKwgd68BsACAdICJDg3NWYxYmY0LTBjNDktNGRiYy04Y2Q1LWUxOTAxZTY0MjgxONgCBeACAQ&sid=989dc5e594027c7ff3b4d7505cacb436&dest_id=-1217214&dest_type=city&dist=0&group_adults=2&group_children=0&hapos=1&hpos=1&no_rooms=1&req_adults=2&req_children=0&room1=A%2CA&sb_price_type=total&sr_order=popularity&srepoch=1737982144&srpvid=15855a1d9db00417&type=total&ucfs=1&",
#                                              hotel_id='e1ada55f-000c-4991-9527-f72362cb6e80',
#                                              source_id='25e89862-0a2c-4d53-900a-6cb3300c4268'))

# pulling user reviews from booking.com for The Bantry Aparthotel by Totalstay
asyncio.run(scrape_hotel_reviews("https://www.booking.com/hotel/za/bantry-bay-suite-hotel-cape-town.html?aid=304142&label=gen173nr-1FCAEoggI46AdIM1gEaPsBiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuALBgf28BsACAdICJDlhNDU1ZjQ1LWRiNmMtNGM0OC1iMDgxLWViNWY1NDZiYjYwNdgCBeACAQ&sid=989dc5e594027c7ff3b4d7505cacb436&dest_id=-1217214&dest_type=city&dist=0&group_adults=2&group_children=0&hapos=4&hpos=4&no_rooms=1&req_adults=2&req_children=0&room1=A%2CA&sb_price_type=total&sr_order=popularity&srepoch=1738499906&srpvid=b15f58da98a20577&type=total&ucfs=1&#tab-main",
            hotel_id = 'b8e318bb-dade-45e5-b87a-b8359f077a2d',
            source_id= '25e89862-0a2c-4d53-900a-6cb3300c4268',
            filename='bantry_aparthotel.csv'
            ))
# print(review_df[['reviewer_name', 'review_text', 'sentiment']])

# print('Loading dataframe into database')
# load_to_postgres(review_df)
