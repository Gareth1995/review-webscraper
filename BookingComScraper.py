import os
from playwright.async_api import async_playwright
import asyncio
from anthropic import Anthropic
import re
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

class BookingComScraper:

    POSTGRES_URI = os.getenv('TEMBO_URI')

    ANTHROPIC_API_KEY = os.getenv('ANTHROPIC_API_KEY')
    TIMEOUT_LENGTH = 500
    
    def __init__(self):

        load_dotenv()
        
    async def get_neg_pos_review(self, card):
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

    async def get_score(self, card):
        score_locator = card.locator('.a3b8729ab1.d86cee9b25')

        # Get the full text and extract the last occurrence of the score
        full_text = await score_locator.text_content()
        
        # Extract the numeric score (usually at the end of the text)
        score = full_text.strip().split()[-1]  # This gets the last part (e.g., '8.0')

        return score

    async def get_reviewer_name(self, card):
        name_locator = card.locator('.a3332d346a.e6208ee469')
        
        if await name_locator.count() > 0:  # Check if locator exists
            reviewer_name = await name_locator.text_content()
            return reviewer_name.strip()  # Store cleaned text
        else:
            return None  # Insert None if locator is missing

    async def get_checkin_date(self, card):
        date_locator = card.locator('[data-testid="review-stay-date"]')
        
        if await date_locator.count() > 0:  # Check if locator exists
            stay_date = await date_locator.text_content()
            return stay_date.strip()  # Store cleaned text
        else:
            return None  # Insert None if locator is missing

    async def get_country(self, card):
        country_locator = card.locator('span.afac1f68d9.a1ad95c055')

        if await country_locator.count() > 0:  # Check if locator exists
            country = await country_locator.text_content()
            return country.strip()  # Store cleaned text
        else:
            return None  # Insert None if locator is missing
    
    async def get_apartment_type(self,card):
        room_locator = card.locator('span[data-testid="review-room-name"]')

        if await room_locator.count() > 0:  # Check if locator exists
            room_name = await room_locator.text_content()
            return room_name.strip()  # Store cleaned text
        else:
            return None  # Insert None if locator is missing
    
    async def get_length_of_stay(self, card):
        nights_locator = card.locator('span[data-testid="review-num-nights"]')

        if await nights_locator.count() > 0:  # Check if locator exists
            num_nights_text = await nights_locator.text_content()
            match = re.search(r'(\d+)', num_nights_text)  # Extract the number using regex
            num_nights = int(match.group(1)) if match else None  # Convert to int if found
        else:
            num_nights = None  # Insert None if locator is missing

        return num_nights

    async def get_group_type(self, card):
        traveler_locator = card.locator('span[data-testid="review-traveler-type"]')

        if await traveler_locator.count() > 0:  # Check if locator exists
            traveler_type = await traveler_locator.text_content()
        else:
            traveler_type = None  # Insert None if locator is missing

        return traveler_type

    async def get_partner_reply(self, card):

        # Look for the "Continue reading" button inside the review card
        continue_reading_button = card.locator('[data-testid="review-pr-toggle"]')

        # Check if the button is visible
        if await continue_reading_button.is_visible():
            # Click to reveal the reply
            await continue_reading_button.click()

            # Wait for the reply text to appear, refine the locator to target the reply specifically
            # reply_locator = card.locator('.a53cbfa6de.b5726afd0b span').nth(1)  # Use .nth(1) to target the second <span> (partner's reply)
            reply_locator = card.locator('[data-testid="review-partner-reply"] .a53cbfa6de.b5726afd0b span')
            await reply_locator.wait_for(timeout=self.TIMEOUT_LENGTH)  # Wait for the reply to appear (try commenting out this wait)

            # Get the reply text
            reply_text = await reply_locator.text_content()

            if reply_text:
                return reply_text.strip()  # Return the reply text if found
            else:
                return None
        else:
            return None

    async def get_review_created_date(self, card):
        # Look for the review date inside the review card
        date_locator = card.locator('[data-testid="review-date"]')

        # Get the text content (e.g., "Reviewed: February 12, 2025")
        date_text = await date_locator.text_content()

        if date_text:
            # Extract the date value (e.g., "February 12, 2025")
            review_date = date_text.replace("Reviewed: ", "").strip()
            return review_date
        else:
            return None

    
    def query_claude(self, content: str, query: str) -> str:
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
            client = Anthropic(api_key=self.ANTHROPIC_API_KEY)
            
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

    def create_review_dataframe(self, hotel_id, hotel_name, source_name, positive_review_text_array, negative_review_text_array, review_rating, reviewer_names, reviewer_country, review_sentiment, review_checkin_dates, review_created_date, apartment_type, num_nights_stay, group_type, review_feedback, review_texts):
        # Create a dictionary with each array as a key-value pair
        data = {
            'hotel_id': hotel_id,
            'hotel_name': hotel_name,
            'source_name': source_name,
            'positive_review': positive_review_text_array,
            'negative_review': negative_review_text_array,
            'review_rating': review_rating,
            'reviewer_name': reviewer_names,
            'country': reviewer_country,
            'sentiment': review_sentiment,
            'reviewer_check_in_date': review_checkin_dates,
            'review_created_date': review_created_date,
            'apartment_type': apartment_type,
            'length_nights_stay': num_nights_stay,
            'group_type': group_type,
            'review_feedback': review_feedback,
            'seen': False,
            'review_text': review_texts
        }
        
        # Create a DataFrame using the dictionary
        df = pd.DataFrame(data)
        
        return df

    def save_to_csv(self, filename, df):
        # Check if the file already exists
        if os.path.exists(filename):
            # Prompt the user for confirmation
            user_input = input(f"The file '{filename}' already exists. Do you want to overwrite it? (y/n): ").strip().lower()

            # If the user confirms, save the dataframe as CSV
            if user_input == 'y':
                df.to_csv(filename, index=False)
                print(f"Review data saved to '{filename}'")
            else:
                print("File not overwritten. Review data not saved.")
        else:
            # If the file doesn't exist, save the dataframe as CSV
            df.to_csv(filename, index=False)
            print(f"Review data saved to '{filename}'")
        
    def get_sentiment(self, text, query):
            
        try:
            response = self.query_claude(text, query)
            # print("Claude's response:", response)
            return response
            
        except Exception as e:
            print(f"Failed to get response: {e}")


    async def scrape_hotel_reviews(self, url_link, hotel_id, hotel_name, source_name, filename):

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
                review_checkin_dates = []
                review_created_date = []
                apartment_type = []
                num_nights_stay = []
                group_type = []
                review_feedback = []
                review_texts = []

                ################### Scraping from Booking.com ###################
                search_url = (url_link)
                await page.goto(search_url)

                await page.wait_for_timeout(self.TIMEOUT_LENGTH)

                # click on see reviews button
                await page.locator('[data-testid="fr-read-all-reviews"]').click()
                print('Clicked on see reviews button')
                
                # // Locate the last <li> element within the <ol> under the <div> with class = ab95b25344
                last_review_page_num = await page.locator('div.ab95b25344 > ol > li:last-child').text_content()
                print('Last review page number:', last_review_page_num)

                for i in range(1, int(last_review_page_num) + 1):
                # for i in range(1, 3):
                    
                    print(f'navigating to page {i}')
                    # click button where aria-label = i
                    await page.locator(f'button[aria-label=" {i}"]').click()
                    await page.wait_for_timeout(self.TIMEOUT_LENGTH)
                    print('Navigation successful')

                    # Get all review cards
                    review_cards = await page.locator('[aria-label="Review card"]').all()
                    total_reviews = len(review_cards)
                    print(f'Found {total_reviews} review cards on page {i}')

                    for card in review_cards:
                        
                        ################################################################
                        ##################### REVIEW TEXT SCRAPING #####################
                        ################################################################
                        positive_review, negative_review = await self.get_neg_pos_review(card)

                        # Store the values
                        positive_review_text_array.append(positive_review)
                        negative_review_text_array.append(negative_review)

                        ################################################################
                        ##################### REVIEW RATING SCRAPE #####################
                        ################################################################
                        score = await self.get_score(card)
                        review_rating.append(score)

                        ################################################################
                        ##################### REVIEWER NAME SCRAPE #####################
                        ################################################################
                        rev_name = await self.get_reviewer_name(card)
                        reviewer_names.append(rev_name)

                        ########################################################################
                        ##################### REVIEWER CHECKIN DATE SCRAPE #####################
                        ########################################################################
                        rev_checkin_date = await self.get_checkin_date(card)
                        review_checkin_dates.append(rev_checkin_date)

                        ########################################################################
                        ##################### REVIEWER CREATED DATE SCRAPE #####################
                        ########################################################################
                        review_date_created = await self.get_review_created_date(card)
                        review_created_date.append(review_date_created)

                        ###################################################################
                        ##################### REVIEWER COUNTRY SCRAPE #####################
                        ###################################################################
                        rev_country = await self.get_country(card)
                        reviewer_country.append(rev_country)

                        ##########################################################################
                        ##################### REVIEWER APARTMENT TYPE SCRAPE #####################
                        ##########################################################################
                        rev_apartment_type = await self.get_apartment_type(card)
                        apartment_type.append(rev_apartment_type)

                        ###################################################################
                        ##################### REVIEWER LENGTH OF STAY #####################
                        ###################################################################
                        rev_length_stay = await self.get_length_of_stay(card)
                        num_nights_stay.append(rev_length_stay)

                        ###################################################################
                        ##################### REVIEWER GROUP TYPE #########################
                        ###################################################################
                        rev_group_type = await self.get_group_type(card)
                        group_type.append(rev_group_type)

                        ###################################################################
                        ##################### FEEDBACK TO REVIEWER ########################
                        ###################################################################
                        partner_reply = await self.get_partner_reply(card)
                        review_feedback.append(partner_reply)
                    

                        ###################################################################
                        ##################### REVIEWER SENTIMENT ##########################
                        ###################################################################
                        sample_content = "Positive: " + str(positive_review) + " negative: " + str(negative_review)
                        sample_query = "tell me which sentiment fits this review best: anger, disgust, fear, joy, neutral, sadness, surprise GIVE ME ONLY THE SENTIMENT. NO OTHER WORDS."

                        # check if there was any review
                        if positive_review or negative_review:
                            review_texts.append(sample_content)
                            rev_sent = self.get_sentiment(sample_content, sample_query)
                            review_sentiment.append(rev_sent)
                            # review_sentiment.append('joy')
                                
                        else:
                            review_sentiment.append(None)
                            review_texts.append(None)

                                    
                print(f'Extracted {len(positive_review_text_array)} positive reviews in total')
                print(f'Extracted {len(negative_review_text_array)} negative reviews in total')
                print(f'Extracted {len(review_rating)} review ratings in total')
                print(f'Extracted {len(reviewer_names)} review names in total')
                print(f'Extracted {len(review_checkin_dates)} review dates in total')
                print(f'Extracted {len(reviewer_country)} reviewer countries in total')
                print(f'Extracted {len(apartment_type)} reviewer apartment types in total')
                print(f'Extracted {len(num_nights_stay)} reviewer stay length in total')
                print(f'Extracted {len(group_type)} reviewer group types in total')
                print(f'Extracted {len(review_feedback)} reviewer feedbacks in total')
                print(f'Extracted {len(review_sentiment)} reviewer feedbacks in total')
                print(f'Extracted {len(review_created_date)} review date created in total')
                print(f'Extracted {len(review_texts)} review texts in total')
                    
                    
                # print('All positive reviews:', positive_review_text_array)
                # print('All negative reviews:', negative_review_text_array)
                # print('All review ratings:', review_rating)
                # print('All review names:', reviewer_names)
                # print('All review dates:', review_checkin_dates)
                # print('All reviewer countries:', reviewer_country)
                # print('All reviewer apartment types:', apartment_type)
                # print('All reviewer stay lengths:', num_nights_stay)
                # print('All reviewer group types:', group_type)
                # print('All reviewer feedbacks:', review_feedback)
                # print('All reviewer sentiments:', review_sentiment)
                # print('All reviewe created dates:', review_created_date)
                # print(f'Total positive reviews: {len(positive_review_text_array)}')
                # print(f'Total negative reviews: {len(negative_review_text_array)}')         


            await browser.close()

            # save data as csv
            # Create the DataFrame
            print('saving review data to csv')
            hotel_ids = [hotel_id] * len(review_rating)
            hotel_name = [hotel_name] * len(review_rating)
            source_name = [source_name] * len(review_rating)

            df_reviews = self.create_review_dataframe(hotel_ids,
                                                hotel_name,
                                                source_name,
                                                positive_review_text_array,
                                                negative_review_text_array,
                                                review_rating,
                                                reviewer_names,
                                                reviewer_country,
                                                review_sentiment,
                                                review_checkin_dates,
                                                review_created_date,
                                                apartment_type,
                                                num_nights_stay,
                                                group_type,
                                                review_feedback,
                                                review_texts)

            # saving reviews as csv
            self.save_to_csv(filename, df_reviews)
            return df_reviews

        except Exception as e:
            print(f"An error occurred: {e}")
            return {"error": str(e)}


if __name__ == '__main__':

    booking_com_scraper = BookingComScraper()

    # pulling user reviews from booking.com for Kwantu Guesthouse 1
    review_df = asyncio.run(booking_com_scraper.scrape_hotel_reviews("https://www.booking.com/hotel/za/kwantu-guesthouses-cape-town.html?aid=304142&label=gen173nr-1FCAEoggI46AdIM1gEaPsBiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKwgd68BsACAdICJDg3NWYxYmY0LTBjNDktNGRiYy04Y2Q1LWUxOTAxZTY0MjgxONgCBeACAQ&sid=989dc5e594027c7ff3b4d7505cacb436&dest_id=-1217214&dest_type=city&dist=0&group_adults=2&group_children=0&hapos=1&hpos=1&no_rooms=1&req_adults=2&req_children=0&room1=A%2CA&sb_price_type=total&sr_order=popularity&srepoch=1737982144&srpvid=15855a1d9db00417&type=total&ucfs=1&",
                                                hotel_id='KWA123',
                                                hotel_name='Kwantu Guesthouse 1',
                                                source_name='booking.com',
                                                filename='output/kwantu_1.csv'))

    # pulling user reviews from booking.com for Kwantu Guesthouse 2
    # review_df = asyncio.run(scrape_hotel_reviews("https://www.booking.com/hotel/za/kwantu-guesthouse-2.html?aid=304142&label=gen173nr-1FCAEoggI46AdIM1gEaPsBiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKwgd68BsACAdICJDg3NWYxYmY0LTBjNDktNGRiYy04Y2Q1LWUxOTAxZTY0MjgxONgCBeACAQ&sid=989dc5e594027c7ff3b4d7505cacb436&checkin=2025-03-01&checkout=2025-03-15&dest_id=2911295&dest_type=hotel&dist=0&group_adults=2&group_children=0&hapos=1&hpos=1&no_rooms=1&req_adults=2&req_children=0&room1=A%2CA&sb_price_type=total&soh=1&sr_order=popularity&srepoch=1740002807&srpvid=ed219b784fca023f&type=total&ucfs=1&#no_availability_msg",
    #                                              hotel_id='e1ada55f-000c-4991-9527-f72362cb6e80',
    #                                              source_id='25e89862-0a2c-4d53-900a-6cb3300c4268',
    #                                              hotel_name='Kwantu Guesthouse 2',
    #                                              source_name='booking.com',
    #                                              filename='output/kwantu_2.csv'))

    # pulling user reviews from booking.com for The Bantry Aparthotel by Totalstay
    # review_df = asyncio.run(scrape_hotel_reviews("https://www.booking.com/hotel/za/bantry-bay-suite-hotel-cape-town.html?aid=304142&label=gen173nr-1FCAEoggI46AdIM1gEaPsBiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuALBgf28BsACAdICJDlhNDU1ZjQ1LWRiNmMtNGM0OC1iMDgxLWViNWY1NDZiYjYwNdgCBeACAQ&sid=989dc5e594027c7ff3b4d7505cacb436&dest_id=-1217214&dest_type=city&dist=0&group_adults=2&group_children=0&hapos=4&hpos=4&no_rooms=1&req_adults=2&req_children=0&room1=A%2CA&sb_price_type=total&sr_order=popularity&srepoch=1738499906&srpvid=b15f58da98a20577&type=total&ucfs=1&#tab-main",
    #             hotel_id = 'b8e318bb-dade-45e5-b87a-b8359f077a2d',
    #             source_id= '25e89862-0a2c-4d53-900a-6cb3300c4268',
    #             hotel_name = 'Bantry Aparthhotel by Totalstay',
    #             source_name = 'booking.com',
    #             filename='output/bantry_aparthotel.csv'
    #             ))
