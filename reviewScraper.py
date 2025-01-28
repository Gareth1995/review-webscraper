from playwright.async_api import async_playwright
import asyncio
from bs4 import BeautifulSoup

async def scrape_hotel_reviews(url_link, website):

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            context = await browser.new_context()
            page = await context.new_page()

            reviews_html = []
            review_text = []
            reviewer_name = []
            reviewer_country = []
            review_rating = []
            review_sentiment = []
            timestamp = []


            if website == "agoda":
                await page.goto("https://www.agoda.com/")
                # Agoda scraping logic here

            elif website == "booking":
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
                # print(reviews_html[0])

            # Parse the HTML with BeautifulSoup
            for review in reviews_html:
                soup = BeautifulSoup(review, 'html.parser')

                # Get the nationality of each reviewer
                # Find all div elements with the specified class
                divs = soup.find_all('div', class_="abf093bdfe f45d8e4c32")

                # Extract the alt text from the img tag within each div
                for div in divs:
                    img_tag = div.find('img')  # Find the img tag inside the div
                    if img_tag and 'alt' in img_tag.attrs:  # Ensure the img tag and 'alt' attribute exist
                        alt_text = img_tag['alt']
                        reviewer_country.append(alt_text)
                    else:
                        print('no alt tag found')

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
                        review_text.append(combined_review)


            # print('Number of countries:', len(reviewer_country))
            # print(reviewer_country)

            print('Number of reviews:', len(reviewer_country))
            print(review_text)

            await browser.close()
    except Exception as e:
        print(f"An error occurred: {e}")
        return {"error": str(e)}

asyncio.run(scrape_hotel_reviews("https://www.booking.com/hotel/za/kwantu-guesthouses-cape-town.html?aid=304142&label=gen173nr-1FCAEoggI46AdIM1gEaPsBiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKwgd68BsACAdICJDg3NWYxYmY0LTBjNDktNGRiYy04Y2Q1LWUxOTAxZTY0MjgxONgCBeACAQ&sid=989dc5e594027c7ff3b4d7505cacb436&dest_id=-1217214&dest_type=city&dist=0&group_adults=2&group_children=0&hapos=1&hpos=1&no_rooms=1&req_adults=2&req_children=0&room1=A%2CA&sb_price_type=total&sr_order=popularity&srepoch=1737982144&srpvid=15855a1d9db00417&type=total&ucfs=1&", "booking"))
