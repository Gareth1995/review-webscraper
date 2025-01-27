from playwright.async_api import async_playwright
import asyncio

async def scrape_hotel_reviews(url_link, website):

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            context = await browser.new_context()
            page = await context.new_page()

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

                reviews_html = []
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
                        div_text = await reviews.nth(j).text_content()
                        if div_text:
                            reviews_html.append(div_text.strip())

                print('Total number of reviews:', len(reviews_html))    
                print(reviews_html)


                # new_tab_promise = page.context.on("page")  # Wait for the new tab to open
                # await first_property_card.click()
                # new_tab = await new_tab_promise

                # # Navigate and scrape reviews
                # await new_tab.locator('[data-testid="fr-read-all-reviews"]').wait_for(state="visible")
                # await new_tab.wait_for_timeout(5000)
                # await new_tab.locator('[data-testid="fr-read-all-reviews"]').click()

                # # Find the last review page number
                # last_review_page_num = await new_tab.locator('div.ab95b25344 > ol > li:last-child').text_content()
                # last_review_page_num = int(last_review_page_num.strip())

                # reviews_html = []

                # for i in range(1, last_review_page_num + 1):
                #     print(f"Navigating to page {i}")
                #     await new_tab.locator(f'button[aria-label=" {i}"]').click()
                #     print("Navigation successful")

                #     await new_tab.wait_for_timeout(3000)

                #     reviews = new_tab.locator('div.c402354066')
                #     count = await reviews.count()
                #     print(f"Found {count} review cards.")

                #     for j in range(count):
                #         div_text = await reviews.nth(j).text_content()
                #         if div_text:
                #             reviews_html.append(div_text.strip())

                #     print("Reviews pushed to array")

                # print("Reviews:", reviews_html)

            await browser.close()
    except Exception as e:
        print(f"An error occurred: {e}")
        return {"error": str(e)}

asyncio.run(scrape_hotel_reviews("https://www.booking.com/hotel/za/kwantu-guesthouses-cape-town.html?aid=304142&label=gen173nr-1FCAEoggI46AdIM1gEaPsBiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKwgd68BsACAdICJDg3NWYxYmY0LTBjNDktNGRiYy04Y2Q1LWUxOTAxZTY0MjgxONgCBeACAQ&sid=989dc5e594027c7ff3b4d7505cacb436&dest_id=-1217214&dest_type=city&dist=0&group_adults=2&group_children=0&hapos=1&hpos=1&no_rooms=1&req_adults=2&req_children=0&room1=A%2CA&sb_price_type=total&sr_order=popularity&srepoch=1737982144&srpvid=15855a1d9db00417&type=total&ucfs=1&", "booking"))
