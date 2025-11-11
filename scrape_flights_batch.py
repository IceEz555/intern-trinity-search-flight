"""
Batch Scraping - แบ่ง scrape ทีละน้อยเพื่อหลบ detection
รัน script นี้แทน scrape_flights_multi_dates.py
"""

import time
from datetime import date, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
import json
import random
import os

def scrape_single_date(origin, destination, departure_date, return_date):
    """Scrape หนึ่งวันด้วย session ใหม่"""
    
    options = webdriver.ChromeOptions()
    # ไม่ใช้ headless
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    driver = webdriver.Chrome(
        service=ChromeService(ChromeDriverManager().install()),
        options=options
    )
    
    # Stealth mode
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": """
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """
    })
    
    dep_str = departure_date.strftime("%Y-%m-%d")
    ret_str = return_date.strftime("%Y-%m-%d")
    url = f"https://www.google.com/travel/flights?q=flights%20from%20{origin}%20to%20{destination}%20on%20{dep_str}%20returning%20{ret_str}"
    
    print(f"  📅 {dep_str} → {ret_str}")
    
    try:
        driver.get(url)
        time.sleep(5)
        
        # เช็ค CAPTCHA
        if "sorry" in driver.current_url or "recaptcha" in driver.page_source.lower():
            print(f"    ⚠️  CAPTCHA! กรุณาแก้ภายใน 90 วินาที...")
            input("    Press Enter after solving CAPTCHA...")
            driver.get(url)
            time.sleep(3)
        
        wait = WebDriverWait(driver, 30)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div.yR1fYc')))
        
        # Scroll
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(2)
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        flights_data = []
        flight_elements = soup.select('div.yR1fYc')
        
        for flight in flight_elements:
            airline = flight.select_one('div.sSHqwe')
            departure_time = flight.select_one('span[aria-label*="Departure time:"]')
            arrival_time = flight.select_one('span[aria-label*="Arrival time:"]')
            duration = flight.select_one('div.gvkrdb')
            stops = flight.select_one('div.EfT7Ae span')
            price_element = flight.select_one('div.U3gSDe span[aria-label*="baht"]')
            
            price_text = price_element.get_text(strip=True) if price_element else "N/A"
            try:
                price = float(price_text.replace("THB", "").replace(",", "").strip())
            except:
                price = None
            
            flights_data.append({
                "airline": airline.get_text(strip=True).replace("\n", " ") if airline else "N/A",
                "departure_time": departure_time.get_text(strip=True) if departure_time else "N/A",
                "arrival_time": arrival_time.get_text(strip=True) if arrival_time else "N/A",
                "duration": duration.get_text(strip=True) if duration else "N/A",
                "stops": stops.get_text(strip=True) if stops else "N/A",
                "price": price,
                "price_text": price_text,
                "departure_date": dep_str,
                "return_date": ret_str,
                "origin": origin,
                "destination": destination
            })
        
        print(f"    ✅ {len(flights_data)} flights")
        return flights_data
        
    except Exception as e:
        print(f"    ❌ Error: {str(e)[:100]}")
        return []
    
    finally:
        driver.quit()


def scrape_batch(origin, destination, duration, start_month_offset, num_months):
    """
    Scrape ทีละ batch
    
    Args:
        start_month_offset: เริ่มจากเดือนที่เท่าไหร่ (0 = เดือนหน้า, 3 = 4 เดือนถัดไป)
        num_months: จำนวนเดือนที่จะ scrape ใน batch นี้
    """
    all_flights = []
    
    today = date.today()
    start_date = date(today.year, today.month, 15)
    
    print(f"\n🚀 Batch Start: Scraping months {start_month_offset} to {start_month_offset + num_months - 1}")
    print(f"   Route: {origin} → {destination} ({duration} days)\n")
    
    for i in range(start_month_offset, start_month_offset + num_months):
        if start_date.month + i > 12:
            year = start_date.year + 1
            month = (start_date.month + i) % 12
            if month == 0:
                month = 12
        else:
            year = start_date.year
            month = start_date.month + i
        
        departure_date = date(year, month, 15)
        return_date = departure_date + timedelta(days=duration)
        
        # Scrape this date
        flights = scrape_single_date(origin, destination, departure_date, return_date)
        all_flights.extend(flights)
        
        # รอสุ่ม 10-20 วินาที
        wait_time = random.randint(10, 20)
        print(f"    ⏰ Waiting {wait_time}s...\n")
        time.sleep(wait_time)
    
    return all_flights


def main():
    """
    Main function - แบ่ง scrape เป็น 3 batches
    """
    ORIGIN = "BKK"
    DESTINATION = "NRT"
    DURATION = 7
    
    # สร้างชื่อไฟล์
    base_filename = f"flights_{ORIGIN}_{DESTINATION}_{DURATION}days"
    
    print("="*70)
    print("🎯 BATCH SCRAPING STRATEGY")
    print("="*70)
    print("เราจะแบ่ง scrape 12 เดือนเป็น 3 รอบ:")
    print("  Batch 1: เดือนที่ 0-3 (4 เดือน)")
    print("  Batch 2: เดือนที่ 4-7 (4 เดือน)")
    print("  Batch 3: เดือนที่ 8-11 (4 เดือน)")
    print("\nคุณจะต้อง:")
    print("  1. รัน batch 1 → รอผลเสร็จ → ปิด browser")
    print("  2. รอ 2-5 นาที")
    print("  3. รัน batch 2 → รอผลเสร็จ → ปิด browser")
    print("  4. รอ 2-5 นาที")
    print("  5. รัน batch 3")
    print("="*70)
    
    # ถามว่าจะรัน batch ไหน
    batch_choice = input("\nเลือก batch ที่จะรัน (1/2/3) หรือ 'all' สำหรับทั้งหมด: ").strip()
    
    all_flights = []
    
    if batch_choice == '1':
        flights = scrape_batch(ORIGIN, DESTINATION, DURATION, 0, 4)
        all_flights.extend(flights)
        filename = f"{base_filename}_batch1.csv"
        
    elif batch_choice == '2':
        flights = scrape_batch(ORIGIN, DESTINATION, DURATION, 4, 4)
        all_flights.extend(flights)
        filename = f"{base_filename}_batch2.csv"
        
    elif batch_choice == '3':
        flights = scrape_batch(ORIGIN, DESTINATION, DURATION, 8, 4)
        all_flights.extend(flights)
        filename = f"{base_filename}_batch3.csv"
        
    elif batch_choice.lower() == 'all':
        print("\n⚠️  Warning: รัน all อาจโดน block!")
        confirm = input("แน่ใจหรือ? (yes/no): ")
        if confirm.lower() == 'yes':
            for batch_num, start_offset in enumerate([(0, 4), (4, 4), (8, 4)], 1):
                print(f"\n{'='*70}")
                print(f"🔄 Starting Batch {batch_num}")
                print(f"{'='*70}")
                
                flights = scrape_batch(ORIGIN, DESTINATION, DURATION, start_offset[0], start_offset[1])
                all_flights.extend(flights)
                
                if batch_num < 3:
                    wait_mins = random.randint(3, 5)
                    print(f"\n⏸️  Cooling down for {wait_mins} minutes before next batch...")
                    time.sleep(wait_mins * 60)
            
            filename = f"{base_filename}_complete.csv"
        else:
            print("❌ Cancelled")
            return
    else:
        print("❌ Invalid choice")
        return
    
    # บันทึกผล
    if all_flights:
        df = pd.DataFrame(all_flights)
        df.to_csv(filename, index=False, encoding='utf-8')
        print(f"\n✅ Saved {len(all_flights)} flights to '{filename}'")
        
        # แสดง summary
        print("\n📊 SUMMARY")
        print(f"Total flights: {len(all_flights)}")
        print(f"Date range: {df['departure_date'].min()} to {df['departure_date'].max()}")
        if 'price' in df.columns:
            print(f"Price range: ฿{df['price'].min():,.0f} - ฿{df['price'].max():,.0f}")
            print(f"Average price: ฿{df['price'].mean():,.0f}")
    else:
        print("\n❌ No flights scraped")
    
    # ถ้ารันครบทั้ง 3 batches แล้ว → merge files
    if batch_choice.lower() == 'all' or os.path.exists(f"{base_filename}_batch3.csv"):
        print("\n🔄 Checking for batch files to merge...")
        
        batch_files = [
            f"{base_filename}_batch1.csv",
            f"{base_filename}_batch2.csv",
            f"{base_filename}_batch3.csv"
        ]
        
        existing_files = [f for f in batch_files if os.path.exists(f)]
        
        if len(existing_files) == 3:
            print("✅ All batch files found! Merging...")
            
            dfs = [pd.read_csv(f) for f in existing_files]
            merged_df = pd.concat(dfs, ignore_index=True)
            
            merged_filename = f"{base_filename}_merged.csv"
            merged_df.to_csv(merged_filename, index=False, encoding='utf-8')
            
            print(f"✅ Merged {len(merged_df)} flights into '{merged_filename}'")
            
            # ลบไฟล์ batch
            for f in existing_files:
                os.remove(f)
                print(f"   🗑️  Deleted {f}")


if __name__ == "__main__":
    main()