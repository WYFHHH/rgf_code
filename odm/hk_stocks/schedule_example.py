import schedule
import time
from odm.hk_stocks.Loop_everyday import *

def job():
    print("I'm working...")
    refresh_daily()
    refresh_not_daily()
    print("Today's refresh completes.")

schedule.every().day.at("19:00").do(job)

while True:
    schedule.run_pending()
    time.sleep(0.5)