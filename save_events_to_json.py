import json
import requests
from wydarzenia_scraper import parse_events, URL

if __name__ == "__main__":
    html = requests.get(URL).text
    events = parse_events(html)
    with open("events.json", "w", encoding="utf-8") as f:
        json.dump(events, f, ensure_ascii=False, indent=2)