# import requests
    # from bs4 import BeautifulSoup
    # from datetime import datetime
    # import locale
    #
    # try:
    #     locale.setlocale(locale.LC_TIME, "pl_PL.UTF-8")
    # except locale.Error:
    #     pass
    #
    # URL = "https://uz.zgora.pl/kalendarz"
    #
    # MONTHS = {
    #     "Styczeń": "01", "Luty": "02", "Marzec": "03", "Kwiecień": "04", "Maj": "05", "Czerwiec": "06",
    #     "Lipiec": "07", "Sierpień": "08", "Wrzesień": "09", "Październik": "10", "Listopad": "11", "Grudzień": "12"
    # }
    #
    # def parse_events(html):
    #     soup = BeautifulSoup(html, "html.parser")
    #     events = []
    #     for item in soup.select(".calendar-list-item"):
    #         day_tag = item.select_one(".date-cont .day")
    #         month_tag = item.select_one(".date-cont .month")
    #         if not day_tag or not month_tag:
    #             continue
    #         day = day_tag.text.strip()
    #         month_name = month_tag.text.strip()
    #         month_num = MONTHS.get(month_name, "01")
    #         year = datetime.now().year
    #
    #         title_link = item.select_one(".h4 a.title")
    #         if not title_link:
    #             continue
    #         title = title_link.text.strip()
    #         link = "https://uz.zgora.pl" + title_link["href"]
    #
    #         cat_label = item.find("b", string="Kategoria:")
    #         categories = []
    #         if cat_label:
    #             cats_text = cat_label.next_sibling
    #             if cats_text:
    #                 categories = [c.strip() for c in cats_text.split(",") if c.strip()]
    #
    #         if not any("studenci" in c.lower() for c in categories):
    #             continue
    #
    #         date_label = item.find("b", string="Data:")
    #         date_iso = None
    #         weekday = None
    #         if date_label and date_label.next_sibling:
    #             date_str = date_label.next_sibling.strip()
    #             if "-" in date_str:
    #                 date_str = date_str.split("-")[-1].strip()
    #             date_parts = date_str.split(".")
    #             if len(date_parts) == 3:
    #                 day, month_num, year = date_parts
    #             try:
    #                 date_obj = datetime(int(year), int(month_num), int(day))
    #                 date_iso = date_obj.strftime("%Y-%m-%d")
    #                 weekday = date_obj.strftime("%A")
    #             except Exception:
    #                 continue
    #
    #         time_label = item.find("b", string="Godzina:")
    #         time_info = time_label.next_sibling.strip() if time_label and time_label.next_sibling else ""
    #         time_info = time_info.replace("-", "–").replace("  ", " ")
    #         time_info = time_info.strip("–: ")
    #
    #         loc_label = item.find("b", string="Miejsce wydarzenia:")
    #         location = loc_label.next_sibling.strip() if loc_label and loc_label.next_sibling else ""
    #
    #         org_label = item.find("b", string="Organizator:")
    #         organizer = org_label.next_sibling.strip() if org_label and org_label.next_sibling else ""
    #
    #         price_label = item.find("b", string="Cena wejścia:")
    #         price = price_label.next_sibling.strip() if price_label and price_label.next_sibling else ""
    #         more_info = price if price else ""
    #         if not more_info:
    #             desc_text = item.get_text()
    #             if "wstęp wolny" in desc_text.lower():
    #                 more_info = "wstęp wolny"
    #
    #         img_tag = item.select_one("img")
    #         image = None
    #         if img_tag and img_tag.get("src"):
    #             image = "https://uz.zgora.pl" + img_tag["src"]
    #
    #         if date_iso and date_iso < datetime.now().strftime("%Y-%m-%d"):
    #             continue
    #
    #         events.append({
    #             "title": title,
    #             "date": date_iso,
    #             "weekday": weekday,
    #             "time": time_info,
    #             "location": location,
    #             "organizer": organizer,
    #             "category": categories,
    #             "more_info": more_info,
    #             "link": link,
    #             "image": image
    #         })
    #     return events
    # if __name__ == "__main__":
    #     import requests
    #     html = requests.get(URL).text
    #     events = parse_events(html)
    #     if not events:
    #         print("Brak wydarzeń dla studentów.")
    #     for event in events:
    #         print(event)