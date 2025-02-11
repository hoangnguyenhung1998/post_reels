import requests
import pandas as pd
import threading
import time
import os
from datetime import datetime

# ✅ Thông tin API
GOOGLE_SHEET_ID = "15xn6RKYUTEzAFge2eLTazF9ES-2m43aKRjbeDSd5EU0"  # Thay bằng Google Sheet ID của bạn
API_KEY = "4f5ed0d36596f93e5822f93ebc396f8ba703713e"  # Thay bằng Google API Key của bạn
ACCESS_TOKEN = "EAAFOwkjZA7ZAIBO3Q9uvgpHdbZCFkp863fZBpM2SbZCIsNtEJ2svB68bS7eGjgEl9wJSIqNpX40ckRDyK3A0MH4pFcyziZC2TcHzoECZC3zj9bCVr62RvQGBjdnE3gT4WBqEcaqZC3GASQDrQfgteUkjLskeZB57DRPtSXdnSC5EgSHuXWngsZBeZCeOlPyeYAz4YPKM5UG47iqSWZCs1OAYhZCC2zdom"

def get_sheets_data():
    """ Lấy dữ liệu từ Google Sheets """
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{GOOGLE_SHEET_ID}/values/Sheet1?key={API_KEY}"
    response = requests.get(url)
    data = response.json()
    
    if "values" in data:
        return pd.DataFrame(data["values"][1:], columns=data["values"][0])
    return None

def upload_video(video_path, caption, page_id):
    """ Đăng video lên Facebook Reels """

    if not os.path.exists(video_path):
        print(f"❌ Không tìm thấy video: {video_path}")
        return
    
    file_size = os.path.getsize(video_path)

    # ✅ Bước 1: Lấy upload_url
    start_url = f"https://graph.facebook.com/v18.0/{page_id}/video_reels"
    start_params = {
        "upload_phase": "start",
        "access_token": ACCESS_TOKEN,
        "file_size": file_size
    }

    start_response = requests.post(start_url, data=start_params)
    start_data = start_response.json()

    if "video_id" not in start_data or "upload_url" not in start_data:
        print("❌ Lỗi khi lấy upload_url:", start_data)
        return

    video_id = start_data["video_id"]
    upload_url = start_data["upload_url"]

    # ✅ Bước 2: Upload video
    headers = {
        "Authorization": f"OAuth {ACCESS_TOKEN}",
        "offset": "0",
        "file_size": str(file_size),
    }

    with open(video_path, "rb") as video_file:
        upload_response = requests.post(upload_url, headers=headers, data=video_file)

    upload_data = upload_response.json()
    if upload_response.status_code != 200 or "success" not in upload_data:
        print("❌ Lỗi khi upload video:", upload_data)
        return

    # ✅ Bước 3: Hoàn tất quá trình upload
    finish_url = f"https://graph.facebook.com/v18.0/{page_id}/video_reels"
    finish_params = {
        "upload_phase": "finish",
        "access_token": ACCESS_TOKEN,
        "video_id": video_id,
        "description": caption,
        "published": "false",
    }

    finish_response = requests.post(finish_url, data=finish_params)
    finish_data = finish_response.json()

    if "id" in finish_data:
        print(f"✅ Đăng Reels thành công! Video ID: {finish_data['id']}")
    else:
        print("❌ Lỗi khi hoàn tất upload:", finish_data)
        return

    # ✅ Bước 4: Tự động đăng video từ bản nháp
    publish_url = f"https://graph.facebook.com/v18.0/{video_id}"
    publish_params = {
        "published": "true",
        "access_token": ACCESS_TOKEN
    }

    publish_response = requests.post(publish_url, data=publish_params)
    publish_data = publish_response.json()

    if "success" in publish_data and publish_data["success"]:
        print("✅ Reels đã được đăng từ bản nháp!")

def schedule_video(video_path, caption, page_id, post_time):
    """ Chờ đến đúng giờ rồi đăng video """
    post_datetime = datetime.strptime(post_time, "%Y-%m-%d %H:%M:%S")
    now = datetime.now()
    wait_time = (post_datetime - now).total_seconds()

    if wait_time > 0:
        print(f"⏳ Chờ {int(wait_time)} giây để đăng {video_path}")
        time.sleep(wait_time)

    upload_video(video_path, caption, page_id)

def schedule_all_videos():
    """ Lấy danh sách video & tạo thread để chờ đúng giờ đăng """
    df = get_sheets_data()
    if df is None:
        print("❌ Lỗi khi lấy dữ liệu Google Sheets")
        return

    for _, row in df.iterrows():
        video_path = row["Video Path"]
        caption = row["Caption"]
        page_id = row["Page ID"]
        post_time = row["Thời Gian Đăng"]  # Định dạng: YYYY-MM-DD HH:MM:SS

        threading.Thread(target=schedule_video, args=(video_path, caption, page_id, post_time)).start()

# 🚀 Chạy chương trình
schedule_all_videos()