import time
import csv
import os
import json
import re
import random
import requests
from DrissionPage import ChromiumPage
from DrissionPage.common import Actions
from DrissionPage.common import Keys

# --- 1. 配置参数 ---
csv_file = "test3.csv"
video_csv_file = "download_urls.csv"
ROOT_DOWNLOAD_DIR = 'downloads_0122'  # 保持原文件夹命名习惯

fieldnames = ["class", "post_id", "title", "content", "create_at", "user_id", "liked_count",
              "cover_url", "post_url", "image_urls", "video_url"]
video_fieldnames = ["post_id", "cover_url", "image_urls", "video_url"]

target_classes = [
    'travel_v3'
]

count_per_class = 210
min_likes = 1000
scraped_ids = set()

# 休息与下载触发阈值
REST_THRESHOLD = 50  
SMALL_REST_MIN = 1
CLASS_REST_MIN = 2

DOWNLOAD_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)",
    "Referer": "https://www.xiaohongshu.com/"
}

# --- 2. 基础功能函数 ---

def initialize_csv():
    if not os.path.exists(ROOT_DOWNLOAD_DIR):
        os.makedirs(ROOT_DOWNLOAD_DIR)
    if not os.path.exists(csv_file):
        with open(csv_file, mode='a', newline='', encoding='utf-8-sig') as file:
            csv.DictWriter(file, fieldnames=fieldnames).writeheader()
    if not os.path.exists(video_csv_file):
        with open(video_csv_file, mode='a', newline='', encoding='utf-8-sig') as file:
            csv.DictWriter(file, fieldnames=video_fieldnames).writeheader()

def convert_liked_count(liked_count_str):
    if not liked_count_str or liked_count_str == '赞': return 0
    s = liked_count_str.replace('+', '').replace(' ', '').strip()
    try:
        if '万' in s:
            return int(float(s.replace('万', '')) * 10000)
        return int(s)
    except:
        return 0

def clean_url(url):
    """去除URL中的非法字符"""
    if isinstance(url, list): return url
    return url.strip("'\"[]\\r\\n\\t ")

def download_file(url, save_path):
    """单文件下载逻辑"""
    if not url or os.path.exists(save_path):
        return True
    try:
        response = requests.get(url, headers=DOWNLOAD_HEADERS, stream=True, timeout=10)
        response.raise_for_status()
        with open(save_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        return True
    except Exception as e:
        print(f"下载出错: {url[:30]}... 错误: {e}")
        return False

def start_download_task():
    """触发下载逻辑：读取当前的csv_file并下载"""
    print("\n--- 启动定时下载任务 ---")
    rows_to_keep = []
    
    with open(csv_file, mode='r', encoding='utf-8-sig') as f:
        reader = list(csv.DictReader(f))
        if not reader: return
        fieldnames_list = reader[0].keys()

    for row in reader:
        post_id = row['post_id']
        # 处理图片URL（可能是字符串形式的列表）
        raw_imgs = row['image_urls']
        if isinstance(raw_imgs, str):
            # 兼容处理：如果是 "['url1', 'url2']" 这种格式
            img_urls = [clean_url(u) for u in re.findall(r"'(https?://[^']+)'", raw_imgs)]
            if not img_urls: # 如果正则没匹配到，尝试普通逗号分割
                img_urls = [clean_url(u) for u in raw_imgs.split(',') if clean_url(u)]
        else:
            img_urls = raw_imgs

        post_folder = os.path.join(ROOT_DOWNLOAD_DIR, str(post_id))
        os.makedirs(post_folder, exist_ok=True)

        success = True
        # 下载封面
        if row['cover_url']:
            if not download_file(row['cover_url'], os.path.join(post_folder, "cover.jpg")):
                success = False
        # 下载图片
        for i, img_url in enumerate(img_urls):
            if not download_file(img_url, os.path.join(post_folder, f"image_{i+1}.jpg")):
                success = False
        # 下载视频
        if row['video_url'] and success:
            if not download_file(row['video_url'], os.path.join(post_folder, "video.mp4")):
                success = False

        if success:
            rows_to_keep.append(row)
        else:
            # 下载失败的行会被过滤掉，实现自动删除
            print(f">>> 资源失效或下载失败，从CSV中移除 ID: {post_id}")

    # 将成功的写回CSV，防止下次任务重复下载失败链接
    with open(csv_file, mode='w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames_list)
        writer.writeheader()
        writer.writerows(rows_to_keep)
    print("--- 下载任务结束 ---\n")

# --- 3. 主程序逻辑 ---
initialize_csv()
page = ChromiumPage()
ac = Actions(page)

try:
    for current_class in target_classes:
        print(f"\n{'='*50}\n开始分类: {current_class}\n{'='*50}")
        
        if current_class != target_classes[0]:
            time.sleep(10) # 切换分类时小休
        page.get(f'https://www.xiaohongshu.com/explore?channel_id=homefeed.{current_class}')
        page.listen.start('https://edith.xiaohongshu.com/api/sns/web/v1/feed')
        
        class_count = 0
        scroll_fail_count = 0

        while class_count < count_per_class:
            items = page.eles('xpath://section[contains(@class, "note-item")]')
            found_any_in_this_batch = False

            for item in items:
                if class_count >= count_per_class: break
                try:
                    like_ele = item.ele('.count', timeout=1)
                    if not like_ele: continue
                    likes = convert_liked_count(like_ele.text)
                    if likes < min_likes: continue 

                    page.scroll.to_see(item)
                    time.sleep(1)
                    item.click()
                    time.sleep(random.uniform(2, 4))

                    res = page.listen.wait(timeout=5)
                    if res:
                        raw = res.response.body
                        if 'data' in raw and raw['data']['items']:
                            info = raw['data']['items'][0]['note_card']
                            post_id = info.get('note_id', '')

                            if post_id and post_id not in scraped_ids:
                                title = info.get('title', '')
                                if title:
                                    content = re.sub(r'\s+', ' ', info.get('desc', '')).strip()
                                    create_at = info.get('time', '')
                                    user_id = info.get('user', {}).get('user_id', '')
                                    image_list = info.get('image_list', [])
                                    cover_url = image_list[0].get('url_default', '') if image_list else ''
                                    image_urls = [i.get('url_default', '') for i in image_list]
                                    
                                    video_url = ''
                                    if info.get('type') == 'video':
                                        stream = info.get('video', {}).get('media', {}).get('stream', {})
                                        v_s = stream.get('h264') or stream.get('h265')
                                        if v_s: video_url = v_s[0].get('master_url') or v_s[0].get('backup_urls', [''])[0]

                                    row = {
                                        "class": current_class, "post_id": post_id, "title": title, "content": content,
                                        "create_at": create_at, "user_id": user_id, "liked_count": likes,
                                        "cover_url": cover_url, "post_url": page.url, "image_urls": image_urls, "video_url": video_url
                                    }
                                    
                                    with open(csv_file, mode='a', newline='', encoding='utf-8-sig') as f:
                                        csv.DictWriter(f, fieldnames=fieldnames).writerow(row)
                                    
                                    if video_url:
                                        with open(video_csv_file, mode='a', newline='', encoding='utf-8-sig') as f:
                                            csv.DictWriter(f, fieldnames=video_fieldnames).writerow({k: row[k] for k in video_fieldnames})
                                    
                                    scraped_ids.add(post_id)
                                    class_count += 1
                                    found_any_in_this_batch = True
                                    scroll_fail_count = 0
                                    print(f"[{current_class}] 抓取第 {class_count} 条: {title[:10]}...")

                                    # --- 触发下载与小休 ---
                                    if class_count % REST_THRESHOLD == 0:
                                        # 立即下载已抓取的内容
                                        start_download_task()
                                        print(f"休息 {SMALL_REST_MIN} 分钟...")
                                        time.sleep(SMALL_REST_MIN * 60)

                    ac.key_down(Keys.ESCAPE).key_up(Keys.ESCAPE)
                    time.sleep(1.5)
                except Exception:
                    ac.key_down(Keys.ESCAPE).key_up(Keys.ESCAPE)
                    continue

            if not found_any_in_this_batch:
                scroll_fail_count += 1
                if scroll_fail_count >= 2:
                    page.refresh()
                    time.sleep(5)
                    scroll_fail_count = 0
                else:
                    page.scroll.to_bottom()
                    time.sleep(4)
            else:
                page.scroll.to_bottom()
                time.sleep(2)

except KeyboardInterrupt:
    print("\n用户手动停止")
finally:
    # 任务结束前最后执行一次下载，确保末尾不满50条的数据也被处理
    start_download_task()
    page.quit()
    print("全部任务结束。")