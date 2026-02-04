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
csv_file = "keyword_1.csv"
video_csv_file = "download_urls.csv"
ROOT_DOWNLOAD_DIR = 'downloads_by_keywords'

fieldnames = ["class", "post_id", "title", "content", "create_at", "user_id", "liked_count",
              "cover_url", "post_url", "image_urls", "video_url"]
video_fieldnames = ["post_id", "cover_url", "image_urls", "video_url"]

# 搜索关键词字典
categories = {
    #"厌恶 / 恐惧损失": ["仅限今天"],
    "社会比较": ["甩开同龄人","嫉妒","测评","完胜"]
}

count_per_keyword = 10  # 每个关键词获取的有效数量
min_likes = 0       # 点赞筛选阈值
scraped_ids = set()     # 避重

# 休息与下载触发阈值
REST_THRESHOLD = 50  
SMALL_REST_MIN = 1

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
    if isinstance(url, list): return url
    return url.strip("'\"[]\\r\\n\\t ")

def download_file(url, save_path):
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
    print("\n--- 启动定时下载任务 ---")
    rows_to_keep = []
    if not os.path.exists(csv_file): return
    
    with open(csv_file, mode='r', encoding='utf-8-sig') as f:
        reader = list(csv.DictReader(f))
        if not reader: return
        fieldnames_list = reader[0].keys()

    for row in reader:
        post_id = row['post_id']
        raw_imgs = row['image_urls']
        img_urls = [clean_url(u) for u in re.findall(r"'(https?://[^']+)'", raw_imgs)] if isinstance(raw_imgs, str) else raw_imgs
        
        post_folder = os.path.join(ROOT_DOWNLOAD_DIR, str(post_id))
        os.makedirs(post_folder, exist_ok=True)

        success = True
        if row['cover_url']:
            if not download_file(row['cover_url'], os.path.join(post_folder, "cover.jpg")): success = False
        for i, img_url in enumerate(img_urls):
            if not download_file(img_url, os.path.join(post_folder, f"image_{i+1}.jpg")): success = False
        if row['video_url'] and success:
            if not download_file(row['video_url'], os.path.join(post_folder, "video.mp4")): success = False

        if success: rows_to_keep.append(row)

    with open(csv_file, mode='w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames_list)
        writer.writeheader()
        writer.writerows(rows_to_keep)
    print("--- 下载任务结束 ---\n")

# --- 3. 主程序逻辑 ---
initialize_csv()
page = ChromiumPage()
ac = Actions(page)
total_scraped_global = 0

try:
    for category_name, keywords in categories.items():
        for keyword in keywords:
            print(f"\n{'='*50}\n开始搜索关键词: {keyword} (属于: {category_name})\n{'='*50}")
            
            # 访问首页并搜索
            page.get('https://www.xiaohongshu.com/')
            time.sleep(2)
            search_input = page.ele('#search-input')
            search_input.clear()
            search_input.input(keyword)
            page.ele('.search-icon').click()
            
            page.listen.start('https://edith.xiaohongshu.com/api/sns/web/v1/feed')
            
            keyword_count = 0
            scroll_fail_count = 0

            while keyword_count < count_per_keyword:
                # 获取当前视野内的帖子
                items = page.eles('xpath://section[contains(@class, "note-item")]')
                found_any_in_this_batch = False

                for item in items:
                    if keyword_count >= count_per_keyword: break
                    
                    # 排除广告或非帖子内容
                    if '大家都在搜' in item.text or '相关搜索' in item.text: continue
                    
                    try:
                        # 1. 列表页初步过滤：检查点赞
                        like_ele = item.ele('.count', timeout=0.5)
                        if not like_ele: continue
                        likes = convert_liked_count(like_ele.text)
                        if likes < min_likes: continue 

                        # 2. 检查 ID 是否已抓取
                        # 尝试从封面图链接或元素特征获取 ID 的简化处理，这里点击后再判断最准
                        page.scroll.to_see(item)
                        item.click()
                        time.sleep(random.uniform(2, 3))

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
                                            "class": category_name, "post_id": post_id, "title": title, "content": content,
                                            "create_at": create_at, "user_id": user_id, "liked_count": likes,
                                            "cover_url": cover_url, "post_url": page.url, "image_urls": image_urls, "video_url": video_url
                                        }
                                        
                                        with open(csv_file, mode='a', newline='', encoding='utf-8-sig') as f:
                                            csv.DictWriter(f, fieldnames=fieldnames).writerow(row)
                                        
                                        if video_url:
                                            with open(video_csv_file, mode='a', newline='', encoding='utf-8-sig') as f:
                                                csv.DictWriter(f, fieldnames=video_fieldnames).writerow({k: row[k] for k in video_fieldnames})
                                        
                                        scraped_ids.add(post_id)
                                        keyword_count += 1
                                        total_scraped_global += 1
                                        found_any_in_this_batch = True
                                        scroll_fail_count = 0
                                        print(f"[{keyword}] 成功抓取 ({likes}赞): {title[:12]}...")

                                        # --- 触发下载任务 ---
                                        if total_scraped_global % REST_THRESHOLD == 0:
                                            start_download_task()
                                            print(f"触发大休，休息 {SMALL_REST_MIN} 分钟...")
                                            time.sleep(SMALL_REST_MIN * 60)

                        # 关闭详情页
                        ac.key_down(Keys.ESCAPE).key_up(Keys.ESCAPE)
                        time.sleep(1)
                    except Exception:
                        ac.key_down(Keys.ESCAPE).key_up(Keys.ESCAPE)
                        continue

                # 滚动逻辑
                if not found_any_in_this_batch:
                    scroll_fail_count += 1
                    if scroll_fail_count >= 3:
                        print("此页面加载缓慢，尝试刷新...")
                        page.refresh()
                        time.sleep(5)
                        scroll_fail_count = 0
                    else:
                        page.scroll.to_bottom()
                        time.sleep(3)
                else:
                    page.scroll.to_bottom()
                    time.sleep(2)

except KeyboardInterrupt:
    print("\n用户手动停止")
finally:
    start_download_task()
    page.quit()
    print("全部搜索任务结束。")