import csv
import os
import re
import time
import requests
from DrissionPage import ChromiumPage

# ===================== 1. 配置参数 =====================
csv_file = "manual_scraped_data.csv"
ROOT_DOWNLOAD_DIR = 'manual_downloads'  # 资源保存根目录

fieldnames = ["post_id", "title", "content", "create_at", "user_id", "liked_count",
              "cover_url", "post_url", "image_urls", "video_url"]

DOWNLOAD_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)",
    "Referer": "https://www.xiaohongshu.com/"
}

# ===================== 2. 功能工具函数 =====================

def initialize_csv():
    """初始化CSV文件和下载目录"""
    if not os.path.exists(ROOT_DOWNLOAD_DIR):
        os.makedirs(ROOT_DOWNLOAD_DIR)
    if not os.path.exists(csv_file):
        with open(csv_file, mode='a', newline='', encoding='utf-8-sig') as file:
            csv.DictWriter(file, fieldnames=fieldnames).writeheader()

def download_file(url, save_path):
    """通用的下载函数"""
    if not url or os.path.exists(save_path):
        return True
    try:
        response = requests.get(url, headers=DOWNLOAD_HEADERS, stream=True, timeout=15)
        response.raise_for_status()
        with open(save_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        return True
    except Exception as e:
        print(f"   ❌ 下载失败: {url[:50]}... 错误: {e}")
        return False

def save_assets(post_id, cover_url, image_urls, video_url):
    """为每个帖子创建文件夹并保存资源"""
    post_folder = os.path.join(ROOT_DOWNLOAD_DIR, str(post_id))
    os.makedirs(post_folder, exist_ok=True)

    # 1. 下载封面
    if cover_url:
        download_file(cover_url, os.path.join(post_folder, "cover.jpg"))
    
    # 2. 下载图集
    for i, img_url in enumerate(image_urls):
        download_file(img_url, os.path.join(post_folder, f"image_{i+1}.jpg"))
    
    # 3. 下载视频
    if video_url:
        download_file(video_url, os.path.join(post_folder, "video.mp4"))

# ===================== 3. 核心监听主程序 =====================

def run_manual_scraper():
    initialize_csv()
    
    # 初始化浏览器，会自动打开一个窗口
    page = ChromiumPage()
    
    print(f"\n{'='*60}")
    print("🚀 手动交互抓取模式已启动！")
    print("使用说明：")
    print("1. 请在弹出的浏览器中正常浏览小红书（建议先登录）。")
    print("2. 只要你【鼠标点击】进入任何一个帖子详情页，程序就会自动采集。")
    print("3. 图片和视频将自动下载到 'manual_downloads' 文件夹。")
    print("4. 控制台会实时显示抓取进度。")
    print(f"{'='*60}\n")

    # 开启数据包监听
    page.listen.start('https://edith.xiaohongshu.com/api/sns/web/v1/feed')

    scraped_ids = set()

    try:
        while True:
            # 持续等待数据包响应
            res = page.listen.wait(timeout=1) 
            
            if res:
                try:
                    raw = res.response.body
                    if 'data' in raw and raw['data']['items']:
                        info = raw['data']['items'][0]['note_card']
                        post_id = info.get('note_id', '')

                        if post_id and post_id not in scraped_ids:
                            # 1. 基础信息解析
                            title = info.get('title', '无标题').strip()
                            # 清洗正文中的换行符
                            content = re.sub(r'\s+', ' ', info.get('desc', '')).strip()
                            create_at = info.get('time', '')
                            user_id = info.get('user', {}).get('user_id', '')
                            liked_count = info.get('interact_info', {}).get('liked_count', '0')
                            
                            # 2. 媒体资源解析
                            image_list = info.get('image_list', [])
                            cover_url = image_list[0].get('url_default', '') if image_list else ''
                            image_urls = [i.get('url_default', '') for i in image_list]
                            
                            video_url = ''
                            if info.get('type') == 'video':
                                stream = info.get('video', {}).get('media', {}).get('stream', {})
                                # 优先尝试最高画质链接
                                v_s = stream.get('h264') or stream.get('h265')
                                if v_s: 
                                    video_url = v_s[0].get('master_url') or v_s[0].get('backup_urls', [''])[0]

                            # 3. 写入CSV记录
                            row = {
                                "post_id": post_id, "title": title, "content": content,
                                "create_at": create_at, "user_id": user_id, "liked_count": liked_count,
                                "cover_url": cover_url, "post_url": page.url, 
                                "image_urls": image_urls, "video_url": video_url
                            }

                            with open(csv_file, mode='a', newline='', encoding='utf-8-sig') as f:
                                csv.DictWriter(f, fieldnames=fieldnames).writerow(row)
                            
                            # 4. 执行异步下载任务
                            print(f"📌 发现新帖子: {title[:15]}... | 开始下载资源...")
                            save_assets(post_id, cover_url, image_urls, video_url)
                            
                            scraped_ids.add(post_id)
                            print(f"✅ 处理完成: {post_id}")
                
                except Exception as e:
                    print(f"⚠️ 解析数据包时出错: {e}")
            
            # 降低CPU占用
            time.sleep(0.1)

    except KeyboardInterrupt:
        print("\n👋 程序已由用户停止，数据已安全保存。")
    finally:
        page.quit()

if __name__ == "__main__":
    run_manual_scraper()