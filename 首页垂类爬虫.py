import time
import csv
import os
import json
import re
from DrissionPage import ChromiumPage
from DrissionPage.common import Actions
from DrissionPage.common import Keys

# CSV文件路径
csv_file = "test1.csv"
fieldnames = ["class", "post_id", "title", "content", "create_at", "user_id", "liked_count",
              "cover_url", "post_url", "image_urls", "video_url"]

video_csv_file = "download_urls.csv"
video_fieldnames = ["post_id", "cover_url", "image_urls", "video_url"]
raw_data_file = "xiaohongshu_raw_data.json"

# 检查文件是否存在，不存在则创建并写入表头
def initialize_csv():
    if not os.path.exists(csv_file):
        with open(csv_file, mode='a', newline='', encoding='utf-8-sig') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()

    if not os.path.exists(video_csv_file):
        with open(video_csv_file, mode='a', newline='', encoding='utf-8-sig') as file:
            writer = csv.DictWriter(file, fieldnames=video_fieldnames)
            writer.writeheader()

# 保存数据到CSV文件
def save_to_csv(cl, post_id, title, content, create_at, user_id, liked_count, cover_url, post_url, image_urls, video_url):
    with open(csv_file, mode='a', newline='', encoding='utf-8-sig') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writerow({
            "class": cl,
            "post_id": post_id,
            "title": title,
            "content": content,
            "create_at": create_at,
            "user_id": user_id,
            "liked_count": liked_count,
            "cover_url": cover_url,
            "post_url": post_url,
            "image_urls": image_urls,
            "video_url": video_url
        })

# 保存视频信息到CSV文件
def save_video_to_csv(post_id, cover_url, image_urls, video_url):
    with open(video_csv_file, mode='a', newline='', encoding='utf-8-sig') as file:
        writer = csv.DictWriter(file, fieldnames=video_fieldnames)
        writer.writerow({
            "post_id": post_id,
            "cover_url": cover_url,
            "image_urls": image_urls,
            "video_url": video_url
        })

# 保存原始响应数据到文件
def save_raw_data(data):
    with open(raw_data_file, 'a+', encoding='utf-8-sig') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write('\n')

# 转换点赞数：处理“万”字
def convert_liked_count(liked_count_str):
    # 去除 "+" 字符
    liked_count_str = liked_count_str.replace('+', '')

    if '万' in liked_count_str:
        # 去除 "万" 字符并转换为浮动数字
        liked_count_str = liked_count_str.replace('万', '')
        liked_count = float(liked_count_str) * 10000
    else:
        # 如果没有 "万"，直接转换为整数
        liked_count = int(liked_count_str)
    
    return int(liked_count)


# 初始化CSV文件
initialize_csv()

# 打开浏览器
page = ChromiumPage(timeout=2)
ac = Actions(page)
cl = 'fitness'
# 定义抓取的目标总条数
total_target = 300
# 每次抓取的条数
grab_per_cycle = 10

# 记录已抓取的总条数
total_count = 0
page.get(f'https://www.xiaohongshu.com/explore?channel_id=homefeed.{cl}_v3')
page.listen.start('https://edith.xiaohongshu.com/api/sns/web/v1/feed')
time.sleep(6)
page.wait.load_start()
while total_count < total_target:
    # 每次刷新后重新访问页面
    #for o in range(20):
        for g in range(40):  # 遍历每页的帖子
            try:
                tag = page.ele(f'xpath://section[contains(@class, "note-item") and @data-index="{g}"]')
                if '大家都在搜' in tag.text or '相关搜索' in tag.text:
                    continue
                else:
                    tag.click()
                    time.sleep(3)

                # 获取当前页面的网址
                post_url = page.url
                print(f"当前帖子的URL: {post_url}")

                try:
                    res = page.listen.wait(timeout=10)
                    if not res:
                        print(f'第{total_count + 1}条数据未获取到数据，跳过。')
                        continue

                    data = res.response.body

                    # 保存原始响应数据
                    save_raw_data(data)

                    # 标题，内容
                    note_card = data['data']['items'][0]['note_card']
                    title = note_card['title']
                    if not title:
                        print(f"第{total_count + 1}条数据标题为空，跳过保存")
                        page.back()  # 或者使用 page.refresh() 刷新页面
                        time.sleep(3)  # 等待页面加载
                        continue
                    content = note_card['desc']
                    content = re.sub(r'\s+', ' ', content).strip()

                    # 时间                                                                               
                    create_at = data["data"]["items"][0]['note_card']["time"]

                    # 封面图和详情图
                    image_list = note_card.get('image_list', [])
                    cover_url = image_list[0].get('url_default', '')
                    image_urls = []
                    for i in image_list:
                        image_url = i['url_default']
                        image_urls.append(image_url)

                    # 用户信息
                    user_info = note_card.get('user', {})
                    user_id = user_info.get('user_id', '')

                    # 点赞数
                    interact_info = note_card.get('interact_info', {})
                    liked_count = convert_liked_count(interact_info.get('liked_count', '0'))  # 使用转换函数

                    # 视频链接
                    post_id = note_card.get('note_id', '')
                    type = data['data']['items'][0]['note_card']['type']
                    if type == 'video':
                        # 获取初步 backup_urls，根据 h264 或 h265 流的存在来选择
                        if "h264" in data["data"]["items"][0]["note_card"]["video"]['media']["stream"]:
                            initial_backup_urls = data["data"]["items"][0]["note_card"]["video"]['media']["stream"]["h264"][0]["backup_urls"]
                        elif "h265" in data["data"]["items"][0]["note_card"]["video"]['media']["stream"]:
                            initial_backup_urls = data["data"]["items"][0]["note_card"]["video"]['media']["stream"]["h265"][0]["backup_urls"]
                        else:
                            initial_backup_urls = []
                        # 判断初步 backup_urls 是否是列表
                        if isinstance(initial_backup_urls, list):
                            # 如果是列表，取其中的第一条链接
                            backup_urls = initial_backup_urls[0] if initial_backup_urls else None
                        else:
                            # 如果不是列表，直接使用初步 backup_urls
                            backup_urls = initial_backup_urls
                    else:
                        backup_urls = ''
                    video_url = backup_urls

                    # 保存到CSV
                    print(title)
                    save_to_csv(cl, post_id, title, content, create_at, user_id, liked_count,
                                cover_url, post_url, image_urls, video_url)

                    # 如果是视频，保存到视频CSV
                    if video_url:
                        save_video_to_csv(post_id, cover_url, image_urls, video_url)

                    total_count += 1
                    print(f"抓取到第{total_count}条数据")

                    # 模拟按下ESC键，关闭当前帖子
                    ac.key_down(Keys.ESCAPE)
                    ac.key_up(Keys.ESCAPE)
                    time.sleep(2)

                except Exception as wait_error:
                    print(f"等待数据时出错: {wait_error}")
                    continue

            except Exception as e:
                print(f"处理数据时出错: {e}")
                continue
        
        print(f"已抓取 {total_count} 条数据，刷新页面...")
        page.refresh()
        time.sleep(300)

        # 确保总抓取数量达到目标
        if total_count >= total_target:
            break

print(f"数据抓取完成，已保存 {total_count} 条数据")
