import time
import re
import random
import pandas as pd
import os
import csv
from DrissionPage import ChromiumPage, ChromiumOptions

# ========= 核心多开配置 =========
PORT = 9223  # 脚本2请改为 9223
CSV_PATH = "所有数据集/59.csv"
# ===============================

USER_COL = "user_id"
FANS_COL = "fans_count"
PROFILE_URL = "https://www.xiaohongshu.com/user/profile/{uid}"

WAIT_RENDER_SEC = 4.0
SLEEP_MIN = 3
SLEEP_MAX = 8

SAVE_EVERY = 20
COOLDOWN_EVERY = 40
COOLDOWN_SEC = 3 * 60

# ========= 数字解析 =========
def parse_cn_num(s: str) -> int:
    s = (s or "").strip().replace("+", "")
    m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*([万千]?)", s)
    if not m:
        return 0
    num = float(m.group(1))
    unit = m.group(2)
    if unit == "万":
        num *= 10000
    elif unit == "千":
        num *= 1000
    return int(num)

# ========= 抓粉丝数 =========
def get_fans_count(page, uid: str) -> int:
    try:
        page.get(PROFILE_URL.format(uid=uid))
        interactions = page.ele("css:.user-interactions", timeout=8)
        
        if interactions:
            fans_node = interactions.ele('text:粉丝')
            if fans_node:
                cnt_ele = fans_node.parent().ele("css:.count") or fans_node.ele("css:.count")
                if cnt_ele:
                    return parse_cn_num(cnt_ele.text)
        
        body_text = page.ele("tag:body").text
        m = re.search(r"粉丝\s*([0-9\.万千\+]+)", body_text)
        if m:
            return parse_cn_num(m.group(1))
    except Exception as e:
        # 这里也增加了具体的报错输出
        print(f"  ❌ [Port:{PORT}] uid:{uid} 抓取异常: {e}")
    return 0

def main():
    if not os.path.exists(CSV_PATH):
        print(f"❌ [Port:{PORT}] 找不到文件: {CSV_PATH}")
        return

    # --- 1. 预检 ---
    print(f"🔍 [Port:{PORT}] 正在检查 {CSV_PATH} 格式...")
    # (预检逻辑保持不变...)
    
    # --- 2. 加载数据 ---
    try:
        df = pd.read_csv(CSV_PATH, dtype={USER_COL: "string"}, encoding="utf-8-sig", on_bad_lines='skip', engine='python')
    except Exception as e:
        print(f"❌ [Port:{PORT}] 读取 CSV 失败: {e}"); return

    if FANS_COL not in df.columns:
        df[FANS_COL] = pd.NA

    mask_need = df[FANS_COL].isna() | (df[FANS_COL] == 0)
    # 转换为列表以便跟踪进度
    uniq_ids = [uid for uid in df.loc[mask_need, USER_COL].dropna().unique() if uid]
    total_to_do = len(uniq_ids)

    print(f"📊 [Port:{PORT}] 任务启动！总计待爬取: {total_to_do} 个 ID")
    if total_to_do == 0: return

    # --- 3. 浏览器配置 ---
    co = ChromiumOptions()
    co.set_local_port(PORT)
    co.set_user_data_path(f'./browser_data_{PORT}') 
    page = ChromiumPage(co)

    try:
        for i, uid in enumerate(uniq_ids, 1):
            # 执行爬取
            fans = get_fans_count(page, uid)
            
            # 回填数据
            df.loc[df[USER_COL] == uid, FANS_COL] = fans
            
            # 【核心修改】：每次爬取后立即输出结果和进度
            percent = (i / total_to_do) * 100
            print(f"✨ [Port:{PORT}] 进度:{percent:>5.2f}% | 序号:{i}/{total_to_do} | UID:{uid:<15} | 粉丝数:{fans:<8}")
            
            # 间隔等待
            time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))

            # 定期保存
            if i % SAVE_EVERY == 0:
                df.to_csv(CSV_PATH, index=False, encoding="utf-8-sig", quoting=1)
                print(f"💾 [Port:{PORT}] 已自动保存当前进度...")

            # 休息
            if i % COOLDOWN_EVERY == 0:
                print(f"💤 [Port:{PORT}] 已处理 {i} 个，休息 {COOLDOWN_SEC} 秒...")
                time.sleep(COOLDOWN_SEC)
                
    except KeyboardInterrupt:
        print(f"\n🛑 [Port:{PORT}] 用户手动停止")
    finally:
        df.to_csv(CSV_PATH, index=False, encoding="utf-8-sig", quoting=1)
        print(f"🏁 [Port:{PORT}] 运行结束，数据已最终保存。")

if __name__ == "__main__":
    main()