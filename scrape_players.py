"""
NPB選手データ収集スクリプト
Wikipedia日本語版からプロ野球選手の経歴情報を収集し、JSON形式で出力する。
"""

import json
import os
import re
import sys
import time
import unicodedata
from datetime import date

import requests

# === 設定 ===
API_URL = "https://ja.wikipedia.org/w/api.php"
HEADERS = {
    "User-Agent": "YakyuQuizBot/1.0 (https://github.com/yakyu-quiz; yakyu.quiz.bot@example.com) Python/requests",
    "Accept-Encoding": "gzip",
}
REQUEST_INTERVAL = 2.0  # 秒
MAX_RETRIES = 3
OUTPUT_FILE = "players.json"
PROGRESS_FILE = "progress.json"  # 中断・再開用
MIN_DEBUT_YEAR = 2000

# 12球団カテゴリ
TEAM_CATEGORIES = {
    "読売ジャイアンツ": "Category:読売ジャイアンツ及び東京巨人軍の選手",
    "阪神タイガース": "Category:阪神タイガース及びその前身球団の選手",
    "広島東洋カープ": "Category:広島東洋カープ及び広島カープの選手",
    "中日ドラゴンズ": "Category:中日ドラゴンズ及びその前身球団の選手",
    "横浜DeNAベイスターズ": "Category:横浜DeNAベイスターズ及びその前身球団の選手",
    "東京ヤクルトスワローズ": "Category:東京ヤクルトスワローズ及びその前身球団の選手",
    "福岡ソフトバンクホークス": "Category:福岡ソフトバンクホークス及びその前身球団の選手",
    "埼玉西武ライオンズ": "Category:埼玉西武ライオンズ及びその前身球団の選手",
    "東北楽天ゴールデンイーグルス": "Category:東北楽天ゴールデンイーグルスの選手",
    "千葉ロッテマリーンズ": "Category:千葉ロッテマリーンズ及びその前身球団の選手",
    "北海道日本ハムファイターズ": "Category:北海道日本ハムファイターズ及びその前身球団の選手",
    "オリックス・バファローズ": "Category:オリックス・バファローズ及びその前身球団の選手",
}


def api_get(params):
    """Wikipedia APIにリクエストを送信する。リトライ付き。"""
    params["format"] = "json"
    for attempt in range(MAX_RETRIES):
        time.sleep(REQUEST_INTERVAL)
        try:
            resp = requests.get(API_URL, params=params, headers=HEADERS, timeout=30)
            if resp.status_code == 429:
                wait = 5 * (attempt + 1)
                print(f"  [RATE LIMIT] waiting {wait}s...", end="", flush=True)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(3)
                continue
            print(f"  [ERROR] API request failed: {e}", file=sys.stderr)
            return None
    return None


# === Step 1: カテゴリから選手ページ一覧を取得 ===
def fetch_category_members(category_title):
    """カテゴリに属する全ページ(ns=0)を取得する。ページネーション対応。"""
    members = []
    params = {
        "action": "query",
        "list": "categorymembers",
        "cmtitle": category_title,
        "cmlimit": "500",
        "cmtype": "page",
        "cmnamespace": "0",
    }
    while True:
        data = api_get(params)
        if data is None:
            break
        for m in data.get("query", {}).get("categorymembers", []):
            title = m["title"]
            # 一覧ページやテンプレートを除外
            if "の選手一覧" in title or "Template:" in title:
                continue
            members.append(title)
        cont = data.get("continue")
        if cont and "cmcontinue" in cont:
            params["cmcontinue"] = cont["cmcontinue"]
        else:
            break
    return members


# === Step 2: 選手ページのwikitext取得 (バッチ) ===
def fetch_wikitext_batch(page_titles):
    """複数ページのwikitextを一括取得する (最大50件)。"""
    results = {}
    titles_str = "|".join(page_titles)
    data = api_get({
        "action": "query",
        "titles": titles_str,
        "prop": "revisions",
        "rvprop": "content",
        "rvslots": "main",
    })
    if data is None or "query" not in data:
        return results
    pages = data["query"].get("pages", {})
    for page_id, page_data in pages.items():
        if int(page_id) < 0:
            continue  # ページが存在しない
        title = page_data.get("title", "")
        revisions = page_data.get("revisions")
        if revisions and len(revisions) > 0:
            slots = revisions[0].get("slots", {})
            main_slot = slots.get("main", {})
            content = main_slot.get("*", "")
            if content:
                results[title] = content
    return results


def fetch_wikitext(page_title):
    """単一ページのwikitextを取得する (フォールバック用)。"""
    result = fetch_wikitext_batch([page_title])
    return result.get(page_title)


# === Step 3: Infobox解析 ===
def extract_infobox(wikitext):
    """Infobox baseball playerテンプレートを抽出する。"""
    start = wikitext.find("{{Infobox baseball player")
    if start == -1:
        # 表記揺れ対応
        start = wikitext.find("{{Infobox Baseball player")
    if start == -1:
        start = wikitext.find("{{Infobox 野球選手")
    if start == -1:
        return None

    # ネストされた {{ }} を考慮してInfoboxの終端を見つける
    depth = 0
    i = start
    while i < len(wikitext):
        if wikitext[i:i+2] == "{{":
            depth += 1
            i += 2
        elif wikitext[i:i+2] == "}}":
            depth -= 1
            if depth == 0:
                return wikitext[start:i+2]
            i += 2
        else:
            i += 1
    return None


def extract_field(infobox, field_name):
    """Infoboxから指定フィールドの値を抽出する。"""
    # | フィールド名 = 値 のパターン
    pattern = rf"\|\s*{re.escape(field_name)}\s*=(.*?)(?=\n\||\n\}})"
    m = re.search(pattern, infobox, re.DOTALL)
    if m:
        return m.group(1).strip()
    return None


def clean_wikilink(text):
    """[[記事名|表示名]] → 表示名、[[記事名]] → 記事名に変換。"""
    # [[記事名|表示名]] パターン
    text = re.sub(r"\[\[[^\]]*?\|([^\]]*?)\]\]", r"\1", text)
    # [[記事名]] パターン
    text = re.sub(r"\[\[([^\]]*?)\]\]", r"\1", text)
    return text


def clean_text(text):
    """wikitextの装飾を除去する。"""
    text = clean_wikilink(text)
    # <br />, <br>, <br/> を除去
    text = re.sub(r"<br\s*/?>", " ", text, flags=re.IGNORECASE)
    # <ref>...</ref> を除去
    text = re.sub(r"<ref[^>]*>.*?</ref>", "", text, flags=re.DOTALL)
    text = re.sub(r"<ref[^>]*/?>", "", text)
    # {{R|...}} を除去 (参照テンプレート)
    text = re.sub(r"\{\{R\|[^}]*\}\}", "", text)
    # {{Ruby|漢字|よみ}} → 漢字
    text = re.sub(r"\{\{Ruby\|([^|]+)\|[^}]+\}\}", r"\1", text)
    # {{JIS2004フォント|文字}} → 文字、HTML数値参照をデコード
    text = re.sub(r"\{\{JIS2004フォント\|([^}]+)\}\}", r"\1", text)
    # 数値参照 &#NNN; と16進参照 &#xHHHH; をデコード
    text = re.sub(r"&#(\d+);", lambda m: chr(int(m.group(1))), text)
    text = re.sub(r"&#x([0-9a-fA-F]+);", lambda m: chr(int(m.group(1), 16)), text)
    # {{Flagicon|...}} を除去
    text = re.sub(r"\{\{Flagicon\|[^}]*\}\}", "", text)
    # CJK互換漢字等をNFKC正規化（JIS2004フォントテンプレート由来）
    text = unicodedata.normalize("NFKC", text)
    return text.strip()


def parse_draft_year(infobox):
    """プロ入り年度を解析する。"""
    field = extract_field(infobox, "プロ入り年度")
    if not field:
        return None
    # {{NPBドラフト|2012}} パターン
    m = re.search(r"\{\{NPBドラフト\|(\d{4})\}\}", field)
    if m:
        return int(m.group(1))
    # 普通の年度
    m = re.search(r"(\d{4})", field)
    if m:
        return int(m.group(1))
    return None


def parse_career(infobox):
    """経歴フィールドを解析して、高校・大学・社会人・プロ球団に分類する。"""
    career_text = extract_field(infobox, "経歴")
    if not career_text:
        return None

    high_schools = []
    universities = []
    amateurs = []
    pro_teams = []

    # 箇条書きの各行を処理
    lines = career_text.split("\n")
    for line in lines:
        line = line.strip()
        if not line.startswith("*"):
            continue
        line = line.lstrip("* ").strip()
        if not line:
            continue

        # wikiリンクを処理する前に、表示テキストを取得
        display = clean_text(line)
        # テンプレート除去
        display = re.sub(r"\{\{[^}]*\}\}", "", display).strip()

        # 年度括弧を抽出: (YYYY - YYYY), (YYYY - ), (YYYY, YYYY - YYYY) 等
        # 各種ダッシュに対応: -, ‐(U+2010), –(U+2013), ー
        DASH = r"[-\u2010\u2013ー]"
        year_match = re.search(
            rf"[（(](\d{{4}})\s*{DASH}\s*(\d{{4}}|途中)?\s*(?:,\s*\d{{4}}\s*{DASH}?\s*\d{{0,4}}\s*)*[）)]",
            display
        )
        if not year_match:
            # 単年度 (YYYY) or (YYYY, YYYY) or 全角（YYYY）
            year_match = re.search(r"[（(](\d{4})(?:\s*,\s*\d{4})*\s*[）)]", display)

        if year_match:
            # プロ球団 (年度括弧がある)
            start_year = year_match.group(1)
            try:
                end_year = year_match.group(2) if year_match.group(2) else ""
            except IndexError:
                end_year = start_year  # 単年度パターン
            team_name = display[:year_match.start()].strip()
            # チーム名の末尾の改行やスペース除去
            team_name = re.sub(r"\s+$", "", team_name)

            if start_year and team_name:
                if end_year == start_year:
                    years_str = start_year  # 単年度
                elif end_year:
                    years_str = f"{start_year}-{end_year}"
                else:
                    years_str = f"{start_year}-"
                pro_teams.append({"team": team_name, "years": years_str})
        else:
            # 年度括弧がない → アマチュア経歴
            if "高等学校" in display or "高校" in display or "高等部" in display:
                if display not in high_schools:
                    high_schools.append(display)
            elif "大学" in display or "大學" in display:
                if display not in universities:
                    universities.append(display)
            elif display:
                # 「軟式」を含む経歴は引退後の草野球なので除外
                if "軟式" in display:
                    continue
                # 社会人/独立リーグなど
                if display not in amateurs:
                    amateurs.append(display)

    return {
        "highSchool": high_schools or None,
        "university": universities or None,
        "amateur": amateurs or None,
        "proTeams": pro_teams,
    }


# === Step 4: 現役/OB判定 ===
def determine_status(infobox, career):
    """現役かOBかを判定する。"""
    # 経歴の最後のプロ球団が開放年度 (YYYY - ) なら現役（最優先）
    if career and career["proTeams"]:
        last_team = career["proTeams"][-1]
        if last_team["years"].endswith("-"):
            return "active"

    # 最終出場フィールドをチェック
    last_game = extract_field(infobox, "最終出場")
    if last_game:
        # コメントアウト <!-- --> を除去して実質的な内容があるか確認
        cleaned = re.sub(r"<!--.*?-->", "", last_game, flags=re.DOTALL).strip()
        if cleaned:
            return "retired"

    # 所属球団フィールドをチェック
    current_team = extract_field(infobox, "所属球団")
    if current_team and current_team.strip():
        # 球団名がある → 現役の可能性
        # ただしコーチ/監督の場合は引退
        role = extract_field(infobox, "役職")
        if role and role.strip():
            return "retired"
        return "active"

    return "retired"


def compute_debut_year(career, draft_year):
    """NPBデビュー年を推定する。ドラフト年+1 or 最初のプロ球団の開始年。"""
    if career and career["proTeams"]:
        first_team = career["proTeams"][0]
        m = re.match(r"(\d{4})", first_team["years"])
        if m:
            return int(m.group(1))
    if draft_year:
        return draft_year + 1
    return None


# === Step 5: メイン処理 ===
def load_progress():
    """進捗ファイルを読み込む。"""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"processed_pages": [], "players": []}


def save_progress(progress):
    """進捗ファイルを保存する。"""
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(progress, f, ensure_ascii=False, indent=2)


def main(teams=None):
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

    if teams is None:
        teams = TEAM_CATEGORIES
    elif isinstance(teams, list):
        teams = {k: v for k, v in TEAM_CATEGORIES.items() if k in teams}

    progress = load_progress()
    processed_set = set(progress["processed_pages"])
    players = progress["players"]

    print(f"=== NPB選手データ収集開始 ===")
    print(f"対象球団数: {len(teams)}")
    print(f"既に処理済み: {len(processed_set)} ページ, {len(players)} 選手")
    print()

    total_fetched = 0
    total_skipped = 0
    total_errors = 0

    for team_name, category in teams.items():
        print(f"--- {team_name} ---")
        print(f"  カテゴリ: {category}")

        members = fetch_category_members(category)
        print(f"  カテゴリメンバー: {len(members)} ページ")

        team_added = 0
        # 未処理のメンバーをフィルタ
        pending = [t for t in members if t not in processed_set]
        print(f"  未処理: {len(pending)} ページ")

        # 50件ずつバッチで取得
        BATCH_SIZE = 50
        for batch_start in range(0, len(pending), BATCH_SIZE):
            batch = pending[batch_start:batch_start + BATCH_SIZE]
            batch_num = batch_start // BATCH_SIZE + 1
            total_batches = (len(pending) + BATCH_SIZE - 1) // BATCH_SIZE
            print(f"  [バッチ {batch_num}/{total_batches}] {len(batch)}件取得中...",
                  flush=True)

            wikitext_map = fetch_wikitext_batch(batch)

            for page_title in batch:
                wikitext = wikitext_map.get(page_title)
                if wikitext is None:
                    total_skipped += 1
                    processed_set.add(page_title)
                    progress["processed_pages"].append(page_title)
                    continue

                infobox = extract_infobox(wikitext)
                if infobox is None:
                    total_skipped += 1
                    processed_set.add(page_title)
                    progress["processed_pages"].append(page_title)
                    continue

                # ドラフト年チェック
                draft_year = parse_draft_year(infobox)
                career = parse_career(infobox)
                debut_year = compute_debut_year(career, draft_year)

                if debut_year is None or debut_year < MIN_DEBUT_YEAR:
                    total_skipped += 1
                    processed_set.add(page_title)
                    progress["processed_pages"].append(page_title)
                    continue

                # 選手名
                name_field = extract_field(infobox, "選手名")
                name = clean_text(name_field) if name_field else page_title
                # 空白の正規化 (全角スペースも)
                name = re.sub(r"[\s　]+", "", name)

                status = determine_status(infobox, career)

                player = {
                    "name": name,
                    "career": career,
                    "status": status,
                    "debutYear": debut_year,
                }
                players.append(player)
                team_added += 1
                total_fetched += 1
                print(f"    OK: {name} ({debut_year}, {status})")

                processed_set.add(page_title)
                progress["processed_pages"].append(page_title)

            # バッチごとに進捗保存
            progress["players"] = players
            save_progress(progress)
            print(f"  [進捗保存] 累計 {total_fetched} 選手")

        print(f"  → {team_name}: {team_added} 選手追加")
        print()

        # 球団ごとに進捗保存
        progress["players"] = players
        save_progress(progress)

    # 最終出力
    active_count = sum(1 for p in players if p["status"] == "active")
    retired_count = sum(1 for p in players if p["status"] == "retired")

    output = {
        "players": players,
        "metadata": {
            "totalCount": len(players),
            "activeCount": active_count,
            "retiredCount": retired_count,
            "scrapedAt": date.today().isoformat(),
        },
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"=== 完了 ===")
    print(f"総取得: {total_fetched}, スキップ: {total_skipped}, エラー: {total_errors}")
    print(f"選手数: {len(players)} (現役: {active_count}, 引退: {retired_count})")
    print(f"出力: {OUTPUT_FILE}")


if __name__ == "__main__":
    # コマンドライン引数で球団指定可能 (テスト用)
    if len(sys.argv) > 1:
        target_teams = sys.argv[1:]
        print(f"指定球団: {target_teams}")
        main(teams=target_teams)
    else:
        main()
