import os
import time
import json
from dotenv import load_dotenv
from supabase import create_client
from playwright.sync_api import sync_playwright

load_dotenv()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
SEASON_LABEL = "2025/26"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ─── League config ────────────────────────────────────────────────
LEAGUES = {
    "premier_league": {"unique_tournament_id": 17,   "season_id": 76986},
    "la_liga":        {"unique_tournament_id": 8,    "season_id": 77559},
    "serie_a":        {"unique_tournament_id": 23,   "season_id": 76457},
    "bundesliga":     {"unique_tournament_id": 35,   "season_id": 77333},
    "ligue_1":        {"unique_tournament_id": 34,   "season_id": 77356},
}

CLUBS = [
    # Premier League
    ("Arsenal",           42,   "premier_league"),
    ("Aston Villa",       40,   "premier_league"),
    ("Chelsea",           38,   "premier_league"),
    ("Liverpool",         44,   "premier_league"),
    ("Manchester City",   17,   "premier_league"),
    ("Manchester United", 35,   "premier_league"),
    ("Newcastle United",  39,   "premier_league"),
    ("Tottenham Hotspur", 33,   "premier_league"),
    # La Liga
    ("Real Madrid",       2829,  "la_liga"),
    ("Barcelona",         2817,  "la_liga"),
    ("Atlético Madrid",   2836,  "la_liga"),
    # Serie A
    ("AC Milan",          2692,  "serie_a"),
    ("Inter Milan",       2697,  "serie_a"),
    ("Napoli",            2714,  "serie_a"),
    ("Como",              2704,  "serie_a"),
    ("Juventus",          2687,  "serie_a"),
    # Bundesliga
    ("Bayern Munich",     2672,  "bundesliga"),
    ("Borussia Dortmund", 2673,  "bundesliga"),
    ("RB Leipzig",       36360,  "bundesliga"),
    # Ligue 1
    ("PSG",               1644,  "ligue_1"),
]

POSITION_MAP = {
    "G": "goalkeeper",
    "D": "defender",
    "M": "midfielder",
    "F": "attacker",
}


# ─── Core functions ───────────────────────────────────────────────

def fetch_json(page, url, retries=2):
    for attempt in range(retries + 1):
        try:
            response = page.goto(url, wait_until="domcontentloaded", timeout=20000)
            if response and response.status >= 400:
                print(f"    HTTP {response.status} for {url}")
                if attempt < retries:
                    time.sleep(3)
                    continue
                return None
            text = page.inner_text("body")
            data = json.loads(text)
            if "error" in data and "statistics" not in data:
                print(f"    API error response for {url}")
                if attempt < retries:
                    time.sleep(3)
                    continue
                return None
            return data
        except Exception as e:
            print(f"    Error fetching {url}: {e}")
            if attempt < retries:
                time.sleep(3)
                continue
            return None
    return None


def get_squad(page, team_id):
    url = f"https://api.sofascore.com/api/v1/team/{team_id}/players"
    data = fetch_json(page, url)
    if data:
        return data.get("players", [])
    return []


def get_player_stats(page, sofascore_id, unique_tournament_id, season_id):
    url = (
        f"https://api.sofascore.com/api/v1/player/{sofascore_id}"
        f"/unique-tournament/{unique_tournament_id}"
        f"/season/{season_id}"
        f"/statistics/overall"
    )
    data = fetch_json(page, url)
    return data


def parse_stats(raw):
    s = raw.get("statistics", {})

    goals          = s.get("goals")
    minutes        = s.get("minutesPlayed")
    shots_total    = s.get("totalShots")
    shots_on       = s.get("shotsOnTarget")
    saves          = s.get("saves")
    goals_conceded = s.get("goalsConceded")
    tackles_total  = s.get("tackles")
    tackles_won    = s.get("tacklesWon")
    duels_won      = s.get("totalDuelsWon")
    duels_lost     = s.get("duelLost")
    ground_won     = s.get("groundDuelsWon")
    ground_pct     = s.get("groundDuelsWonPercentage")
    aerial_won     = s.get("aerialDuelsWon")
    aerial_pct     = s.get("aerialDuelsWonPercentage")
    dribbles_done  = s.get("successfulDribbles")
    dribbles_pct   = s.get("successfulDribblesPercentage")
    penalty_goals  = s.get("penaltyGoals") or 0

    duels_total = (
        (duels_won or 0) + (duels_lost or 0)
        if duels_won is not None or duels_lost is not None
        else None
    )
    duels_won_pct = (
        round((duels_won / duels_total) * 100, 1)
        if duels_won and duels_total
        else None
    )
    ground_total = (
        round(ground_won / (ground_pct / 100))
        if ground_won and ground_pct
        else None
    )
    aerial_total = (
        round(aerial_won / (aerial_pct / 100))
        if aerial_won and aerial_pct
        else None
    )
    tackle_success_pct = (
        round((tackles_won / tackles_total) * 100, 1)
        if tackles_won and tackles_total
        else None
    )
    shot_accuracy_pct = (
        round((shots_on / shots_total) * 100, 1)
        if shots_on and shots_total
        else None
    )
    conversion_rate = (
        round((goals / shots_total) * 100, 1)
        if goals and shots_total
        else None
    )
    non_penalty_goals = (
        (goals or 0) - penalty_goals
        if goals is not None
        else None
    )
    save_pct = (
        round((saves / (saves + goals_conceded)) * 100, 1)
        if saves and goals_conceded is not None and (saves + goals_conceded) > 0
        else None
    )
    goals_allowed_per90 = (
        round((goals_conceded * 90 / minutes), 2)
        if goals_conceded is not None and minutes
        else None
    )

    return {
        "appearances":         s.get("appearances"),
        "minutes_played":      minutes,
        "goals":               goals,
        "assists":             s.get("assists"),
        "xg":                  s.get("expectedGoals"),
        "xa":                  s.get("expectedAssists"),
        "non_penalty_goals":   non_penalty_goals,
        "shots_total":         shots_total,
        "shots_on_target":     shots_on,
        "shot_accuracy_pct":   shot_accuracy_pct,
        "conversion_rate":     conversion_rate,
        "penalties_scored":    s.get("penaltyGoals"),
        "penalties_taken":     s.get("penaltiesTaken"),
        "key_passes":          s.get("keyPasses"),
        "big_chances_created": s.get("bigChancesCreated"),
        "dribbles_completed":  dribbles_done,
        "dribble_success_pct": dribbles_pct,
        "fouls_won":           s.get("wasFouled"),
        "fouls_committed":     s.get("fouls"),
        "passes_total":        s.get("totalPasses"),
        "pass_accuracy_pct":   s.get("accuratePassesPercentage"),
        "long_balls":          s.get("accurateLongBalls"),
        "crosses":             s.get("totalCross"),
        "tackles":             tackles_total,
        "tackle_success_pct":  tackle_success_pct,
        "interceptions":       s.get("interceptions"),
        "duels_won":           duels_won,
        "duels_total":         duels_total,
        "duels_won_pct":       duels_won_pct,
        "ground_duels_won":    ground_won,
        "ground_duels_total":  ground_total,
        "aerial_duels_won":    aerial_won,
        "aerial_duels_total":  aerial_total,
        "ball_recoveries":     s.get("ballRecovery"),
        "dribbles_past":       s.get("dribbledPast"),
        "blocks":              s.get("blockedShots"),
        "clearances":          s.get("clearances"),
        "errors_leading_goal": s.get("errorLeadToGoal"),
        "penalties_conceded":  s.get("penaltyConceded"),
        "clean_sheets":        s.get("cleanSheet"),
        "saves":               saves,
        "save_pct":            save_pct,
        "goals_conceded":      goals_conceded,
        "penalties_saved":     s.get("penaltySave"),
        "sweeper_clearances":  s.get("punches"),
        "catches":             s.get("runsOut"),
        "crosses_against":     s.get("crossesNotClaimed"),
        "goals_allowed_per90": goals_allowed_per90,
        "chances_created":     s.get("totalAttemptAssist"),
    }


def upsert_player(player_data, club_id, role):
    p = player_data.get("player", player_data)
    sofascore_id = p.get("id")
    name         = p.get("name")
    short_name   = p.get("shortName", name)
    position     = p.get("position", "")
    nationality  = p.get("country", {}).get("name", "") if isinstance(p.get("country"), dict) else ""

    if not sofascore_id or not name:
        return None

    supabase.table("players").upsert({
        "sofascore_id":    sofascore_id,
        "name":            name,
        "short_name":      short_name,
        "club_id":         club_id,
        "role":            role,
        "position_detail": position,
        "nationality":     nationality,
        "is_active":       True,
    }, on_conflict="sofascore_id").execute()

    result = supabase.table("players").select("id").eq("sofascore_id", sofascore_id).single().execute()
    return result.data["id"] if result.data else None


def run():
    print(f"Underlap scraper starting — season {SEASON_LABEL}\n")
    total_success, total_failed, total_skipped = 0, 0, 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
        page = context.new_page()

        print("Warming up browser...")
        page.goto("https://www.sofascore.com", wait_until="domcontentloaded", timeout=60000)
        time.sleep(3)

        print("Season IDs loaded.\n")

        for club_name, team_id, league_key in CLUBS:
            print(f"\n{'='*50}")
            print(f"{club_name} ({league_key})")
            print(f"{'='*50}")

            league = LEAGUES.get(league_key)
            if not league or not league["season_id"]:
                print(f"  ⏭️  Skipping — no season_id for {league_key}")
                continue

            ut_id     = league["unique_tournament_id"]
            season_id = league["season_id"]

            club = supabase.table("clubs").select("id").eq("name", club_name).single().execute()
            if not club.data:
                print(f"  ❌ Club not found in Supabase: {club_name}")
                continue
            club_id = club.data["id"]

            squad = get_squad(page, team_id)
            if not squad:
                print(f"  ❌ No squad returned (check team_id={team_id})")
                continue

            print(f"  Found {len(squad)} players")
            time.sleep(2)

            club_success = 0
            for entry in squad:
                p_data = entry.get("player", entry)
                pos_code = p_data.get("position", "")
                role = POSITION_MAP.get(pos_code)
                if not role:
                    continue

                name = p_data.get("name", "unknown")
                sofascore_id = p_data.get("id")
                print(f"  → {name}", end="")

                player_id = upsert_player(entry, club_id, role)
                if not player_id:
                    print(" — ❌ upsert failed")
                    total_failed += 1
                    continue

                raw = get_player_stats(page, sofascore_id, ut_id, season_id)
                if not raw or "statistics" not in raw:
                    print(" — ⏭️  no stats (may not have played)")
                    total_skipped += 1
                    time.sleep(1.5)
                    continue

                stats = parse_stats(raw)
                stats["player_id"] = player_id
                stats["season"]    = SEASON_LABEL

                supabase.table("player_stats").upsert(
                    stats, on_conflict="player_id,season"
                ).execute()

                goals = stats.get('goals', 0) or 0
                assists = stats.get('assists', 0) or 0
                apps = stats.get('appearances', 0) or 0
                print(f" — ✅ {goals}G {assists}A in {apps} apps")
                total_success += 1
                club_success += 1
                time.sleep(1.5)

            supabase.table("scrape_log").insert({
                "club_id": club_id,
                "season":  SEASON_LABEL,
                "status":  "success" if club_success > 0 else "partial",
                "notes":   f"{len(squad)} squad, {club_success} with stats",
            }).execute()

        browser.close()

    print(f"\n{'='*50}")
    print(f"DONE — ✅ {total_success} success | ⏭️ {total_skipped} no stats | ❌ {total_failed} failed")
    print(f"{'='*50}")


if __name__ == "__main__":
    run()
