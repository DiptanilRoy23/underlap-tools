import os
import time
import requests
from supabase import create_client

# ── Config ────────────────────────────────────────────────────
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
SEASON_ID    = 61627
SEASON_LABEL = "2025/26"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Origin": "https://www.sofascore.com",
    "Referer": "https://www.sofascore.com/",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
    "Cache-Control": "no-cache",
}

# ── Sofascore team IDs matching our clubs table ───────────────
CLUBS = [
    ("Arsenal",           42),
    ("Aston Villa",       40),
    ("Chelsea",           38),
    ("Liverpool",         44),
    ("Manchester City",   17),
    ("Manchester United", 35),
    ("Newcastle United",  39),
    ("Tottenham Hotspur", 33),
    ("Real Madrid",        2),
    ("Barcelona",          3),
    ("Atlético Madrid",   45),
    ("AC Milan",         118),
    ("Inter Milan",      110),
    ("Napoli",           130),
    ("Como",            5821),
    ("Juventus",         114),
    ("Bayern Munich",     36),
    ("Borussia Dortmund", 37),
    ("RB Leipzig",      1062),
    ("PSG",               96),
]

# Position mapping from Sofascore to our roles
POSITION_MAP = {
    "G":  "goalkeeper",
    "D":  "defender",
    "M":  "midfielder",
    "F":  "attacker",
}


def get_squad(team_id: int) -> list:
    url = f"https://api.sofascore.com/api/v1/team/{team_id}/players"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code == 200:
            data = r.json()
            return data.get("players", [])
        print(f"  ⚠ Squad fetch status {r.status_code} for team {team_id}")
        return []
    except Exception as e:
        print(f"  ✗ Squad fetch error for team {team_id}: {e}")
        return []


def get_player_stats(sofascore_id: int) -> dict | None:
    url = f"https://api.sofascore.com/api/v1/player/{sofascore_id}/statistics/season/{SEASON_ID}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code == 200:
            return r.json()
        print(f"    ⚠ Stats status {r.status_code} for player {sofascore_id}")
        return None
    except Exception as e:
        print(f"    ✗ Stats error for player {sofascore_id}: {e}")
        return None


def parse_stats(raw: dict) -> dict:
    s = raw.get("statistics", {})
    return {
        "appearances":         s.get("appearances"),
        "minutes_played":      s.get("minutesPlayed"),
        "goals":               s.get("goals"),
        "assists":             s.get("assists"),
        "xg":                  s.get("expectedGoals"),
        "xa":                  s.get("expectedAssists"),
        "shots_total":         s.get("totalShots"),
        "shots_on_target":     s.get("shotsOnTarget"),
        "key_passes":          s.get("keyPasses"),
        "big_chances_created": s.get("bigChancesCreated"),
        "dribbles_completed":  s.get("successfulDribbles"),
        "fouls_won":           s.get("fouledTotal"),
        "passes_total":        s.get("totalPasses"),
        "pass_accuracy_pct":   s.get("accuratePasses"),
        "forward_passes":      s.get("forwardPasses"),
        "long_balls":          s.get("accurateLongBalls"),
        "through_balls":       s.get("accurateThroughBalls"),
        "crosses":             s.get("totalCross"),
        "chances_created":     s.get("chancesCreated"),
        "tackles":             s.get("tackles"),
        "interceptions":       s.get("interceptions"),
        "duels_won":           s.get("duelsWon"),
        "duels_total":         s.get("duelsTotal"),
        "ground_duels_won":    s.get("groundDuelsWon"),
        "ground_duels_total":  s.get("groundDuelsTotal"),
        "aerial_duels_won":    s.get("aerialDuelsWon"),
        "aerial_duels_total":  s.get("aerialDuelsTotal"),
        "fouls_committed":     s.get("foulsCommitted"),
        "ball_recoveries":     s.get("ballRecoveries"),
        "dribbles_past":       s.get("dribbledPast"),
        "progressive_carries": s.get("progressiveCarries"),
        "blocks":              s.get("blockedShots"),
        "clearances":          s.get("clearances"),
        "errors_leading_goal": s.get("errorLeadToGoal"),
        "penalties_conceded":  s.get("penaltyConceded"),
        "clean_sheets":        s.get("cleanSheets"),
        "saves":               s.get("saves"),
        "goals_conceded":      s.get("goalsConceded"),
        "penalties_saved":     s.get("penaltySaves"),
        "sweeper_clearances":  s.get("punches"),
        "catches":             s.get("runsOut"),
        "crosses_against":     s.get("crossesTotal"),
        "xg_conceded":         s.get("expectedGoalsConceeded"),
        "psxg":                s.get("xgSave"),
    }


def upsert_player(player_data: dict, club_id: int, role: str) -> int | None:
    p = player_data.get("player", player_data)
    sofascore_id  = p.get("id")
    name          = p.get("name")
    short_name    = p.get("shortName", name)
    position      = p.get("position", "")
    nationality   = p.get("country", {}).get("name", "") if isinstance(p.get("country"), dict) else ""

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
    print(f"🚀 Underlap scraper starting — season {SEASON_LABEL}\n")
    total_success, total_failed = 0, 0

    for club_name, team_id in CLUBS:
        print(f"\n🏟 {club_name} (team_id: {team_id})")

        # Get club_id from Supabase
        club = supabase.table("clubs").select("id").eq("name", club_name).single().execute()
        if not club.data:
            print(f"  ✗ Club not found in DB: {club_name}")
            continue
        club_id = club.data["id"]

        # Get squad from Sofascore
        squad = get_squad(team_id)
        if not squad:
            print(f"  ✗ No squad returned for {club_name}")
            continue

        print(f"  Found {len(squad)} players in squad")
        time.sleep(3)

        for entry in squad:
            p = entry.get("player", entry)
            pos_code = p.get("position", "")
            role = POSITION_MAP.get(pos_code)

            # Skip if position unknown
            if not role:
                continue

            name = p.get("name", "unknown")
            sofascore_id = p.get("id")
            print(f"  → {name} ({sofascore_id}) [{role}]")

            player_id = upsert_player(entry, club_id, role)
            if not player_id:
                total_failed += 1
                continue

            raw = get_player_stats(sofascore_id)
            if not raw:
                total_failed += 1
                time.sleep(2)
                continue

            stats = parse_stats(raw)
            stats["player_id"] = player_id
            stats["season"]    = SEASON_LABEL

            supabase.table("player_stats").upsert(
                stats, on_conflict="player_id,season"
            ).execute()

            print(f"    ✓ {stats.get('goals')}G {stats.get('assists')}A in {stats.get('appearances')} apps")
            total_success += 1
            time.sleep(2)

        # Log scrape for this club
        supabase.table("scrape_log").insert({
            "club_id":  club_id,
            "season":   SEASON_LABEL,
            "status":   "success",
            "notes":    f"{len(squad)} players in squad",
        }).execute()

    print(f"\n✅ Done — {total_success} success, {total_failed} failed")


if __name__ == "__main__":
    run()
