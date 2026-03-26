import os
import time
import requests
from supabase import create_client

# ── Config ────────────────────────────────────────────────────
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
SEASON_ID    = 61627  # Sofascore 2025/26 season ID
SEASON_LABEL = "2025/26"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Referer": "https://www.sofascore.com/",
}

# ── Players to scrape ─────────────────────────────────────────
# Format: (sofascore_player_id, name, short_name, club_name, role, position_detail)
PLAYERS = [
    # Arsenal
    (714077, "Bukayo Saka",         "Saka",        "Arsenal", "attacker",   "RW"),
    (981080, "Leandro Trossard",    "Trossard",    "Arsenal", "attacker",   "LW"),
    (939325, "Kai Havertz",         "Havertz",     "Arsenal", "attacker",   "CF"),
    (885819, "Gabriel Martinelli",  "Martinelli",  "Arsenal", "attacker",   "LW"),
    (wire := None),  # placeholder — expand below

    # Liverpool
    (159665, "Mohamed Salah",       "Salah",       "Liverpool", "attacker", "RW"),
    (838957, "Luis Diaz",           "L. Diaz",     "Liverpool", "attacker", "LW"),
    (958291, "Cody Gakpo",          "Gakpo",       "Liverpool", "attacker", "LW"),
    (870631, "Darwin Nunez",        "Nunez",       "Liverpool", "attacker", "CF"),

    # Chelsea
    (1107826, "Cole Palmer",        "Palmer",      "Chelsea",  "attacker",  "CAM"),
    (956209,  "Nicolas Jackson",    "Jackson",     "Chelsea",  "attacker",  "CF"),
    (1229669, "Jadon Sancho",       "Sancho",      "Chelsea",  "attacker",  "LW"),

    # Man City
    (389554,  "Erling Haaland",     "Haaland",     "Manchester City", "attacker", "CF"),
    (833015,  "Phil Foden",         "Foden",       "Manchester City", "attacker", "LW"),
    (714068,  "Jeremy Doku",        "Doku",        "Manchester City", "attacker", "LW"),

    # Real Madrid
    (697083,  "Vinicius Jr",        "Vinicius Jr", "Real Madrid", "attacker", "LW"),
    (875136,  "Kylian Mbappe",      "Mbappe",      "Real Madrid", "attacker", "CF"),
    (910836,  "Rodrygo",            "Rodrygo",     "Real Madrid", "attacker", "RW"),
]

# Remove None placeholders
PLAYERS = [p for p in PLAYERS if p is not None]


def get_player_stats(sofascore_id: int) -> dict | None:
    url = f"https://api.sofascore.com/api/v1/player/{sofascore_id}/statistics/season/{SEASON_ID}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code == 200:
            return r.json()
        print(f"  ⚠ Status {r.status_code} for player {sofascore_id}")
        return None
    except Exception as e:
        print(f"  ✗ Error fetching {sofascore_id}: {e}")
        return None


def parse_stats(raw: dict) -> dict:
    s = raw.get("statistics", {})
    return {
        "appearances":          s.get("appearances"),
        "minutes_played":       s.get("minutesPlayed"),
        "goals":                s.get("goals"),
        "assists":              s.get("assists"),
        "xg":                   s.get("expectedGoals"),
        "xa":                   s.get("expectedAssists"),
        "shots_total":          s.get("totalShots"),
        "shots_on_target":      s.get("shotsOnTarget"),
        "key_passes":           s.get("keyPasses"),
        "big_chances_created":  s.get("bigChancesCreated"),
        "dribbles_completed":   s.get("successfulDribbles"),
        "fouls_won":            s.get("fouledTotal"),
        "passes_total":         s.get("totalPasses"),
        "pass_accuracy_pct":    s.get("accuratePasses"),
        "forward_passes":       s.get("forwardPasses"),
        "long_balls":           s.get("accurateLongBalls"),
        "through_balls":        s.get("accurateThroughBalls"),
        "crosses":              s.get("totalCross"),
        "chances_created":      s.get("chancesCreated"),
        "tackles":              s.get("tackles"),
        "interceptions":        s.get("interceptions"),
        "duels_won":            s.get("duelsWon"),
        "duels_total":          s.get("duelsTotal"),
        "ground_duels_won":     s.get("groundDuelsWon"),
        "ground_duels_total":   s.get("groundDuelsTotal"),
        "aerial_duels_won":     s.get("aerialDuelsWon"),
        "aerial_duels_total":   s.get("aerialDuelsTotal"),
        "fouls_committed":      s.get("foulsCommitted"),
        "ball_recoveries":      s.get("ballRecoveries"),
        "dribbles_past":        s.get("dribbledPast"),
        "progressive_carries":  s.get("progressiveCarries"),
        "blocks":               s.get("blockedShots"),
        "clearances":           s.get("clearances"),
        "errors_leading_goal":  s.get("errorLeadToGoal"),
        "penalties_conceded":   s.get("penaltyConceded"),
        "clean_sheets":         s.get("cleanSheets"),
        "saves":                s.get("saves"),
        "goals_conceded":       s.get("goalsConceded"),
        "penalties_saved":      s.get("penaltySaves"),
        "sweeper_clearances":   s.get("punches"),
        "catches":              s.get("runsOut"),
        "crosses_against":      s.get("crossesTotal"),
        "xg_conceded":          s.get("expectedGoalsConceeded"),
        "psxg":                 s.get("xgSave"),
    }


def upsert_player(p: tuple) -> int | None:
    sofascore_id, name, short_name, club_name, role, position = p

    # Get club_id
    club = supabase.table("clubs").select("id").eq("name", club_name).single().execute()
    if not club.data:
        print(f"  ✗ Club not found: {club_name}")
        return None
    club_id = club.data["id"]

    # Upsert player
    result = supabase.table("players").upsert({
        "sofascore_id":    sofascore_id,
        "name":            name,
        "short_name":      short_name,
        "club_id":         club_id,
        "role":            role,
        "position_detail": position,
        "is_active":       True,
    }, on_conflict="sofascore_id").execute()

    # Get player id
    player = supabase.table("players").select("id").eq("sofascore_id", sofascore_id).single().execute()
    return player.data["id"] if player.data else None


def run():
    print(f"🚀 Underlap scraper starting — season {SEASON_LABEL}\n")
    success, failed = 0, 0

    for p in PLAYERS:
        sofascore_id, name = p[0], p[1]
        print(f"→ {name} ({sofascore_id})")

        player_id = upsert_player(p)
        if not player_id:
            failed += 1
            continue

        raw = get_player_stats(sofascore_id)
        if not raw:
            failed += 1
            continue

        stats = parse_stats(raw)
        stats["player_id"] = player_id
        stats["season"]    = SEASON_LABEL

        supabase.table("player_stats").upsert(
            stats, on_conflict="player_id,season"
        ).execute()

        print(f"  ✓ Saved — {stats.get('goals')}G {stats.get('assists')}A in {stats.get('appearances')} apps")
        success += 1
        time.sleep(2)  # be polite to Sofascore

    print(f"\n✅ Done — {success} success, {failed} failed")


if __name__ == "__main__":
    run()
