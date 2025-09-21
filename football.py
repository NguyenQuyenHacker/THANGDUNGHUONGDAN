import numpy as np
import pandas as pd
from util import logger, dwh


def get_team_form():
    # Bảng chứa dữ liệu giả định
    TBL_MATCHES = "stg_football_matches"
    TBL_TEAMS = "stg_football_teams"

    # Query dữ liệu trận đấu
    df_matches = dwh.query(f"""
    SELECT match_date, home_team, away_team, home_goals, away_goals
    FROM staging.{TBL_MATCHES}
    WHERE match_date >= '2020-01-01'
    ORDER BY match_date
    """).astype({'match_date': 'datetime64'})

    # Query danh sách đội bóng
    df_teams = dwh.query(f"""
    SELECT team_id, team_name, country
    FROM staging.{TBL_TEAMS}
    """)

    # Tính số bàn thắng, thua và hiệu số
    df_home = df_matches.assign(team=lambda d: d.home_team, goals=lambda d: d.home_goals,
                                conceded=lambda d: d.away_goals)
    df_away = df_matches.assign(team=lambda d: d.away_team, goals=lambda d: d.away_goals,
                                conceded=lambda d: d.home_goals)

    df_all = (
        pd.concat([df_home[['match_date', 'team', 'goals', 'conceded']],
                   df_away[['match_date', 'team', 'goals', 'conceded']]])
        .sort_values('match_date')
    )

    # Tính phong độ: trung bình bàn thắng 5 trận gần nhất
    df_all['avg_goals_5'] = df_all.groupby('team')['goals'].rolling(5, closed='left').mean().reset_index(0, drop=True)

    # Hiệu số bàn thắng lũy kế
    df_all['goal_diff'] = df_all.goals - df_all.conceded
    df_all['cum_goal_diff'] = df_all.groupby('team')['goal_diff'].cumsum()

    # Gộp với bảng đội bóng để lấy thêm thông tin
    df_final = df_all.merge(df_teams, left_on='team', right_on='team_id', how='left')

    # Thêm cột thời gian tạo dữ liệu
    df_final['createddatetime'] = pd.Timestamp.now()

    return df_final[['match_date', 'team', 'team_name', 'country', 'goals', 'conceded', 'avg_goals_5', 'cum_goal_diff',
                     'createddatetime']]
