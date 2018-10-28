DROP TABLE IF EXISTS nhlstats.yearly_goalie_summaries;

CREATE TABLE nhlstats.yearly_goalie_summaries (
    id                            INT            NOT NULL,
    year                          VARCHAR        NOT NULL,
    playoff                       INT      NOT NULL,
    num_shots_against             REAL,
    pred_goals_against            REAL,
    pred_save_perc                REAL,
    act_goals_against             REAL,
    act_save_perc                 REAL,
    saves_added_per_shot          REAL,
    created                       TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY(id, year, playoff)
);
