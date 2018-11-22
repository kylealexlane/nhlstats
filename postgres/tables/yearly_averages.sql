DROP TABLE IF EXISTS nhlstats.yearly_averages;

CREATE TABLE nhlstats.yearly_averages (
    year_start                     INT        NOT NULL,
    year_end                       INT        NOT NULL,
    playoff                        INT        NOT NULL,
    period_start                   TIMESTAMP,
    period_end                     TIMESTAMP,
    num_shots                      REAL,
    num_goals                      REAL,
    sum_xgoals                     REAL,
    avg_shoot_perc                 REAL,
    avg_pred                       REAL,
    wrist_shot_perc                REAL,
    backhand_perc                  REAL,
    slap_shot_perc                 REAL,
    snap_shot_perc                 REAL,
    tip_in_perc                    REAL,
    deflected_perc                REAL,
    wrap_around_perc              REAL,
    mean_dist                     REAL,
    std_dist                      REAL,
    mean_ang                      REAL,
    std_ang                       REAL,
    created                       TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY(year_start, year_end, playoff)
);
