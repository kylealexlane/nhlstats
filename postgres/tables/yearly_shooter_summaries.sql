DROP TABLE IF EXISTS nhlstats.yearly_shooter_summaries;

CREATE TABLE nhlstats.yearly_shooter_summaries (
    id                            INT         NOT NULL,
    year_start                    INT         NOT NULL,
    year_end                      INT         NOT NULL,
    playoff                       INT         NOT NULL,
    num_shots                     REAL,
    act_goals                     REAL,
    pred_goals                    REAL,
    act_shoot_perc                REAL,
    pred_shoot_perc               REAL,
    shoot_skill                   REAL,
    created                       TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY(id, year_start, year_end, playoff)
);
