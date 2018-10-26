/*
  All the play events in a game
 */
DROP TABLE IF EXISTS nhlstats.allplays;

CREATE TABLE nhlstats.allplays (
    game_id              INT            NOT NULL,
    game_date            TIMESTAMP,
    event_idx            INT            NOT NULL,
    event_id             INT,
    period              INT,
    period_type          VARCHAR,
    ordinal_num          VARCHAR,
    period_time          VARCHAR,
    period_time_remaining VARCHAR,
    away_goals           INT,
    home_goals           INT,
    x_coords             INT,
    y_coords             INT,
    team_id              INT,
    team_name            VARCHAR,
    team_tri_code         VARCHAR,
    player1_id           INT,
    player1_name     VARCHAR,
    player1_type         VARCHAR,
    player2_id           INT,
    player2_name     VARCHAR,
    player2_type          VARCHAR,
    event_type           VARCHAR,
    event_code           VARCHAR,
    event_type_id         VARCHAR,
    event_desc           VARCHAR,
    event_second_type     VARCHAR,
    created             TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY(game_id, event_idx, event_id)
);