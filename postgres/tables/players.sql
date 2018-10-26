/*
  All the play events in a game
 */
DROP TABLE IF EXISTS nhlstats.players;

CREATE TABLE nhlstats.players (
    id                    INT            NOT NULL,
    full_name             VARCHAR,
    link                  VARCHAR,
    first_name            VARCHAR,
    last_name             VARCHAR,
    primary_number        INT,
    birth_date            VARCHAR,
    current_age           INT,
    birth_city            VARCHAR,
    birth_state_province  VARCHAR,
    birth_country         VARCHAR,
    nationality           VARCHAR,
    height                VARCHAR,
    weight                INT,
    active                BOOL,
    alternateCaptain      BOOL,
    captain               BOOL,
    rookie                BOOL,
    shootsCatches         VARCHAR,
    rosterStatus          VARCHAR,
    team_id               INT,
    team_name             VARCHAR,
    team_link             VARCHAR,
    pos_code              VARCHAR,
    pos_name              VARCHAR,
    pos_type              VARCHAR,
    pos_abbr              VARCHAR,
    created               TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY(id)
);
