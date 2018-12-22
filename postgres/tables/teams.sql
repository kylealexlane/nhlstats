DROP TABLE IF EXISTS nhlstats.teams;

CREATE TABLE nhlstats.teams (
    id                  INT            NOT NULL,
    name                VARCHAR,
    link                VARCHAR,
    venue               INT,
    abbreviation        VARCHAR,
    team_name           VARCHAR,
    location_name       VARCHAR,
    first_year_of_play  VARCHAR,
    division            INT,
    conference          INT,
    franchise           INT,
    short_name          VARCHAR,
    official_site_url   VARCHAR,
    franchise_id        INT,
    active              BOOL,
    venue_name          VARCHAR,
    division_name       VARCHAR,
    conference_name     VARCHAR,
    franchise_name      VARCHAR,
    created             TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY(id)
);
