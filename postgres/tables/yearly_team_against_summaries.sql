DROP TABLE IF EXISTS nhlstats.yearly_team_against_summaries;

CREATE TABLE nhlstats.yearly_team_against_summaries (
    id                            INT         NOT NULL,
    year_code                     INT         NOT NULL,
    month                         VARCHAR         NOT NULL,
    game_type                     VARCHAR         NOT NULL,
    num_goals                     REAL,
    wrist_shot_num                REAL,
    backhand_num                  REAL,
    slap_shot_num                 REAL,
    snap_shot_num                 REAL,
    tip_in_num                    REAL,
    deflected_num                 REAL,
    wrap_around_num               REAL,

    wrist_shot_pred               REAL,
    backhand_pred                 REAL,
    slap_shot_pred                REAL,
    snap_shot_pred                REAL,
    tip_in_pred                   REAL,
    deflected_pred                REAL,
    wrap_around_pred              REAL,

    mean_dist                     REAL,
    mean_ang                      REAL,

    sum_xgoals                    REAL,
    num_shots                     REAL,
    save_perc                     REAL,
    shot_quality                  REAL,

    wrist_shot_freq               REAL,
    backhand_freq                 REAL,
    slap_shot_freq                REAL,
    snap_shot_freq                REAL,
    tip_in_freq                   REAL,
    deflected_freq                REAL,
    wrap_around_freq              REAL,

    xsave_perc                    REAL,
    xsave_perc_wrist_shot         REAL,
    xsave_perc_backhand           REAL,
    xsave_perc_slap_shot          REAL,
    xsave_perc_snap_shot          REAL,
    xsave_perc_tip_in             REAL,
    xsave_perc_deflected          REAL,
    xsave_perc_wrap_around        REAL,

    saves_aa_per_shot             REAL,

    wrist_shot_save_perc          REAL,
    backhand_save_perc            REAL,
    slap_shot_save_perc           REAL,
    snap_shot_save_perc           REAL,
    tip_in_save_perc              REAL,
    deflected_save_perc           REAL,
    wrap_around_save_perc         REAL,

    save_perc_aa                  REAL,

    wrist_shot_freq_aa            REAL,
    backhand_freq_aa              REAL,
    slap_shot_freq_aa             REAL,
    snap_shot_freq_aa             REAL,
    tip_in_freq_aa                REAL,
    deflected_freq_aa             REAL,
    wrap_around_freq_aa           REAL,

    xsave_perc_aa                 REAL,
    xsave_perc_wrist_shot_aa      REAL,
    xsave_perc_backhand_aa        REAL,
    xsave_perc_slap_shot_aa       REAL,
    xsave_perc_snap_shot_aa       REAL,
    xsave_perc_tip_in_aa          REAL,
    xsave_perc_deflected_aa       REAL,
    xsave_perc_wrap_around_aa     REAL,

    wrist_shot_save_perc_aa       REAL,
    backhand_save_perc_aa         REAL,
    slap_shot_save_perc_aa        REAL,
    snap_shot_save_perc_aa        REAL,
    tip_in_save_perc_aa           REAL,
    deflected_save_perc_aa        REAL,
    wrap_around_save_perc_aa      REAL,

    created                       TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY(id, year_code, month, game_type)
);
