DROP TABLE IF EXISTS nhlstats.yearly_shooter_ranks;

CREATE TABLE nhlstats.yearly_shooter_ranks (
    id                                  INT         NOT NULL,
    year_code                           INT         NOT NULL,
    month                               VARCHAR         NOT NULL,
    game_type                           VARCHAR         NOT NULL,

    num_goals_pctile                     REAL,
    wrist_shot_num_pctile                REAL,
    backhand_num_pctile                  REAL,
    slap_shot_num_pctile                 REAL,
    snap_shot_num_pctile                 REAL,
    tip_in_num_pctile                    REAL,
    deflected_num_pctile                 REAL,
    wrap_around_num_pctile               REAL,

    wrist_shot_pred_pctile               REAL,
    backhand_pred_pctile                 REAL,
    slap_shot_pred_pctile                REAL,
    snap_shot_pred_pctile                REAL,
    tip_in_pred_pctile                   REAL,
    deflected_pred_pctile                REAL,
    wrap_around_pred_pctile              REAL,

    mean_dist_pctile                     REAL,
    mean_ang_pctile                      REAL,

    sum_xgoals_pctile                    REAL,
    num_shots_pctile                     REAL,
    avg_shoot_perc_pctile                REAL,
    shot_quality_pctile                  REAL,

    wrist_shot_freq_pctile               REAL,
    backhand_freq_pctile                 REAL,
    slap_shot_freq_pctile                REAL,
    snap_shot_freq_pctile                REAL,
    tip_in_freq_pctile                   REAL,
    deflected_freq_pctile                REAL,
    wrap_around_freq_pctile              REAL,

    avg_xgoals_pctile                    REAL,
    avg_xgoals_wrist_shot_pctile         REAL,
    avg_xgoals_backhand_pctile           REAL,
    avg_xgoals_slap_shot_pctile          REAL,
    avg_xgoals_snap_shot_pctile          REAL,
    avg_xgoals_tip_in_pctile             REAL,
    avg_xgoals_deflected_pctile          REAL,
    avg_xgoals_wrap_around_pctile        REAL,

    goals_aa_per_shot_pctile          REAL,

    wrist_shot_shooting_perc_pctile      REAL,
    backhand_shooting_perc_pctile        REAL,
    slap_shot_shooting_perc_pctile       REAL,
    snap_shot_shooting_perc_pctile       REAL,
    tip_in_shooting_perc_pctile          REAL,
    deflected_shooting_perc_pctile       REAL,
    wrap_around_shooting_perc_pctile     REAL,

    avg_shoot_perc_aa_pctile             REAL,

    wrist_shot_freq_aa_pctile            REAL,
    backhand_freq_aa_pctile              REAL,
    slap_shot_freq_aa_pctile             REAL,
    snap_shot_freq_aa_pctile             REAL,
    tip_in_freq_aa_pctile                REAL,
    deflected_freq_aa_pctile             REAL,
    wrap_around_freq_aa_pctile           REAL,

    avg_xgoals_aa_pctile                 REAL,
    avg_xgoals_wrist_shot_aa_pctile      REAL,
    avg_xgoals_backhand_aa_pctile        REAL,
    avg_xgoals_slap_shot_aa_pctile       REAL,
    avg_xgoals_snap_shot_aa_pctile       REAL,
    avg_xgoals_tip_in_aa_pctile          REAL,
    avg_xgoals_deflected_aa_pctile       REAL,
    avg_xgoals_wrap_around_aa_pctile     REAL,

    wrist_shot_shooting_perc_aa_pctile   REAL,
    backhand_shooting_perc_aa_pctile     REAL,
    slap_shot_shooting_perc_aa_pctile    REAL,
    snap_shot_shooting_perc_aa_pctile    REAL,
    tip_in_shooting_perc_aa_pctile       REAL,
    deflected_shooting_perc_aa_pctile    REAL,
    wrap_around_shooting_perc_aa_pctile  REAL,


    num_goals_rank                     REAL,
    wrist_shot_num_rank                REAL,
    backhand_num_rank                  REAL,
    slap_shot_num_rank                 REAL,
    snap_shot_num_rank                 REAL,
    tip_in_num_rank                    REAL,
    deflected_num_rank                 REAL,
    wrap_around_num_rank               REAL,

    wrist_shot_pred_rank               REAL,
    backhand_pred_rank                 REAL,
    slap_shot_pred_rank                REAL,
    snap_shot_pred_rank                REAL,
    tip_in_pred_rank                   REAL,
    deflected_pred_rank                REAL,
    wrap_around_pred_rank              REAL,

    mean_dist_rank                     REAL,
    mean_ang_rank                      REAL,

    sum_xgoals_rank                    REAL,
    num_shots_rank                     REAL,
    avg_shoot_perc_rank                REAL,
    shot_quality_rank                  REAL,

    wrist_shot_freq_rank               REAL,
    backhand_freq_rank                 REAL,
    slap_shot_freq_rank                REAL,
    snap_shot_freq_rank                REAL,
    tip_in_freq_rank                   REAL,
    deflected_freq_rank                REAL,
    wrap_around_freq_rank              REAL,

    avg_xgoals_rank                    REAL,
    avg_xgoals_wrist_shot_rank         REAL,
    avg_xgoals_backhand_rank           REAL,
    avg_xgoals_slap_shot_rank          REAL,
    avg_xgoals_snap_shot_rank          REAL,
    avg_xgoals_tip_in_rank             REAL,
    avg_xgoals_deflected_rank          REAL,
    avg_xgoals_wrap_around_rank        REAL,

    goals_aa_per_shot_rank             REAL,

    wrist_shot_shooting_perc_rank      REAL,
    backhand_shooting_perc_rank        REAL,
    slap_shot_shooting_perc_rank       REAL,
    snap_shot_shooting_perc_rank       REAL,
    tip_in_shooting_perc_rank          REAL,
    deflected_shooting_perc_rank       REAL,
    wrap_around_shooting_perc_rank     REAL,

    avg_shoot_perc_aa_rank             REAL,

    wrist_shot_freq_aa_rank            REAL,
    backhand_freq_aa_rank              REAL,
    slap_shot_freq_aa_rank             REAL,
    snap_shot_freq_aa_rank             REAL,
    tip_in_freq_aa_rank                REAL,
    deflected_freq_aa_rank             REAL,
    wrap_around_freq_aa_rank           REAL,

    avg_xgoals_aa_rank                 REAL,
    avg_xgoals_wrist_shot_aa_rank      REAL,
    avg_xgoals_backhand_aa_rank        REAL,
    avg_xgoals_slap_shot_aa_rank       REAL,
    avg_xgoals_snap_shot_aa_rank       REAL,
    avg_xgoals_tip_in_aa_rank          REAL,
    avg_xgoals_deflected_aa_rank       REAL,
    avg_xgoals_wrap_around_aa_rank     REAL,

    wrist_shot_shooting_perc_aa_rank   REAL,
    backhand_shooting_perc_aa_rank     REAL,
    slap_shot_shooting_perc_aa_rank    REAL,
    snap_shot_shooting_perc_aa_rank    REAL,
    tip_in_shooting_perc_aa_rank       REAL,
    deflected_shooting_perc_aa_rank    REAL,
    wrap_around_shooting_perc_aa_rank  REAL,

    created                       TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY(id, year_code, month, game_type)
);




