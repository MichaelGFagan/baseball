import pandas
import pandas_gbq
import argparse
from pybaseball import batting_stats
from pybaseball import pitching_stats
from google.oauth2 import service_account

parser = argparse.ArgumentParser(description="Load Fangraphs data into Google BigQuery")
parser.add_argument('-s', '--start', type=int, default=1871, help="first year to extract from Retrosheet")
parser.add_argument('-e', '--end', type=int, default=2020, help="last year to extract from Retrosheet")

args = parser.parse_args()
start = args.start
end = args.end + 1
years = range(start, end)

credentials = service_account.Credentials.from_service_account_file(
    '../bigquery_credentials.json'
)
project_id = 'baseball-source'
sides = ['batting', 'pitching']

renamed_columns = {
    '1B': '_1B',
    '2B': '_2B',
    '3B': '_3B',
    'BB%': 'BB_pct',
    'K%': 'K_pct',
    'BB/K': 'BB_K',
    'GB/FB': 'GB_FB',
    'LD%': 'LD_pct',
    'GB%': 'GB_pct',
    'FB%': 'FB_pct',
    'IFFB%': 'IFFB_pct',
    'HR/FB': 'HR_FB',
    'IFH%': 'IFH_pct',
    'BUH%': 'BUH_pct',
    'wRC+': 'wRC_plus',
    '-WPA': 'minus_WPA',
    '+WPA': 'plus_WPA',
    'WPA/LI': 'WPA_LI',
    'FB% (Pitch)': 'FB_pct_pitch',
    'SL%': 'SL_pct',
    'CT%': 'CT_pct',
    'CB%': 'CB_pct',
    'CH%': 'CH_pct',
    'SF%': 'SF_pct',
    'KN%': 'KN_pct',
    'XX%': 'XX_pct',
    'PO%': 'PO_pct',
    'wFB/C': 'wFB_C',
    'wSL/C': 'wSL_C',
    'wCT/C': 'wCT_C',
    'wCB/C': 'wCB_C',
    'wCH/C': 'wCH_C',
    'wSF/C': 'wSF_C',
    'wKN/C': 'wKN_C',
    'O-Swing%': 'O_Swing_pct',
    'Z-Swing%': 'Z_Swing_pct',
    'Swing%': 'Swing_pct',
    'O-Contact%': 'O_Contact_pct',
    'Z-Contact%': 'Z_Contact_pct',
    'Contact%': 'Contact_pct',
    'Zone%': 'Zone_pct',
    'F-Strike%': 'F_Strike_pct',
    'SwStr%': 'SwStr_pct',
    'FA% (pfx)': 'FA_pct_pfx',
    'FT% (pfx)': 'FT_pct_pfx',
    'FC% (pfx)': 'FC_pct_pfx',
    'FS% (pfx)': 'FS_pct_pfx',
    'FO% (pfx)': 'FO_pct_pfx',
    'SI% (pfx)': 'SI_pct_pfx',
    'SL% (pfx)': 'SL_pct_pfx',
    'CU% (pfx)': 'CU_pct_pfx',
    'KC% (pfx)': 'KC_pct_pfx',
    'EP% (pfx)': 'EP_pct_pfx',
    'CH% (pfx)': 'CH_pct_pfx',
    'SC% (pfx)': 'SC_pct_pfx',
    'KN% (pfx)': 'KN_pct_pfx',
    'UN% (pfx)': 'UN_pct_pfx',
    'vFA (pfx)': 'vFA_pfx',
    'vFT (pfx)': 'vFT_pfx',
    'vFC (pfx)': 'vFC_pfx',
    'vFS (pfx)': 'vFS_pfx',
    'vFO (pfx)': 'vFO_pfx',
    'vSI (pfx)': 'vSI_pfx',
    'vSL (pfx)': 'vSL_pfx',
    'vCU (pfx)': 'vCU_pfx',
    'vKC (pfx)': 'vKC_pfx',
    'vEP (pfx)': 'vEP_pfx',
    'vCH (pfx)': 'vCH_pfx',
    'vSC (pfx)': 'vSC_pfx',
    'vKN (pfx)': 'vKN_pfx',
    'FA-X (pfx)': 'FA_X_pfx',
    'FT-X (pfx)': 'FT_X_pfx',
    'FC-X (pfx)': 'FC_X_pfx',
    'FS-X (pfx)': 'FS_X_pfx',
    'FO-X (pfx)': 'FO_X_pfx',
    'SI-X (pfx)': 'SI_X_pfx',
    'SL-X (pfx)': 'SL_X_pfx',
    'CU-X (pfx)': 'CU_X_pfx',
    'KC-X (pfx)': 'KC_X_pfx',
    'EP-X (pfx)': 'EP_X_pfx',
    'CH-X (pfx)': 'CH_X_pfx',
    'SC-X (pfx)': 'SC_X_pfx',
    'KN-X (pfx)': 'KN_X_pfx',
    'FA-Z (pfx)': 'FA_Z_pfx',
    'FT-Z (pfx)': 'FT_Z_pfx',
    'FC-Z (pfx)': 'FC_Z_pfx',
    'FS-Z (pfx)': 'FS_Z_pfx',
    'FO-Z (pfx)': 'FO_Z_pfx',
    'SI-Z (pfx)': 'SI_Z_pfx',
    'SL-Z (pfx)': 'SL_Z_pfx',
    'CU-Z (pfx)': 'CU_Z_pfx',
    'KC-Z (pfx)': 'KC_Z_pfx',
    'EP-Z (pfx)': 'EP_Z_pfx',
    'CH-Z (pfx)': 'CH_Z_pfx',
    'SC-Z (pfx)': 'SC_Z_pfx',
    'KN-Z (pfx)': 'KN_Z_pfx',
    'wFA (pfx)': 'wFA_pfx',
    'wFT (pfx)': 'wFT_pfx',
    'wFC (pfx)': 'wFC_pfx',
    'wFS (pfx)': 'wFS_pfx',
    'wFO (pfx)': 'wFO_pfx',
    'wSI (pfx)': 'wSI_pfx',
    'wSL (pfx)': 'wSL_pfx',
    'wCU (pfx)': 'wCU_pfx',
    'wKC (pfx)': 'wKC_pfx',
    'wEP (pfx)': 'wEP_pfx',
    'wCH (pfx)': 'wCH_pfx',
    'wSC (pfx)': 'wSC_pfx',
    'wKN (pfx)': 'wKN_pfx',
    'wFA/C (pfx)': 'wFA_C_pfx',
    'wFT/C (pfx)': 'wFT_C_pfx',
    'wFC/C (pfx)': 'wFC_C_pfx',
    'wFS/C (pfx)': 'wFS_C_pfx',
    'wFO/C (pfx)': 'wFO_C_pfx',
    'wSI/C (pfx)': 'wSI_C_pfx',
    'wSL/C (pfx)': 'wSL_C_pfx',
    'wCU/C (pfx)': 'wCU_C_pfx',
    'wKC/C (pfx)': 'wKC_C_pfx',
    'wEP/C (pfx)': 'wEP_C_pfx',
    'wCH/C (pfx)': 'wCH_C_pfx',
    'wSC/C (pfx)': 'wSC_C_pfx',
    'wKN/C (pfx)': 'wKN_C_pfx',
    'O-Swing% (pfx)': 'O_Swing_pct_pfx',
    'Z-Swing% (pfx)': 'Z_Swing_pct_pfx',
    'Swing% (pfx)': 'Swing_pct_pfx',
    'O-Contact% (pfx)': 'O_Contact_pct_pfx',
    'Z-Contact% (pfx)': 'Z_Contact_pct_pfx',
    'Contact% (pfx)': 'Contact_pct_pfx',
    'Zone% (pfx)': 'Zone_pct_pfx',
    'Pull%': 'Pull_pct',
    'Cent%': 'Cent_pct',
    'Oppo%': 'Oppo_pct',
    'Soft%': 'Soft_pct',
    'Med%': 'Med_pct',
    'Hard%': 'Hard_pct',
    'TTO%': 'TTO_pct',
    'CH% (pi)': 'CH_pct_pi',
    'CS% (pi)': 'CS_pct_pi',
    'CU% (pi)': 'CU_pct_pi',
    'FA% (pi)': 'FA_pct_pi',
    'FC% (pi)': 'FC_pct_pi',
    'FS% (pi)': 'FS_pct_pi',
    'KN% (pi)': 'KN_pct_pi',
    'SB% (pi)': 'SB_pct_pi',
    'SI% (pi)': 'SI_pct_pi',
    'SL% (pi)': 'SL_pct_pi',
    'XX% (pi)': 'XX_pct_pi',
    'vCH (pi)': 'vCH_pi',
    'vCS (pi)': 'vCS_pi',
    'vCU (pi)': 'vCU_pi',
    'vFA (pi)': 'vFA_pi',
    'vFC (pi)': 'vFC_pi',
    'vFS (pi)': 'vFS_pi',
    'vKN (pi)': 'vKN_pi',
    'vSB (pi)': 'vSB_pi',
    'vSI (pi)': 'vSI_pi',
    'vSL (pi)': 'vSL_pi',
    'vXX (pi)': 'vXX_pi',
    'CH-X (pi)': 'CH_X_pi',
    'CS-X (pi)': 'CS_X_pi',
    'CU-X (pi)': 'CU_X_pi',
    'FA-X (pi)': 'FA_X_pi',
    'FC-X (pi)': 'FC_X_pi',
    'FS-X (pi)': 'FS_X_pi',
    'KN-X (pi)': 'KN_X_pi',
    'SB-X (pi)': 'SB_X_pi',
    'SI-X (pi)': 'SI_X_pi',
    'SL-X (pi)': 'SL_X_pi',
    'XX-X (pi)': 'XX_X_pi',
    'CH-Z (pi)': 'CH_Z_pi',
    'CS-Z (pi)': 'CS_Z_pi',
    'CU-Z (pi)': 'CU_Z_pi',
    'FA-Z (pi)': 'FA_Z_pi',
    'FC-Z (pi)': 'FC_Z_pi',
    'FS-Z (pi)': 'FS_Z_pi',
    'KN-Z (pi)': 'KN_Z_pi',
    'SB-Z (pi)': 'SB_Z_pi',
    'SI-Z (pi)': 'SI_Z_pi',
    'SL-Z (pi)': 'SL_Z_pi',
    'XX-Z (pi)': 'XX_Z_pi',
    'wCH (pi)': 'wCH_pi',
    'wCS (pi)': 'wCS_pi',
    'wCU (pi)': 'wCU_pi',
    'wFA (pi)': 'wFA_pi',
    'wFC (pi)': 'wFC_pi',
    'wFS (pi)': 'wFS_pi',
    'wKN (pi)': 'wKN_pi',
    'wSB (pi)': 'wSB_pi',
    'wSI (pi)': 'wSI_pi',
    'wSL (pi)': 'wSL_pi',
    'wXX (pi)': 'wXX_pi',
    'wCH/C (pi)': 'wCH_C_pi',
    'wCS/C (pi)': 'wCS_C_pi',
    'wCU/C (pi)': 'wCU_C_pi',
    'wFA/C (pi)': 'wFA_C_pi',
    'wFC/C (pi)': 'wFC_C_pi',
    'wFS/C (pi)': 'wFS_C_pi',
    'wKN/C (pi)': 'wKN_C_pi',
    'wSB/C (pi)': 'wSB_C_pi',
    'wSI/C (pi)': 'wSI_C_pi',
    'wSL/C (pi)': 'wSL_C_pi',
    'wXX/C (pi)': 'wXX_C_pi',
    'O-Swing% (pi)': 'O_Swing_pct_pi',
    'Z-Swing% (pi)': 'Z_Swing_pct_pi',
    'Swing% (pi)': 'Swing_pct_pi',
    'O-Contact% (pi)': 'O_Contact_pct_pi',
    'Z-Contact% (pi)': 'Z_Contact_pct_pi',
    'Contact% (pi)': 'Contact_pct_pi',
    'Zone% (pi)': 'Zone_pct_pi',
    'Pace (pi)': 'Pace_pi',
    'AVG+': 'AVG_plus',
    'BB%+': 'BB_pct_plus',
    'K%+': 'K_pct_plus',
    'OBP+': 'OBP_plus',
    'SLG+': 'SLG_plus',
    'ISO+': 'ISO_plus',
    'BABIP+': 'BABIP_plus',
    'LD+%': 'LD_pct_plus',
    'LD%+': 'LD_pct_plus',
    'GB%+': 'GB_pct_plus',
    'FB%+': 'FB_pct_plus',
    'HR/FB%+': 'HR_FB_pct_plus',
    'Pull%+': 'Pull_pct_plus',
    'Cent%+': 'Cent_pct_plus',
    'Oppo%+': 'Oppo_pct_plus',
    'Soft%+': 'Soft_pct_plus',
    'Med%+': 'Med_pct_plus',
    'Hard%+': 'Hard_pct_plus',
    'Barrel%': 'Barrel_pct',
    'HardHit%': 'HardHit_pct',
    'K/9': 'K_9',
    'BB/9': 'BB_9',
    'K/BB': 'K_BB',
    'H/9': 'H_9',
    'HR/9': 'HR_9',
    'LOB%': 'LOB_pct',
    'Start-IP': 'Start_IP',
    'Relief-IP': 'Relief_IP',
    'FB% 2': 'FB_pct_2',
    'ERA-': 'ERA_minus',
    'FIP-': 'FIP_minus',
    'xFIP-': 'xFIP_minus',
    'RS/9': 'RS_9',
    'E-F': 'E_F',
    'RA9-WAR': 'RA9_WAR',
    'BIP-Wins': 'BIP_Wins',
    'LOB-Wins': 'LOB_Wins',
    'FDP-Wins': 'FDP_Wins',
    'Age Rng': 'Age_Rng',
    'K-BB%': 'K_BB_pct',
    'K/9+': 'K_9_plus',
    'BB/9+': 'BB_9_plus',
    'H/9+': 'H_9_plus',
    'HR/9+': 'HR_9_plus',
    'LOB%+': 'LOB_pct_plus',
    'WHIP+': 'WHIP_plus'
}

for year in years:
    for side in sides:
        table_name = side + '_' + str(year)
        table_id = "fangraphs." + table_name
        if side == 'batting':
            df = batting_stats(year)
        elif side == 'pitching':
            df = pitching_stats(year)
        df.rename(columns=renamed_columns, inplace=True)
        print('Loading ' + table_id)
        pandas_gbq.to_gbq(df, table_id, project_id=project_id, if_exists='replace')
        # try:
        #     if side == 'batting':
        #         df = batting_stats(year)
        #     elif side == 'pitching':
        #         df = pitching_stats(year)
        #     print('Loading ' + table_id)
        #     pandas_gbq.to_gbq(df, table_id, project_id=project_id, if_exists='replace')
        # except:
        #     pass