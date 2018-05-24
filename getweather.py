#! python

import os
import pandas as pd

BASEDIR = os.path.dirname(__file__)
WEATHERFILE = os.path.join(BASEDIR, 'onemin-WS_1-2017')
GROUNDFILE = os.path.join(BASEDIR, 'onemin-Ground-2017')
EASTERN_TZ = 'Etc/GMT+5'
# LATITUDE, LONGITUDE = 39.1374, -77.2187  # weather station
# LATITUDE, LONGITUDE = 39.1319, -77.2141  # ground array
HORIZON_ZENITH = 90.0  # degrees
GHI_THRESH = 0  # [W/m^2]

# read in data from NIST, parse times, and set them as the indices
ws_data = []
gnd_data = []

for month in range(12):
    ws_file = os.path.join(WEATHERFILE,
                           'onemin-WS_1-2017-{:02d}.csv'.format(month+1))
    gnd_file = os.path.join(GROUNDFILE,
                            'onemin-Ground-2017-{:02d}.csv'.format(month+1))
    ws_data.append(pd.read_csv(ws_file, index_col='TIMESTAMP',
                               parse_dates=True))
    gnd_data.append(pd.read_csv(gnd_file, index_col='TIMESTAMP',
                                parse_dates=True))
ws_data = pd.concat(ws_data)
gnd_data = pd.concat(gnd_data)

# fix timezone
ws_data.index = ws_data.index.tz_localize('UTC').tz_convert(EASTERN_TZ)
gnd_data.index = gnd_data.index.tz_localize('UTC').tz_convert(EASTERN_TZ)

# remove night time, when sun is below the horizon
nighttime = ws_data.SolarZenith_deg_Avg > HORIZON_ZENITH
# remove any times with negative power
outtages = (gnd_data.InvPAC_kW_Avg < 0) | (gnd_data.InvPDC_kW_Avg < 0)
# remove any times with negative irradiance
neg_irrad = (ws_data.Pyra1_Wm2_Avg < GHI_THRESH) | (ws_data.Pyrad1_Wm2_Avg < 0)
# apply all filters
ws_data = ws_data[~nighttime & ~outtages & ~neg_irrad]
gnd_data = gnd_data[~nighttime & ~outtages & ~neg_irrad]

# make new dataframe with desired fields and names
in_data = {'GHI': ws_data.Pyra1_Wm2_Avg,
           'DIF': ws_data.Pyrad1_Wm2_Avg,
           'Temp': ws_data.AirTemp_C_Avg,
           'WS': ws_data.WindSpeedAve_ms,
           'AZ': ws_data.SolarAzFromSouth_deg_Avg,
           'ZE': ws_data.SolarZenith_deg_Avg}
in_data = pd.DataFrame(in_data, index=ws_data.index)

# make a new dataframe for output data
out_data = {
    'GHI_GND': gnd_data.Pyra1_Wm2_Avg,
    'POA_GND': gnd_data.Pyra2_Wm2_Avg,
    'T_GND': gnd_data.AmbTemp_C_Avg,
    'PDC_GND': gnd_data.InvPDC_kW_Avg,
    'PAC_GND': gnd_data.InvPAC_kW_Avg,
    'VDC_GND': gnd_data.InvVDCin_Avg,
    'IDC_GND': gnd_data.InvIDCin_Avg,
    'VPV_GND': gnd_data.InvVPVin_Avg
}
out_data = pd.DataFrame(out_data, index=gnd_data.index)

# temporarily merge input and output data
data = pd.concat([in_data, out_data], axis=1)

# rollup data hourly
data = data.resample('H').mean()
data = data.dropna()  # remove dropouts
in_data = data[['GHI', 'DIF', 'Temp', 'WS']]

# recenter timestamps to center of interval from the beginning after resampling
timestamp = [ts.strftime('%Y-%m-%d %H:%M:%S') for ts in in_data.index]
in_data.index = timestamp
in_data.index.name = 'Date/Time'

# write input data to file
in_data.to_csv('NIST_weather_hourly.txt', sep='\t')

# save output data
data.index.name = 'TIMESTAMP'

# write input data to file
data.to_csv('NIST_ground_hourly.csv')
