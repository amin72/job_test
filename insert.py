import csv
import os
from tortoise import Tortoise, fields, run_async
from tortoise.models import Model


class Information(Model):
    id = fields.IntField(pk=True)
    altitude = fields.CharField(max_length=255, null=True)
    anemometer_alive = fields.BooleanField(null=True)
    awdir_geo = fields.CharField(max_length=255, null=True)
    aws_internet = fields.CharField(max_length=255, null=True)
    aws_q_size = fields.CharField(max_length=255, null=True)
    aws_sent_payloads = fields.CharField(max_length=255, null=True)
    aws_timeouts = fields.CharField(max_length=255, null=True)
    aws_uprate = fields.CharField(max_length=255, null=True)
    ch4_advection = fields.CharField(max_length=255, null=True)
    ch4_alive = fields.BooleanField(null=True)
    ch4_ch4d = fields.CharField(max_length=255, null=True)
    ch4_diag = fields.CharField(max_length=255, null=True)
    ch4_droprate = fields.CharField(max_length=255, null=True)
    ch4_epochtime = fields.CharField(max_length=255, null=True)
    ch4_errorcode = fields.CharField(max_length=255, null=True)
    ch4_milliseconds = fields.CharField(max_length=255, null=True)
    ch4_moleratio = fields.CharField(max_length=255, null=True)
    ch4_mr_anomaly = fields.CharField(max_length=255, null=True)
    ch4_mr_delayed = fields.CharField(max_length=255, null=True)
    ch4_mr_mean = fields.CharField(max_length=255, null=True)
    ch4_mr_median = fields.CharField(max_length=255, null=True)
    ch4_mr_sd = fields.CharField(max_length=255, null=True)
    ch4_nanoseconds = fields.CharField(max_length=255, null=True)
    ch4_plume = fields.CharField(max_length=255, null=True)
    ch4_pressure = fields.CharField(max_length=255, null=True)
    ch4_qc = fields.CharField(max_length=255, null=True)
    ch4_recordtime = fields.DatetimeField(null=True)
    ch4_rssi = fields.CharField(max_length=255, null=True)
    ch4_rssi_sd = fields.CharField(max_length=255, null=True)
    ch4_temp = fields.CharField(max_length=255, null=True)
    closest_fenced_site = fields.CharField(max_length=255, null=True)
    computer_time = fields.DatetimeField(null=True)
    dead_reckoning = fields.CharField(max_length=255, null=True)
    dt_logger_alive = fields.BooleanField(null=True)
    estimated_heading = fields.CharField(max_length=255, null=True)
    fence_status = fields.BooleanField(null=True)
    frame = fields.CharField(max_length=255, null=True)
    geo_sep = fields.CharField(max_length=255, null=True)
    gps_alive = fields.BooleanField(null=True)
    gps_fix = fields.BooleanField(null=True)
    gps_time_ori = fields.CharField(max_length=255, null=True)
    gps_time_pos = fields.CharField(max_length=255, null=True)
    heading = fields.CharField(max_length=255, null=True)
    heading_available = fields.CharField(max_length=255, null=True)
    horizontal_dil = fields.CharField(max_length=255, null=True)
    hpr_type = fields.CharField(max_length=1, null=True)
    latest_gps_update = fields.DatetimeField(null=True)
    latitude = fields.FloatField()
    lgr_alive = fields.BooleanField(null=True)
    log_time = fields.CharField(max_length=255, null=True)
    longitude = fields.FloatField(null=True)
    note = fields.TextField(null=True)
    num_sats = fields.CharField(max_length=255, null=True)
    par_frame = fields.CharField(max_length=255, null=True)
    pitch = fields.CharField(max_length=255, null=True)
    public_IP = fields.CharField(max_length=15, null=True)
    ref_station_id = fields.CharField(max_length=255, null=True)
    rm_young_86000_alive = fields.BooleanField(null=True)
    rmys_record_time = fields.DatetimeField(null=True)
    rmys_sonictemp = fields.CharField(max_length=255, null=True)
    rmys_status = fields.CharField(max_length=255, null=True)
    rmys_wdir_cf = fields.CharField(max_length=255, null=True)
    rmys_wspd2D_cf = fields.CharField(max_length=255, null=True)
    rmys_wspd3D_cf = fields.CharField(max_length=255, null=True)
    roll = fields.CharField(max_length=255, null=True)
    spd_over_grnd = fields.CharField(max_length=255, null=True)
    speed_mean = fields.CharField(max_length=255, null=True)
    status_nominal = fields.BooleanField(null=True)
    system_id = fields.CharField(max_length=255, null=True)
    track_mean = fields.CharField(max_length=255, null=True)
    track_turn_rate = fields.CharField(max_length=255, null=True)
    track_turn_rate_filtered = fields.CharField(max_length=255, null=True)
    travel_az = fields.CharField(max_length=255, null=True)
    travel_speed = fields.CharField(max_length=255, null=True)
    true_track = fields.CharField(max_length=255, null=True)
    true_wdir = fields.CharField(max_length=255, null=True)
    true_wspd = fields.CharField(max_length=255, null=True)
    utm_zone = fields.CharField(max_length=255, null=True)
    wdir_cf_corrected = fields.CharField(max_length=255, null=True)
    wspd_cf_corrected = fields.CharField(max_length=255, null=True)
    x_utm = fields.CharField(max_length=255, null=True)
    y_utm = fields.CharField(max_length=255, null=True)

    class Meta:
        table = "information"

    def __str__(self):
        return f'({self.latitude}, {self.longitude})'


async def run():
    # connect to postgres
    # HINT: read username, password and other database information from enviroment
    await Tortoise.init(
      db_url="postgres://test_user:testpassword@localhost:5432/test_db",
      modules={"models": ["__main__"]}
    )
    
    # create tables
    await Tortoise.generate_schemas()

    path = '80/' # hard coded folder name
    total_record_inserted = 0
    total_record_ignored = 0

    # loop over files
    for filename in os.listdir(path):
        print(f'[*] processing file: {filename}')
    
        # open csv file for processing
        with open(f'{path}{filename}') as csvfile:
            reader = csv.DictReader(csvfile, delimiter=',')
    
            record_count_inserted = 0
            record_count_ignored = 0
    
            for row in reader:
                latitude = row['latitude']
                longitude = row['longitude']
    
                if latitude and longitude:
                    # latitude and longitude attributes are set
                    # insert data
                    try:
                        await Information.create(**row)
                    except Exception as e:
                        print(row)
                        print()
                        raise
                    record_count_inserted += 1
                else:
                    # latitude and longitude attributes are not set
                    record_count_ignored += 1
    
            # total processed records by now
            total_record_inserted += record_count_inserted
            total_record_ignored += record_count_ignored
    
            print(f'[*] processing file: {filename} done.')
            print(f'[+] {record_count_inserted} inserted.')
            print(f'[-] {record_count_ignored} ignored.')
            print()
    
    print(f'[+] {total_record_inserted} records inserted')
    print(f'[-] {total_record_ignored} records ignored')


if __name__ == "__main__":
    run_async(run())

