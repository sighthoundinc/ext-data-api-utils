import argparse
import datetime
import json
from time import time

import requests
import dateutil.parser
import dateutil.tz
import pprint
import textwrap
import sys, os, subprocess
import re
from google.cloud import storage
import google.auth
import ffmpeg
import shutil
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import warnings
warnings.filterwarnings("ignore", "Your application has authenticated using end user credentials")

VIDEO_LENTH_MINUTES = 5
SECONDS_BEFORE_EVENT = 10
SECONDS_AFTER_EVENT = 5

def query_flat(args, sensors):
    url = 'https://data-api.boulderai.com/data/sensor/query'
    headers = {'Content-type': 'application/json', 'X-API-Key': f'{args.key}'}
    data = {'deviceId': f'{args.deviceId}',
            'sensors': [f'{sensors}'],
            'startTime': f'{args.startTime}',
            'endTime': f'{args.endTime}'}
    print(f'Issuing curl "{url}" -d \'{{"deviceId":"{args.deviceId}","sensors":["{sensors}"],'
          f'"startTime":"{args.startTime}","endTime":"{args.endTime}"}}\' \\\n'
          f'-X POST \\\n'
          f'-H "Content-Type: application/json" \\\n'
          f'-H "X-API-KEY: {args.key}"')
    r = requests.post(url, data=json.dumps(data), verify=False, headers=headers)
    r.raise_for_status()
    return r.json()


def time_parse(args, parser):
    format_str = "%Y-%m-%dT%H:%M:%S.000Z"
    if args.startTime is not None:
        passed_start_time = args.startTime
        start_date = dateutil.parser.parse(args.startTime)
        args.startTime = start_date.astimezone(dateutil.tz.UTC).strftime(format_str)
        print(f'converted start time {passed_start_time} to UTC time value {args.startTime}')
    if args.endTime is not None:
        passed_end_time = args.endTime
        end_date = dateutil.parser.parse(args.endTime)
        args.endTime = end_date.astimezone(dateutil.tz.UTC).strftime(format_str)
        print(f'converted end time {passed_end_time} to UTC time value {args.endTime}')
    else:
        args.endTime = datetime.datetime.now().astimezone(dateutil.tz.UTC).strftime(format_str)
        end_date = dateutil.parser.parse(args.endTime)
        print(f'endTime not specified, using time now ({args.endTime})')
    if args.lastDays is not None:
        args.startTime = (end_date - datetime.timedelta(days=args.lastDays)).strftime(format_str)
        print(f'lastDays {args.lastDays} specified, used this to set startTime to {args.startTime}')
    if args.lastHours is not None:
        args.startTime = (end_date - datetime.timedelta(hours=args.lastHours)).strftime(format_str)
        print(f'lastHours {args.lastHours} specified, used this to set startTime to {args.startTime}')
    if args.lastMinutes is not None:
        args.startTime = (end_date - datetime.timedelta(minutes=args.lastMinutes)).strftime(format_str)
        print(f'lastMinutes {args.lastMinutes} specified, used this to set startTime to {args.startTime}')


    if args.startTime is None or args.endTime is None:
        print(f'Time range not specified for query')
        parser.print_help()
        raise ValueError('Invalid arguments')

def findVideo(gcp_client, args, time):
    bucket = None
    basePath = None
    prefix = ''
    if args.sourceGCPpath:
        bucket = args.sourceGCPpath.split("/")[0]
        basePath = "/".join(args.sourceGCPpath.split("/")[1:])[1:]
        if len(basePath) == 0:
            prefix = f"{args.deviceId}/data_acq_video/" + time.strftime("%Y-%m-%d") + "/"
        else:
            prefix = f"{basePath}/{args.deviceId}/data_acq_video/" + time.strftime("%Y-%m-%d") + "/"
    else:
        bucket = "bai-rawdata"
        basePath = "gcpbai"
        prefix = f"{basePath}/{args.deviceId}/" + time.strftime("%Y-%m-%d") + "/"
    # google cloud sdk doesn't like double //
    prefix = prefix.replace("//","/")
    blobs = gcp_client.list_blobs(bucket, prefix=prefix)
    format_str = "DataAcqVideo_%Y-%m-%d-%H-%M-%S.%f"
    after_time = time - datetime.timedelta(minutes=VIDEO_LENTH_MINUTES)
    # search for matching video
    for blob in blobs:
        video_name = re.search("DataAcqVideo_.*mp4", blob.name)
        if video_name:
            video_time = datetime.datetime.strptime(video_name.group(0).replace(".mp4", "000"), format_str).replace(tzinfo=dateutil.tz.UTC)
            if video_time > after_time and video_time < time:
                return blob
    return False

def trim(start,end,input,output):
    (
        ffmpeg
        .input(input)
        .trim(start=start, end=end)
        .output(output)
        .overwrite_output()
        .run(quiet=True)
    )
            
def downloadClip(gcp_client, args, event, video_blob):
    tmp_filename = args.output + "/tmp/" + video_blob.name.split('/')[-1] 
    # download file if it doesn't exist already
    if not os.path.isfile(tmp_filename):
        # delete last tmp file if we're not using it
        if os.path.isdir(args.output + "/tmp"):
            shutil.rmtree(args.output + "/tmp/")
        os.mkdir(args.output + "/tmp/")
        with open(tmp_filename, "+w"):
            video_blob.download_to_filename(tmp_filename)
    # find cooresponding time in video
    event_time = dateutil.parser.parse(event['timeCollected']).astimezone(dateutil.tz.UTC)
    video_name = re.search("DataAcqVideo_.*mp4", video_blob.name)
    format_str = "DataAcqVideo_%Y-%m-%d-%H-%M-%S.%f"
    video_time = datetime.datetime.strptime(video_name.group(0).replace(".mp4", "000"), format_str).replace(tzinfo=dateutil.tz.UTC)
    video_relative_time = event_time - video_time
    format_str = "%H:%M:%S"
    start_time = str(video_relative_time - datetime.timedelta(seconds=SECONDS_BEFORE_EVENT))
    end_time = str(video_relative_time + datetime.timedelta(seconds=SECONDS_AFTER_EVENT))
    # make sure we're not going out of bounds
    if datetime.timedelta(seconds=SECONDS_BEFORE_EVENT) > video_relative_time:
        start_time = "00:00:00.000"
    if (video_relative_time + datetime.timedelta(seconds=SECONDS_AFTER_EVENT)) > datetime.timedelta(seconds=VIDEO_LENTH_MINUTES*60):
        end_time = f"00:0{VIDEO_LENTH_MINUTES}:00"
    # trim the video using ffmpeg
    output_filename = args.output + "/" + event['id'] + ".mp4"
    trim(start_time, end_time, tmp_filename, output_filename)
    return output_filename

# add start time to event list    
def addStartTime(eventList):
    format_str = "%Y-%m-%dT%H:%M:%S.%f"
    for event in eventList:
        if not event['value']:
            print(f"ERROR: Event {event['id']} does not have value content")
            sys.exit(1)
        timedelta_from_value = datetime.timedelta(seconds=event["value"])
        event_time = dateutil.parser.parse(event['timeCollected']).astimezone(dateutil.tz.UTC)
        event["startTime"] = (event_time - timedelta_from_value).strftime(format_str)

# find event with closest timestamp to provided timestamp
def findClosest(timestamp, event_list):
    event_time = dateutil.parser.parse(timestamp).astimezone(dateutil.tz.UTC)
    closest = None
    before = True
    for potential_event in event_list:
        # find difference in time
        potential_event_time = dateutil.parser.parse(potential_event['timeCollected']).astimezone(dateutil.tz.UTC)
        if re.match(r"(COLLISION_[0-9]*)|(PRESENCE_.*_[0-9]*)", potential_event['sensorName']):
            potential_event_time = dateutil.parser.parse(potential_event['startTime']).astimezone(dateutil.tz.UTC)
        time_difference = None
        if potential_event_time < event_time:
            before = True
            time_difference = event_time - potential_event_time
        else: 
            before = False
            time_difference = potential_event_time - event_time

        if not closest:
            if before == True:
                closest = {"event": potential_event, "time_difference_str": "-" + str(time_difference), "time_difference_timedelta": time_difference}
            else:
                closest = {"event": potential_event, "time_difference_str": str(time_difference), "time_difference_timedelta": time_difference}
            continue
        if time_difference < closest["time_difference_timedelta"]:
            if before == True:
                closest = {"event": potential_event, "time_difference_str": "-" + str(time_difference), "time_difference_timedelta": time_difference}
            else:
                closest = {"event": potential_event, "time_difference_str": str(time_difference), "time_difference_timedelta": time_difference}
    return closest

# add CrossReference events
def addCrossReferences(filtered_result, crossReferenceEvents):
    for primary_event in filtered_result:
        if 'startTime' in primary_event:
            primary_event['closestStartTime'] = findClosest(primary_event['startTime'], crossReferenceEvents)
        else:
            primary_event['closestTimeCollected'] = findClosest(primary_event['timeCollected'], crossReferenceEvents)
    return filtered_result

# write csvInfo to csv
def write_to_csv(args, csvInfo):
    if args.csv:
        csv_file = None
        try: 
            csv_file = open(args.csv, "w+")
            print(f"Writing to CSV file at {args.csv}... ", end="")
        except:
            print(f"ERROR: Could not open {args.csv}")
            sys.exit(1)
        csv_file.write("eventId,")
        for key in list(list(csvInfo.values())[0].keys()):
            csv_file.write(str(key)+",")
        csv_file.write("\n")
        for eventId in csvInfo:
            csv_file.write(f"{eventId},")
            for key in csvInfo[eventId]:
                csv_file.write(f"{csvInfo[eventId][key]},")
            csv_file.write("\n")
        print("Done!")

def uploadEventClips(args, downloaded, csvInfo, gcp_client):
    # upload event clips
    uploads = []
    if args.uploadEventClips:
        bucket_name = args.uploadEventClips.split("/")[0]
        base_path = "/".join(args.uploadEventClips.split("/")[1:]) + "/"
        base_path = base_path.replace("//","/")
        bucket = None
        try:
            bucket = gcp_client.bucket(bucket_name)
            print(f"Uploading all event clips to bucket {bucket_name} and path {base_path}")
        except:
            print(f"ERROR: Trouble opening bucket {bucket_name}")
            sys.exit(1)
        
        for filepath in downloaded:
            filename = filepath.split("/")[-1]
            print(f"Uploading {filename} to {bucket_name}/{base_path}... ", end="")
            blob = bucket.blob(base_path + filename)
            blob.upload_from_filename(filepath)
            uploads.append(bucket_name + "/" + base_path + filename)
            print("Done!")
            
        if args.csv: 
            for clip in uploads:
                eventId = clip.split("/")[-1].replace(".mp4","")
                csvInfo[eventId]["GCP Authenticated URL"] = "https://storage.cloud.google.com/" + clip 
    
    return args, csvInfo

def sensor_query():
    parser = argparse.ArgumentParser(description="Data API query tool for the Sigthhound Data API",
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     epilog=textwrap.dedent('''\
    Examples:
    Query data for collision sensor on BAI_0000754 for the last 3 days:
        python data-api.py --key=${API_KEY} --sensors=COLLISION_1 --deviceId=BAI_0000754 --lastDays=3
    Query data for collision sensor on BAI_0000754 for the last 5 hours:
        python data-api.py --key=${API_KEY} --sensors=COLLISION_1 --deviceId=BAI_0000754 --lastHours=5
    Query data for collision sensor on BAI_0000754 for a specific date range:
        python data-api.py --key=${API_KEY} --sensors=COLLISION_1 --deviceId=BAI_0000754 \
            --startTime=2021-07-20T16:49:41 --endTime=2021-07-22T16:49:41
    Query data for collision sensor on BAI_0000754 for the last day, filtering on events which occurred
    in the first 5 minutes of any 10 minute interval:
        python data-api.py --key=${API_KEY} --sensors=COLLISION_1 --deviceId=BAI_0000754 --lastDays=1 \
            --filterMinutesModulo=10 --filterMinutesRestrict=5
    Download clips of all collision events in the last hour to output folder ./output/:
        python3 data-api.py --key=${API_KEY} --sensors=COLLISION_1 --deviceId=BAI_0000754 --lastHours=1 \
            --filterMinutesModulo=10 --filterMinutesRestrict=5 --downloadEventClips --output output/
    Download event clips of all collision events in the last hour from GCP bucket base path sh-ext-customer (change to 
    your bucket name), upload event clips to GCP bucket bai-dev-data/ai-analysis/sample, and save a CSV file out.csv 
    with links to the clips. Note that this is the legacy implementation.
        export BUCKET_PATH=sh-ext-customer/
        python3 data-api.py --key=${API_KEY} --sensors=COLLISION_1 --deviceId=BAI_0000754 --lastHour=1 \
            --filterMinutesModulo=10 --filterMinutesRestrict=5 --downloadEventClips --sourceGCPpath ${BUCKET_PATH} \
            --output output/ --uploadEventClips bai-dev-data/ai-analysis/sample/ --csv out.csv
    '''))
    parser.add_argument('--sensors', help="A comma separated list of sensors to query")
    parser.add_argument('--deviceId', help="The device ID (BAI_XXXXXXX)")
    parser.add_argument('--lastMinutes',
                        type=int,
                        help="A number of minutes relative to endTime (or now if endTime is not specified) to query")
    parser.add_argument('--lastHours',
                        type=int,
                        help="A number of hours relative to endTime (or now if endTime is not specified) to query")
    parser.add_argument('--lastDays',
                        type=int,
                        help="A number of days relative to endTime (or now if endTime is not specified) to query")
    parser.add_argument('--startTime',
                        help="The start time, accepted in any format dateutil.parser supports.  Optional and not used"
                             "if --lastHours aor --lastDays is specified")
    parser.add_argument('--endTime',
                        help="The end time, accepted in any format dateutil.parser supports.\n"
                             "see https://dateutil.readthedocs.io/en/stable/examples.html#parse-examples.\n"
                             "If not specified, set to now")
    parser.add_argument('--key',
                        help="The API key for the workspace associated with the device (available from the platform)")
    parser.add_argument('--filterMinutesModulo', type=int,
                        help='An optional modulo filter.  When specified with filterMinutesRestrict this filters\n'
                             'out events which occurred outside periods defined by a modulus of the minute of event\n'
                             'for instance, specifying --filterMinutesModulo 10 and --filterMinutesRestrict 3 would\n'
                             'include events which happened during the first 3 minutes of every 10 minute interval,\n'
                             'starting at the top of the hour.')
    parser.add_argument('--filterMinutesRestrict', type=int,
                        help='An optional restrict filter.  See notes for filterMinutesModulo')
    parser.add_argument('--crossReferenceSensor', type=str,
                        help='A sensor to cross reference events with. The cross referenceed sensors time and \n'
                             'time difference relative to the original sensor will be included in the csv \n'
                             'if --csv is specified.')
    parser.add_argument('--downloadEventClips', action='store_true',
                        help='An optional argument to download the video clips of the events if they exist in the bai-rawdata\n'
                             'GCP bucket. Must be used with --output flag. ')
    parser.add_argument('-o', '--output',
                        help='Directory to download event clips. To be used with --downloadEventClips flag.')
    parser.add_argument('--sourceGCPpath', 
                        help='GCP path to search for and retrieve video clips from. Should be in the format'
                             '<bucket>/pathTo/deviceDirs/ . Defaults to bai-rawdata/gcpbai/ if not specified.')
    parser.add_argument('--uploadEventClips', 
                        help='GCP path to upload trimmed event clips to. Should be in the format'
                             '<bucket>/pathTo/deviceDirs/ . If specified, video clips will be deleted locally')
    parser.add_argument('--csv', 
                        help='Path to output CSV file with event clip information.'
                             'eventId and time collected information for each uploaded clip.')
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)
    args = parser.parse_args()

    time_parse(args, parser)
    result = query_flat(args, args.sensors)
    if args.filterMinutesModulo and args.filterMinutesRestrict:
        print(f"Events filtered for the first {args.filterMinutesRestrict} minutes of each "
              f"{args.filterMinutesModulo} minute interval")
        filtered_result = []
        for event in result:
            minutes = dateutil.parser.parse(event['timeCollected']).timetuple().tm_min
            if minutes % args.filterMinutesModulo < args.filterMinutesRestrict:
                filtered_result.append(event)
    else:
        filtered_result = result
    start_date = dateutil.parser.parse(args.startTime).astimezone(dateutil.tz.tzlocal())
    end_date = dateutil.parser.parse(args.endTime).astimezone(dateutil.tz.tzlocal())
    print(f"Starting at {args.startTime} (local time {start_date}) "
          f"and ending {end_date - start_date} later at {args.endTime} (local time {end_date})")
    pprint.pprint(filtered_result)

    if len(filtered_result) == 0:
        print("No events matching filters.")
        return None

    # initialize csvInfo dictionary
    csvInfo = {}
    if args.csv:
        for event in filtered_result:
            csvInfo[event['id']] = {}
            if event['value']:
                csvInfo[event['id']]['value'] = event['value']

    # cross reference events
    if args.crossReferenceSensor:
        print(f"Cross referencing {args.sensors} events with {args.crossReferenceSensor}")
        crossReferenceEvents = query_flat(args, args.crossReferenceSensor)
        for event_list in [filtered_result, crossReferenceEvents]:
            if event_list and re.match(r"(COLLISION_[0-9]*)|(PRESENCE_.*_[0-9]*)", event_list[0]['sensorName']):
                eventList = addStartTime(event_list)
        # add cross reference events to filtered_result dictionary
        filtered_result = addCrossReferences(filtered_result, crossReferenceEvents)
        if filtered_result and re.match(r"COLLISION_[0-9]*", filtered_result[0]['sensorName']):
            for event in filtered_result:
                csvInfo[event['id']]['startTime'] = event['startTime']
                csvInfo[event['id']]['endTime'] = event['timeCollected']
                csvInfo[event['id']][f'{args.crossReferenceSensor} Time'] = event['closestStartTime']['event']['startTime'] if event['closestStartTime'] else None
                csvInfo[event['id']][f'{args.crossReferenceSensor} Time Difference'] = event['closestStartTime']['time_difference_str'] if event['closestStartTime'] else None
        else:
            for event in filtered_result:
                csvInfo[event['id']]['timeCollected'] = event['timeCollected']
                csvInfo[event['id']][f'{args.crossReferenceSensor} Time'] = event['closestTimeCollected']['event']['timeCollected'] if event['closestTimeCollected'] else None
                csvInfo[event['id']][f'{args.crossReferenceSensor} Time Difference'] = event['closestTimeCollected']['time_difference_str'] if event['closestTimeCollected'] else None


    # download clips if video exists in source GCP bucket 
    if args.downloadEventClips:
        if not args.output:
            print("ERROR: must pass --output flag with --downloadEventClips")
            sys.exit(1)
        if not os.path.isdir(args.output):
            print(f"Creating output directory {args.output}")
            os.mkdir(args.output)

        format_str = "%Y-%m-%dT%H:%M:%S.000Z"
        # initialize gcp client
        gcp_client = None
        try:
            credentials, project = google.auth.default()
            gcp_client = storage.Client(project, credentials)
        except:
            print(f"Failed opening GCP storage client, please login using `gcloud auth application-default login`")
            sys.exit(1)

        downloaded = []
        for event in filtered_result:
            event_time = dateutil.parser.parse(event['timeCollected']).astimezone(dateutil.tz.UTC)
            print(f"Searching for video for event with ID {event['id']}... ", end="", flush=True)
            video_blob = findVideo(gcp_client, args, event_time)
            if video_blob == False:
                print("No luck.")
                continue
            else:
                print("Found!")
            filename = downloadClip(gcp_client, args, event, video_blob)
            downloaded.append(filename)
            print(f"Downloaded {filename}")
        # clear up tmp files
        if os.path.isdir(args.output + "/tmp/"):
            shutil.rmtree(args.output + "/tmp/")

        args, csvInfo = uploadEventClips(args, downloaded, csvInfo, gcp_client)
    # write results to csv
    write_to_csv(args, csvInfo)

    return filtered_result


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    result = sensor_query()

