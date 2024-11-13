import time
import datetime as dt
from datetime import datetime, timedelta, date
import argparse
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
import requests
import xml.etree.ElementTree as ET
from anthropic import AnthropicBedrock
from astral import LocationInfo
from astral.sun import sun
import re
import json
from collections import OrderedDict

parser = argparse.ArgumentParser()  # argument parser
parser.add_argument("-f", "--frequency", help="Specify how often data is collected (in minutes). Default = 60",
                    type=float, default=60)
parser.add_argument("-m", "--maidenhead", type=str, default="FN05GK",
                    help="Specify the Maidenhead Grid Locator you'd like to track conditions at. Default = FN05GK")
args = parser.parse_args()
maidenhead_grid_locator = args.maidenhead
frequency = args.frequency


def call_bedrock(current_data):
    """
    Makes an API call to AWS Bedrock.

    :param access_key: AWS Access Key ID.
    :param secret_key: AWS Secret Access Key.
    :param current_data: Solar data to provide to model.
    :return: json containing data.
    """
    xml_request = f"""
    <persona>:"You are a professional shortwave propagation prediction expert. Your writing style is clear, concise, and highly actionable."
    <target audience>:"Your target audience is ham radio operators that focus on contesting and DX-ing. Most of them do not understand how space weather and geomagnetic conditions impact radio waves propagation. They want to know at what time of the day and on what bands they can achieve high QSO rates and reach rare DX station. They want specific guidance what bands and modes to choose and what to avoid."
    <band groups>:
       <Low_Bands>:"160 and 80 meters"
       <Medium_Bands>:"40 and 30 meters"
       <Upper_Bands>:"20, 17, and 15 meters"
       <High_Bands>:"12 and 10 meters"
       <Magic_Band>:"6 meters"
    </band groups>
    <rating criteria>:
       <Poor>:"Propagation is highly unreliable or nonexistent on the specified band. Contacts are extremely difficult to achieve due to high signal absorption, heavy distortion, geomagnetic disturbances, or band closure. Expect no consistent or stable communication, with only rare and brief openings"
       <Fair>:"Propagation is possible but limited. Contacts may be sporadic, with signals often weak, distorted, or fading. Suitable mainly for short-range or regional communications. DX contacts are unlikely, but some local or regional communication may be possible with patience"
       <Good>: "Propagation is generally reliable, but conditions are not ideal or changing. Regional and DX contacts are feasible, though signals may experience occasional fading, distortion, or degradation. Good for moderate-distance contacts; DX is possible, but paths may be somewhat variable depending on geomagnetic conditions"
       <Excellent>: "Propagation is strong and stable. Ideal for long-distance (DX) communication. Signals are clear, with minimal fading or noise. Consistent openings for regional and global contacts, offering reliable communication across a wide range of distances and paths"
    </rating criteria>
    <JSON output format>
       <Summary>:200 word summary of current solar and geomagnetic conditions and their impact on short wave propagation
       <Low_Bands>:one word rating+":" + propagation prediction
       <Medium_Bands>:one word rating+":" + propagation prediction
       <Upper_Bands>:one word rating+":" + propagation prediction
       <High_Bands>:one word rating+":" + propagation prediction
       <Magic_Band>:one word rating+":" + propagation prediction
    </JSON output format>
    <task>:”For each band group, create a single word rating using the rating criteria and propagation prediction for the next 2 to 8 hours in the JSON output format. Consider seasonal, daytime, nighttime, dusk, and dawn differences. Consider east-west, cross-equatorial, and polar paths. Consider CW, SSB, and FT8/FT modes. Consider both regional and DX contacts impact. Be specific with distances and regions (East Coast of USA and Canada, West Coast of USA and Canada, Southern Canada and Midwest, Southern USA and Mexico, Central America, Caribbean, South America, Eastern Europe, Central Europe, Western Europe, Northern Europe, Africa, Middle East, Asia, Australia and New Zealand, Pacific Islands). Use the following station, space weather, and geomagnetic data.”
    <station data>
       <Maidenhead_grid_locator>:{maidenhead_grid_locator}
       <Location_noise_level>
          <Rural>:"-150 dbm"
       </Location_noise_level>
    </station data>
    {current_data}
    """

    session = boto3.Session()
    credentials = session.get_credentials().get_frozen_credentials()

    client = AnthropicBedrock(
        aws_access_key=credentials.access_key,
        aws_secret_key=credentials.secret_key,
        aws_session_token=credentials.token,
        aws_region=session.region_name
    )

    message = client.messages.create(
        model="us.anthropic.claude-3-5-sonnet-20240620-v1:0",
        messages=[{"role": "user", "content": f"{xml_request}"}],
        temperature=0.1,
        max_tokens=1500
    )

    output = message.content[0].text

    # Updated regex pattern
    pattern = r'"(?P<band>Summary|[A-Za-z_]+)":\s*"(?P<rating>Good|Excellent|Fair|Poor)?:?\s*(?P<explanation>.+?)"(?=\s*[,}])'
    # Use re.finditer to find all matches in the output text
    matches = re.finditer(pattern, output, re.DOTALL)

    # Parse the matches into the desired dictionary structure
    parsed_data = OrderedDict()

    for match in matches:
        band = match.group("band")
        rating = match.group("rating") or ""
        explanation = match.group("explanation").strip()

        if rating:
            parsed_data[band] = {
                "Rating": rating,
                "Explanation": explanation
            }
        else:
            # Handle "Summary" which has no rating
            parsed_data[band] = explanation

    # Convert to JSON string
    json_output = json.dumps(parsed_data, indent=4)

    return json_output


def upload_to_s3(file_name, bucket_name):
    session = boto3.Session()
    credentials = session.get_credentials().get_frozen_credentials()
    s3_client = boto3.client(
        's3',
        aws_access_key_id=credentials.access_key,
        aws_secret_access_key=credentials.secret_key,
        aws_session_token=credentials.token
    )
    obj_name = 'bedrock.json'

    try:
        s3_client.upload_file(file_name, bucket_name, obj_name, ExtraArgs={'ContentType':'application/json; charset=utf-8'})
        print(f"File {file_name} uploaded successfully to {bucket_name}/{obj_name}")
        return True
    except FileNotFoundError:
        print(f"The file {file_name} was not found")
    except NoCredentialsError:
        print("Credentials not available")
    except PartialCredentialsError:
        print("Incomplete credentials provided")
    except Exception as e:
        print(f"An error occurred: {e}")
    return False




def location_sunrise_sunset(grid, input_date):
    """
    Calculates the latitude, longitude, sunrise, and sunset times for a location
    based on its Maidenhead grid locator and date.

    :param grid: Maidenhead grid locator
    :param input_date: Date for which sunrise and sunset are calculated.
    :return: Dictionary with 'latitude', 'longitude', 'sunrise', and 'sunset' times in UTC.
    """
    grid = grid.upper()
    lon = -180
    lat = -90

    # First two characters (field)
    lon += (ord(grid[0]) - ord('A')) * 20
    lat += (ord(grid[1]) - ord('A')) * 10
    # Second two characters (square)
    lon += int(grid[2]) * 2
    lat += int(grid[3]) * 1
    # Third two characters (subsquare), if provided
    if len(grid) >= 6:
        lon += (ord(grid[4]) - ord('A')) * 5 / 60
        lat += (ord(grid[5]) - ord('A')) * 2.5 / 60
    # Calculate the center of the grid square
    lon += 1 / 60  # half of 2 minutes in degrees
    lat += 0.5 / 60  # half of 1 minute in degrees

    # Create a LocationInfo object for Astral
    location = LocationInfo(latitude=lat, longitude=lon)

    # Calculate sunrise and sunset
    s = sun(location.observer, date=input_date)

    # Return the results in a dictionary
    return {
        'latitude': lat,
        'longitude': lon,
        'sunrise': s['sunrise'].strftime('%Y-%m-%d %H:%M:%S UTC'),
        'sunset': s['sunset'].strftime('%Y-%m-%d %H:%M:%S UTC')
    }


def run(s3_bucket):
    # fetch solar widget XML data.
    solar_response = requests.get("https://www.hamqsl.com/solarxml.php")
    xml_data = solar_response.content
    root = ET.fromstring(xml_data)

    today = date.today()
    tomorrow = today + timedelta(days=1)
    loc_rise_set = location_sunrise_sunset(maidenhead_grid_locator, today)
    loc_rise_set_tomorrow = location_sunrise_sunset(maidenhead_grid_locator, tomorrow)
    now = dt.datetime.now(dt.timezone.utc).strftime("%b %d, %Y %H:%M")

    solar_data_bedrock_adjusted = {
        "SFI": root.findtext("solardata/solarflux"),
        "Sunspots": root.findtext("solardata/sunspots"),
        "K-Index": root.findtext("solardata/kindex"),
        "X-Ray": root.findtext("solardata/xray"),
        "Aurora Latitude": root.findtext("solardata/latdegree"),
        "Helium Line": root.findtext("solardata/heliumline"),
        "Proton Flux": root.findtext("solardata/protonflux"),
        "Electron Flux": root.findtext("solardata/electonflux"),
        "Solar Wind": root.findtext("solardata/solarwind"),
        "Bz": root.findtext("solardata/magneticfield"),
        "Date and Time": (now + " UTC"),
        "Latitude": loc_rise_set.get('latitude'),
        "Longitude": loc_rise_set.get('longitude'),
        "Today's Sunrise": loc_rise_set.get('sunrise'),
        "Today's Sunset": loc_rise_set.get('sunset'),
        "Tomorrow's Sunrise": loc_rise_set_tomorrow.get('sunrise'),
        "Tomorrow's Sunset": loc_rise_set_tomorrow.get('sunset'),
        "Aurora Short Term Forecast": "https://services.swpc.noaa.gov/text/aurora-nowcast-hemi-power.txt",
        "D-Region Absorption": "https://services.swpc.noaa.gov/text/drap_global_frequencies.txt"
    }

    xml_current_data = "<current_data>\n"
    for key, value in solar_data_bedrock_adjusted.items():
        # Handle None values to avoid empty tags
        if value is None:
            value = ""
        xml_current_data += f"   <{key}>{value}</{key}>\n"
    xml_current_data += "</current_data>"

    json_file = call_bedrock(xml_current_data)

    with open("bedrock.json", "w", encoding="utf-8") as file:
        file.write(json_file)

    upload_to_s3("bedrock.json", s3_bucket)


if __name__ == '__main__':
    time_to_wait = frequency * 60  # time to wait in between re-running program

    s3_bucket = input("Enter the name of the S3 Bucket you'd like to write to: ")
    while True:
        run(s3_bucket)
        time.sleep(time_to_wait)
