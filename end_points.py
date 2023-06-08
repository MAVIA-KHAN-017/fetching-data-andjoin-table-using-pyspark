import requests
import uvicorn
from fastapi import FastAPI

APP_HOST = '0.0.0.0'
APP_PORT = 8000
XLOOP_API_BASE_URL = 'https://xloop-dummy.herokuapp.com'
TOMTOM_API_KEY = 'ir7kpwjOTDBQAvJ9vhYV1gSPuWLIvsD8'

app = FastAPI()

@app.get("/api/{patient_id}/{councillor_id}")
def calculate_distance(patient_id: int, councillor_id: int):
    patient_data = get_patient_data(patient_id)
    patient_user_id = patient_data["user_id"]
    patient_account_data = get_account_data(patient_user_id)
    patient_latitude = patient_account_data["address"]["location"]["lat"]
    patient_longitude = patient_account_data["address"]["location"]["lng"]
    patient_region = patient_account_data["address"]["region"]

    councillor_data = get_councillor_data(councillor_id)
    councillor_user_id = councillor_data["user_id"]
    councillor_account_data = get_account_data(councillor_user_id)
    councillor_latitude = councillor_account_data["address"]["location"]["lat"]
    councillor_longitude = councillor_account_data["address"]["location"]["lng"]
    councillor_region = councillor_account_data["address"]["region"]

    distance = get_distance(patient_latitude, patient_longitude, councillor_latitude, councillor_longitude)
    return distance, {"Region": councillor_region}


def get_patient_data(patient_id: int):
    url = f"{XLOOP_API_BASE_URL}/patient/{patient_id}"
    return get_data_from_url(url)


def get_councillor_data(councillor_id: int):
    url = f"{XLOOP_API_BASE_URL}/councillor/{councillor_id}"
    return get_data_from_url(url)


def get_account_data(user_id: str):
    url = f"{XLOOP_API_BASE_URL}/account/{user_id}"
    return get_data_from_url(url)


def get_data_from_url(url: str):
    response = requests.get(url)
    return response.json()


def get_distance(patient_latitude: float, patient_longitude: float, councillor_latitude: float,
                 councillor_longitude: float):
    # url = f"https://api.tomtom.com/routing/1/calculateRoute/{patient_latitude},{patient_longitude}:" \
    #       f"{councillor_latitude},{councillor_longitude}/json?key={TOMTOM_API_KEY}"
    url = f"https://api.tomtom.com/routing/1/calculateRoute/24.802018524835642,67.02959734583848:" \
           f"24.802152371128617,67.03002125428164/json?key={TOMTOM_API_KEY}"
    distance_response = requests.get(url)
    data = distance_response.json()
    dist = data["routes"][0]["summary"]["lengthInMeters"]
    time = data["routes"][0]["summary"]["travelTimeInSeconds"]
    return {"distance": dist, "travelTimeInSeconds": time}
    # sreturn data


if __name__ == '__main__':
    uvicorn.run(app='end_points:app', reload=True, host=APP_HOST, port=APP_PORT)
