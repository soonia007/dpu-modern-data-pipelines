import json

import requests


if __name__ == "__main__":
    # Get data from Dog API
    # โค้ดส่วนนี้จะเป็นการเชื่อมต่อกับ Dog API โดยเราจะใส่ URL Endpoint
    url = "https://dog.ceo/api/breeds/image/random"
    # เสร็จแล้วจะยิง Request ไปดึงข้อมูลมาเก็บใส่ Response
    response = requests.get(url)
    data = response.json()
    print(data)

    # Write data to file
    with open("dogs.json", "w") as f:
        json.dump(data, f)




if __name__ == "__main__":
    # Get data from Dog API
    # โค้ดส่วนนี้จะเป็นการเชื่อมต่อกับ Dog API โดยเราจะใส่ URL Endpoint
    url = "https://dog.ceo/api/breeds/image/random"
    # เสร็จแล้วจะยิง Request ไปดึงข้อมูลมาเก็บใส่ Response
    response = requests.get(url)
    data = response.json()
    print(data)

    # Write data to file
    with open("dogs.json", "w") as f:
        json.dump(data, f)

    # โค้ดส่วนนี้จะเป็นการเตรียมเรื่อง Credentials และ Configuration ต่าง ๆ เพื่อใช้ในการเชื่อมต่อกับ Dog API
    api_url = "https://api.jsonbin.io/v3/b"
    headers = {
        "Content-Type": "application/json",
        "X-Master-Key": "$2a$10$rO3m/MnafmnrgC0kv8/lm.VlY5tClu7Q6ikL7UPo0z9g1nJuikqyK",
        "X-Collection-Id": "678b6261acd3cb34a8ce4087",
    }

    # Read data from file
    with open("dogs.json", "r") as f:
        data = json.load(f)

    # โค้ดส่วนนี้จะเป็นการยิง Request ไปเพื่ออัพโหลดข้อมูลไปยังเซิฟเวอร์ของ JSONBin
    response = requests.post(api_url, json=data, headers=headers)
    print(response.json())