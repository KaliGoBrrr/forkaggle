import cv2
from datetime import datetime
from confluent_kafka import Producer
import json
import os

# Delivery report callback function
def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

cap = cv2.VideoCapture("C:\\Linhtinh\\Video\\VIRAT_S_000002.mp4")
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'videodetect'

producer_config = {
    'bootstrap.servers': kafka_bootstrap_servers,
}

producer = Producer(producer_config)

# Create a JSON file to store information about motion detection events
#json_log_filename = "C:\\Users\\admin\\PycharmProjects\\BigData\\CSVanh"
#event_log = open(json_log_filename, "w", encoding='utf-8')

ret, frame1 = cap.read()
ret, frame2 = cap.read()
print(frame1.shape)

while cap.isOpened():
    diff = cv2.absdiff(frame1, frame2)
    gray = cv2.cvtColor(diff, cv2.COLOR_BGR2GRAY)
    blur = cv2.GaussianBlur(gray, (5, 5), 0)
    _, thresh = cv2.threshold(blur, 20, 255, cv2.THRESH_BINARY)
    dilated = cv2.dilate(thresh, None, iterations=3)
    contours, _ = cv2.findContours(dilated, cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE)

    for contour in contours:
        (x, y, w, h) = cv2.boundingRect(contour)

        if cv2.contourArea(contour) < 900:
            continue
        cv2.rectangle(frame1, (x, y), (x + w, y + h), (0, 255, 0), 2)
        cv2.putText(frame1, "Status: Movement", (10, 20), cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 0, 255), 3)

        # Create a dictionary with information about the event
        event_datetime = datetime.now()
        event_data = {
            "frame": int(cap.get(cv2.CAP_PROP_POS_FRAMES)),
            "datetime": event_datetime.strftime("%Y-%m-%d %H:%M:%S")
        }

        # Convert the dictionary to a JSON string
        event_json = json.dumps(event_data)

        # Print the JSON data to the console
        print(event_json)

        # Write the JSON data to the log file
        #event_log.write(event_json + "\n")

        # Send data to Kafka
        producer.produce(kafka_topic, key=None, value=event_json, callback=delivery_report)

        # Save the image to the directory
        frame_filename = f"C:\\Users\\admin\\PycharmProjects\\BigData\\CSVanh\\frame_{int(cap.get(cv2.CAP_PROP_POS_FRAMES))}.jpg"
        cv2.imwrite(frame_filename, frame1)

    cv2.imshow("feed", frame1)

    frame1 = frame2
    ret, frame2 = cap.read()

    if cv2.waitKey(40) == 27:
        break

# Close the JSON file and release resources
#event_log.close()
cv2.destroyAllWindows()
cap.release()
