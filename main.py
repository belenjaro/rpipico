import network
import ubinascii
import machine
import utime
import json
from umqtt.simple import MQTTClient
import dht


TOPIC = "belen/{}".format(ubinascii.hexlify(machine.unique_id()).decode())

# Conectar a WiFi
def connect_wifi():
    wlan = network.WLAN(network.STA_IF)
    wlan.active(True)
    wlan.connect(SSID, PASSWORD)
    while not wlan.isconnected():
        print("Conectando a WiFi...")
        utime.sleep(1)
    print("Conectado a WiFi", wlan.ifconfig())

# Configuración MQTT
client = MQTTClient("pico_client", BROKER)

# Sensores y actuadores
d = dht.DHT22(machine.Pin(13))  # Sensor DHT22
led = machine.Pin(27, machine.Pin.OUT)
rele = machine.Pin(12, machine.Pin.OUT)

# Estado inicial
parametros = {
    "temperatura": 0.0,
    "humedad": 0.0,
    "setpoint": 26.5,
    "periodo": 10,
    "modo": "auto",
    "rele": "OFF"
}

def sub_cb(topic, msg):
    global parametros
    topic = topic.decode()
    msg = msg.decode()
    print(f"Recibido -> {topic}: {msg}")
    
    if topic == "setpoint":
        try:
            parametros['setpoint'] = float(msg)
        except ValueError:
            print("ERROR: setpoint debe ser flotante")
    elif topic == "periodo":
        try:
            parametros['periodo'] = float(msg)
        except ValueError:
            print("ERROR: periodo debe ser flotante")
    elif topic == "modo":
        if msg in ["auto", "manual"]:
            parametros['modo'] = msg
        else:
            print("ERROR: modo debe ser auto o manual")
    elif topic == "rele" and parametros['modo'] == "manual":
        if msg == "ON":
            rele.value(0)
        elif msg == "OFF":
            rele.value(1)
    client.publish(TOPIC, json.dumps(parametros))

def monitoreo():
    while True:
        d.measure()
        parametros["temperatura"] = d.temperature()
        parametros["humedad"] = d.humidity()
        
        if parametros["modo"] == "auto":
            if parametros["temperatura"] > parametros["setpoint"]:
                rele.value(0)
                parametros["rele"] = "ON"
            else:
                rele.value(1)
                parametros["rele"] = "OFF"
        
        client.publish(TOPIC, json.dumps(parametros))
        utime.sleep(parametros['periodo'])

# Conectar WiFi y MQTT
connect_wifi()
client.set_callback(sub_cb)
client.connect()
client.subscribe("setpoint")
client.subscribe("periodo")
client.subscribe("modo")
client.subscribe("rele")

while True:
    try:
        client.check_msg()
        monitoreo()
    except Exception as e:
        print("Error:", e)
        utime.sleep(5)
