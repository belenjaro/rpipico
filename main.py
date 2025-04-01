from mqtt_as import MQTTClient, config
import uasyncio as asyncio
import dht
import machine
import ujson as json
from settings import SSID, password, BROKER

# Configuración de pines y sensor
sensor = dht.DHT22(machine.Pin(15))
rele = machine.Pin(2, machine.Pin.OUT)  # Relé para calefacción
led = machine.Pin("LED", machine.Pin.OUT)

# ID único del dispositivo (MAC)
id_dispositivo = "".join("{:02X}".format(b) for b in machine.unique_id())
print("El ID del dispositivo es", id_dispositivo)

# Configuración del cliente MQTT sin callbacks
config["server"] = BROKER
config["ssid"] = SSID
config["wifi_pw"] = password
config["queue_len"] = 10  # Mantener una cola de eventos
MQTTClient.DEBUG = False  # Deshabilitar mensajes de debug

# Crear cliente MQTT
client = MQTTClient(config)


async def manejar_mensajes():
    """Maneja los mensajes recibidos desde la cola de eventos."""
    async for topic, msg, retained in client.queue:
        topic = topic.decode()
        msg = msg.decode().lower()
        print(f"Mensaje recibido en {topic}: {msg}")

        global setpoint, periodo, modo, rele_estado

        if topic.endswith("/setpoint"):
            setpoint = int(msg)
        elif topic.endswith("/periodo"):
            periodo = int(msg)
        elif topic.endswith("/modo"):
            modo = msg
        elif topic.endswith("/rele"):
            rele_estado = 0 if msg in ["on", "1"] else 1  # Lógica inversa
        elif topic.endswith("/destello") and msg == "destello":
            asyncio.create_task(destellar_led())

        guardar_parametros()
        asyncio.create_task(actualizar_rele(False))  # No bloqueante


async def conexion_establecida():
    """Se ejecuta cuando la conexión MQTT está activa."""
    while True:
        await client.up.wait()  # Esperar conexión
        client.up.clear()
        print("Conectado a MQTT, renovando suscripciones...")
        await client.subscribe(f"{id_dispositivo}/setpoint", 1)
        await client.subscribe(f"{id_dispositivo}/periodo", 1)
        await client.subscribe(f"{id_dispositivo}/modo", 1)
        await client.subscribe(f"{id_dispositivo}/rele", 1)
        await client.subscribe(f"{id_dispositivo}/destello", 1)


async def publicar_datos():
    """Publica datos del sensor periódicamente."""
    while True:
        try:
            sensor.measure()
            data = {
                "temperatura": sensor.temperature(),
                "humedad": sensor.humidity(),
                "setpoint": setpoint,
                "periodo": periodo,
                "modo": modo
            }
            print("Publicando:", data)
            await client.publish(id_dispositivo, json.dumps(data), qos=1)
        except OSError:
            print("Error al leer el sensor")
        await asyncio.sleep(periodo)


async def destellar_led():
    """Hace parpadear el LED de forma asíncrona."""
    for _ in range(5):
        led.on()
        await asyncio.sleep(0.5)
        led.off()
        await asyncio.sleep(0.5)


async def actualizar_rele(medir=True):
    """Controla el estado del relé basado en la temperatura y el modo."""
    global setpoint, modo, rele_estado

    if modo == "auto":
        if medir:
            await asyncio.sleep(0)  # Ceder el control de eventos
            sensor.measure()

        temperatura = sensor.temperature()
        rele.value(0 if temperatura > setpoint else 1)  # Lógica inversa
    else:
        rele.value(rele_estado)


def leer_parametros():
    """Carga los parámetros desde config.json."""
    try:
        with open("config.json", "r") as f:
            return json.load(f)
    except (OSError, ValueError):
        return {"setpoint": 20, "periodo": 10, "modo": "auto", "rele": 0}


def guardar_parametros():
    """Guarda los parámetros en config.json."""
    try:
        with open("config.json", "w") as f:
            json.dump({"setpoint": setpoint, "periodo": periodo, "modo": modo, "rele": rele_estado}, f)
        print("Parámetros guardados.")
    except Exception as e:
        print(f"Error al guardar parámetros: {e}")


async def main():
    """Inicia el cliente MQTT y las tareas principales."""
    await client.connect()
    asyncio.create_task(conexion_establecida())
    asyncio.create_task(manejar_mensajes())
    asyncio.create_task(publicar_datos())

    while True:
        await asyncio.sleep(1)  # Mantener el bucle en ejecución


# Cargar valores iniciales
config_data = leer_parametros()
setpoint = config_data["setpoint"]
periodo = config_data["periodo"]
modo = config_data["modo"]
rele_estado = config_data["rele"]

# Ejecutar el bucle principal
try:
    asyncio.run(main())
finally:
    client.close()
