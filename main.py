import machine
import uasyncio as asyncio
import dht
from mqtt_as import MQTTClient
from mqtt_local import config
import ujson as json

# Obtener un ID único basado en la dirección MAC del Raspberry Pi Pico W
id_dispositivo = "".join("{:02X}".format(b) for b in machine.unique_id())
print("El ID del dispositivo es:", id_dispositivo)

# Definición de pines
sensor = dht.DHT22(machine.Pin(15))  # Sensor de temperatura y humedad DHT22
rele = machine.Pin(2, machine.Pin.OUT)  # Relé para controlar calefacción
led = machine.Pin("LED", machine.Pin.OUT)  # LED indicador en la placa




async def wifi_han(state):
    print('WiFi está', 'conectado' if state else 'desconectado')
    await asyncio.sleep(1)






# Función para leer los parámetros desde config.json
def leer_parametros():
    """Lee los parámetros desde un archivo JSON."""
    try:
        with open("config.json", "r") as f:
            return json.load(f)
    except (OSError, ValueError):
        return {"setpoint": 20, "periodo": 10, "modo": "auto", "rele": 0}

# Cargar valores almacenados
config_data = leer_parametros()
setpoint = config_data["setpoint"]
periodo = config_data["periodo"]
modo = config_data["modo"]
rele_estado = config_data["rele"]


# Función para guardar parámetros en config.json
def guardar_parametros():
    """Guarda los parámetros en un archivo JSON."""
    try:
        # Crea el diccionario con los parámetros
        json_data = {"setpoint": setpoint, "periodo": periodo, "modo": modo, "rele": rele_estado}
        
        # Abre el archivo en modo escritura ('w')
        with open("config.json", "w") as f:
            # Escribe el diccionario en formato JSON usando ujson
            json.dump(json_data, f)
        print("Parámetros guardados.")

    except Exception as e:
        print(f"Error al guardar parámetros: {e}")





async def destellar_led():
    """Realiza el destello de los LED de forma asíncrona."""
    for _ in range(5):
        led.on()
        await asyncio.sleep(0.5)  # Usar asyncio.sleep para no bloquear el ciclo de eventos
        led.off()
        await asyncio.sleep(0.5)





async def actualizar_rele(medir=True):
    """Controla el estado del relé de forma asíncrona."""
    global setpoint, modo, rele_estado

    if modo == "auto":
        if medir:
            await asyncio.sleep(0)  # Permitir que otras tareas se ejecuten
            sensor.measure()
        
        temperatura = sensor.temperature()

        if temperatura > setpoint:
            rele.value(0)  # Encender relé si temperatura es mayor al setpoint
        else:
            rele.value(1)  # Apagar relé si la temperatura es menor o igual al setpoint
    else:
        rele.value(rele_estado)








def manejar_mensajes(topic, msg, retained):
    """Maneja los mensajes recibidos por MQTT y actualiza los parámetros."""
    
    print("Mensaje recibido")

    global setpoint, periodo, modo, rele_estado
    topic = topic.decode()
    msg = msg.decode()
    
    if topic.endswith("/setpoint"):
        setpoint = int(msg)
    elif topic.endswith("/periodo"):
        periodo = int(msg)
    elif topic.endswith("/modo"):
        modo = msg
    elif topic.endswith("/rele"):
        # Alternar el estado del relé cuando se recibe "rele"
        if msg.lower() == "rele":
            rele_estado = 1 if rele_estado == 0 else 0  # Cambia entre 0 y 1
    elif topic.endswith("/destello") and msg == "destello":
        # Llamamos a la función de destello sin bloquear el ciclo de eventos
        asyncio.create_task(destellar_led())  # Inicia el destello en una tarea separada
    
    guardar_parametros()
    asyncio.create_task(actualizar_rele(True))  # Llamada asíncrona para evitar bloqueos





async def publicar_datos(client):

    await client.connect()
    
    """Publica periódicamente los datos del sensor en MQTT."""
    while True:
        sensor.measure()
        asyncio.create_task(actualizar_rele(False))  # Llamada asíncrona

        data = {
            "temperatura": sensor.temperature(),
            "humedad": sensor.humidity(),
            "setpoint": setpoint,
            "periodo": periodo,
            "modo": modo
        }

        print(data)

        await client.publish(id_dispositivo, json.dumps(data), qos=1)
        await asyncio.sleep(periodo)


async def conexion_exitosa(client):
    """Se ejecuta cuando se establece la conexión MQTT."""
    await client.subscribe(f"{id_dispositivo}/setpoint", 1)
    await client.subscribe(f"{id_dispositivo}/periodo", 1)
    await client.subscribe(f"{id_dispositivo}/modo", 1)
    await client.subscribe(f"{id_dispositivo}/rele", 1)
    await client.subscribe(f"{id_dispositivo}/destello", 1)
    print("Conexión MQTT exitosa")
    
 


# Configuración de MQTT
config['subs_cb'] = manejar_mensajes
config['server'] = config['server']
config['connect_coro'] = conexion_exitosa
config['wifi_coro'] = wifi_han
config['ssl'] = True

# Configuración y ejecución del cliente MQTT
MQTTClient.DEBUG = True
client = MQTTClient(config)

try:
    asyncio.run(publicar_datos(client))
finally:
    client.close()
    asyncio.new_event_loop()
