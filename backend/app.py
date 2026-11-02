import asyncio
import os
import subprocess
from pathlib import Path
from urllib.parse import quote

from dotenv import load_dotenv
from flask import Flask, Response, jsonify, send_from_directory, stream_with_context
from tapo import ApiClient
from tuya_connector import TuyaOpenAPI

load_dotenv(Path(__file__).with_name(".env"))

app = Flask(__name__, static_folder="../frontend", static_url_path="")

# Credenciales de la cancela (leídas de .env)
ACCESS_ID = os.getenv("TUYA_CLIENT_ID")
ACCESS_KEY = os.getenv("TUYA_SECRET_KEY")
ENDPOINT = os.getenv("TUYA_ENDPOINT", "https://openapi.tuyaeu.com")
DEVICE_ID = os.getenv("TUYA_DEVICE_ID")

# Credenciales Tapo en backend/.env; las IP también se pueden configurar allí.
TAPO_USERNAME = os.getenv("TAPO_USERNAME", "")
TAPO_PASSWORD = os.getenv("TAPO_PASSWORD", "")
TAPO_CAMERA_USERNAME = os.getenv("TAPO_CAMERA_USERNAME", "")
TAPO_CAMERA_PASSWORD = os.getenv("TAPO_CAMERA_PASSWORD", "")
TAPO_DEVICES = {
    "luces": {"ip": os.getenv("TAPO_LUCES_IP", "192.168.18.230")},
    "depuradora": {"ip": os.getenv("TAPO_DEPURADORA_IP", "192.168.18.239")},
    "entrada": {"ip": os.getenv("TAPO_ENTRADA_IP", "192.168.18.48")},
    "termo": {"ip": os.getenv("TAPO_TERMO_IP", "192.168.18.231")},
}

# Servir la PWA visual
@app.route("/")
def index():
    return send_from_directory(app.static_folder, "index.html")

# Estado del servidor
@app.route("/api/status")
def status():
    return jsonify({"status": "online", "message": "Domo-trapa Backend funcionando correctamente"})

# Pulso de la cancela
@app.route("/api/gate/pulse", methods=["POST"])
def gate_pulse():
    try:
        openapi = TuyaOpenAPI(ENDPOINT, ACCESS_ID, ACCESS_KEY)
        openapi.connect()
        command = {
            "commands": [
                {"code": "switch_1", "value": True}
            ]
        }
        response = openapi.post(f"/v1.0/iot-03/devices/{DEVICE_ID}/commands", command)
        return jsonify({"success": True, "response": response})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

async def discover_tapo_device(ip, email, password):
    client = ApiClient(email, password)
    discovery = await client.discover_devices(ip, timeout_s=5)
    discovery_errors = []

    async for candidate in discovery:
        try:
            result = candidate.get()
        except Exception as exc:
            discovery_errors.append(exc)
            continue

        handler = getattr(result, "handler", None)
        device_info = getattr(result, "device_info", None)
        if handler is None or device_info is None:
            model = getattr(device_info, "model", "desconocido")
            raise ValueError(f"El dispositivo detectado ({model}) no tiene un controlador de encendido compatible")

        turn_on = getattr(handler, "on", None)
        turn_off = getattr(handler, "off", None)
        if not callable(turn_on) or not callable(turn_off):
            model = getattr(device_info, "model", "desconocido")
            raise ValueError(f"El dispositivo detectado ({model}) no permite conmutar encendido/apagado")

        is_on = getattr(device_info, "device_on", None)
        if is_on is None:
            device_info = await handler.get_device_info()

        return handler, device_info

    if discovery_errors:
        raise discovery_errors[0]
    raise TimeoutError(f"No se encontró ningún dispositivo Tapo en {ip}")


# Descubre el handler según el modelo; evita asumir que todos los equipos son P100.
async def control_tapo_async(ip, email, password):
    handler, device_info = await discover_tapo_device(ip, email, password)
    is_on = getattr(device_info, "device_on", None)
    if is_on is None:
        model = getattr(device_info, "model", "desconocido")
        raise ValueError(f"No se pudo leer el estado actual del dispositivo {model}")

    await (handler.off() if is_on else handler.on())

# Control genérico de enchufes Tapo
@app.route("/api/tapo/<device_name>/toggle", methods=["POST"])
def toggle_tapo_device(device_name):
    if device_name not in TAPO_DEVICES:
        return jsonify({"success": False, "error": "Dispositivo no encontrado"}), 404

    dev = TAPO_DEVICES[device_name]
    try:
        if not TAPO_USERNAME or not TAPO_PASSWORD:
            return jsonify({"success": False, "error": "Configura TAPO_USERNAME y TAPO_PASSWORD en backend/.env"}), 503
        # Ejecutamos la función asíncrona de forma síncrona dentro de Flask
        asyncio.run(control_tapo_async(dev["ip"], TAPO_USERNAME, TAPO_PASSWORD))
        return jsonify({"success": True, "message": f"Dispositivo '{device_name}' conmutado correctamente"})
    except Exception as e:
        detail = str(e)
        if "HASH_MISMATCH" in detail:
            detail = "El enchufe rechazó las credenciales Tapo; comprueba el correo y la contraseña exactos, respetando mayúsculas."
        return jsonify({"success": False, "error": f"Error al conmutar: {detail}"}), 500


@app.route("/api/camera/entrada/stream")
def stream_entrada_camera():
    if not TAPO_CAMERA_USERNAME or not TAPO_CAMERA_PASSWORD:
        return jsonify({
            "success": False,
            "error": "Configura TAPO_CAMERA_USERNAME y TAPO_CAMERA_PASSWORD en backend/.env",
        }), 503

    process = None
    try:
        import imageio_ffmpeg

        camera_ip = TAPO_DEVICES["entrada"]["ip"]
        username = quote(TAPO_CAMERA_USERNAME, safe="")
        password = quote(TAPO_CAMERA_PASSWORD, safe="")
        stream_url = f"rtsp://{username}:{password}@{camera_ip}:554/stream2"
        command = [
            imageio_ffmpeg.get_ffmpeg_exe(),
            "-hide_banner", "-loglevel", "error", "-nostdin",
            "-rtsp_transport", "tcp", "-timeout", "8000000",
            "-i", stream_url,
            "-an", "-vf", "fps=8,scale=640:-2", "-q:v", "6",
            "-f", "mpjpeg", "pipe:1",
        ]
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            bufsize=0,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        first_chunk = process.stdout.read(4096)
        if not first_chunk:
            process.terminate()
            process.wait(timeout=3)
            return jsonify({
                "success": False,
                "error": "No se recibió vídeo. Comprueba la cuenta de cámara y que RTSP esté disponible.",
            }), 502
    except Exception:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait()
        return jsonify({
            "success": False,
            "error": "No se pudo iniciar la transmisión de la cámara.",
        }), 502

    def frames():
        try:
            yield first_chunk
            while True:
                chunk = process.stdout.read(4096)
                if not chunk:
                    break
                yield chunk
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()

    response = Response(
        stream_with_context(frames()),
        mimetype="multipart/x-mixed-replace; boundary=ffmpeg",
    )
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    return response

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=False)
