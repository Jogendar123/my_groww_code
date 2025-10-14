from flask import Flask
import subprocess, os, signal

app = Flask(__name__)
process = None

@app.route('/start')
def start_bot():
    global process
    if process is None:
        process = subprocess.Popen(["python", "main.py"])
        return "✅ Trading bot started!"
    return "⚠️ Already running."

@app.route('/stop')
def stop_bot():
    global process
    if process:
        os.kill(process.pid, signal.SIGTERM)
        process = None
        return "🛑 Trading bot stopped!"
    return "⚠️ Bot not running."

@app.route('/')
def index():
    return "<h3>Groww Nifty Bot Control Panel</h3><p>/start → run bot<br>/stop → stop bot</p>"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
