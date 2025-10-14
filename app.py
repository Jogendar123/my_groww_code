from flask import Flask
import threading
import os
import main   # 👈 this imports your main.py file

app = Flask(__name__)

is_running = False
thread = None

def background_task():
    """Run your real logic from main.py"""
    main.run_script()  # 👈 call the function from main.py

@app.route('/start')
def start():
    global is_running, thread
    if not is_running:
        is_running = True
        thread = threading.Thread(target=background_task)
        thread.start()
        return "✅ Script started successfully (main.py running)!"
    else:
        return "⚠️ Script is already running!"

@app.route('/stop')
def stop():
    global is_running
    if is_running:
        is_running = False
        return "🛑 Script stopped."
    else:
        return "⚠️ Script is not running."

@app.route('/')
def home():
    return "🌐 Use /start or /stop to control your script."

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
